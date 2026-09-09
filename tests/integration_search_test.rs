use axum::body::Body;
use axum::http::{Request, StatusCode};
use flate2::write::GzEncoder;
use flate2::Compression;
use http_body_util::BodyExt;
use imdb_indexer::api::{create_router, AppState};
use imdb_indexer::config::Config;
use imdb_indexer::downloader::{DUMP_AKAS, DUMP_BASICS, DUMP_RATINGS};
use imdb_indexer::index::manager::IndexManager;
use imdb_indexer::ingestion::temp_store::TempIngestStore;
use imdb_indexer::ingestion::IngestionPipeline;
use imdb_indexer::search::{SearchEngine, SearchParams};
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::sync::RwLock;
use tower::ServiceExt;

fn write_mock_tsv_gz<P: AsRef<Path>>(path: P, content: &str) -> anyhow::Result<()> {
    let file = File::create(path)?;
    let mut encoder = GzEncoder::new(file, Compression::default());
    encoder.write_all(content.as_bytes())?;
    encoder.finish()?;
    Ok(())
}

fn setup_test_environment() -> anyhow::Result<(tempfile::TempDir, IndexManager)> {
    let temp_dir = tempfile::tempdir()?;
    let downloads_dir = temp_dir.path().join("downloads");
    fs::create_dir_all(&downloads_dir)?;

    // 1. Mock title.ratings.tsv.gz
    let ratings_content = "tconst\taverageRating\tnumVotes\n\
                           tt0111161\t9.3\t2800000\n\
                           tt0468569\t9.0\t2850000\n\
                           tt0816692\t8.7\t2000000\n\
                           tt0137523\t8.8\t2250000\n\
                           tt0120737\t8.8\t1950000\n\
                           tt1375666\t8.8\t2500000\n\
                           tt0068646\t9.2\t1980000\n\
                           tt0109830\t8.8\t2200000\n\
                           tt0133093\t8.7\t2050000\n\
                           tt0075314\t8.4\t150000\n\
                           tt9999001\t2.1\t3\n\
                           tt9999002\t4.0\t5\n";
    write_mock_tsv_gz(downloads_dir.join(DUMP_RATINGS), ratings_content)?;

    // 2. Mock title.akas.tsv.gz
    let akas_content = "titleId\tordering\ttitle\tregion\tlanguage\ttypes\tattributes\tisOriginalTitle\n\
                        tt0111161\t1\tПобег из Шоушенка\tRU\tru\t\\N\t\\N\t0\n\
                        tt0468569\t1\tТёмный рыцарь\tRU\tru\t\\N\t\\N\t0\n\
                        tt0816692\t1\tИнтерстеллар\tRU\tru\t\\N\t\\N\t0\n\
                        tt0137523\t1\tБойцовский клуб\tRU\tru\t\\N\t\\N\t0\n\
                        tt0120737\t1\tВластелин колец: Братство Кольца\tRU\tru\t\\N\t\\N\t0\n\
                        tt1375666\t1\tНачало\tRU\tru\t\\N\t\\N\t0\n\
                        tt0068646\t1\tКрёстный отец\tRU\tru\t\\N\t\\N\t0\n\
                        tt0109830\t1\tФоррест Гамп\tRU\tru\t\\N\t\\N\t0\n\
                        tt0133093\t1\tМатрица\tRU\tru\t\\N\t\\N\t0\n\
                        tt0075314\t1\tВосхождение\tSU\tru\t\\N\t\\N\t0\n\
                        tt9999001\t1\tИнтерстеларчик\tRU\tru\t\\N\t\\N\t0\n\
                        tt9999002\t1\tТемный рыцырь\tRU\tru\t\\N\t\\N\t0\n";
    write_mock_tsv_gz(downloads_dir.join(DUMP_AKAS), akas_content)?;

    // 3. Mock title.basics.tsv.gz
    let basics_content = "tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenres\n\
                          tt0111161\tmovie\tThe Shawshank Redemption\tThe Shawshank Redemption\t0\t1994\t\\N\t142\tDrama\n\
                          tt0468569\tmovie\tThe Dark Knight\tThe Dark Knight\t0\t2008\t\\N\t152\tAction,Crime,Drama\n\
                          tt0816692\tmovie\tInterstellar\tInterstellar\t0\t2014\t\\N\t169\tAdventure,Drama,Sci-Fi\n\
                          tt0137523\tmovie\tFight Club\tFight Club\t0\t1999\t\\N\t139\tDrama\n\
                          tt0120737\tmovie\tThe Lord of the Rings\tThe Lord of the Rings\t0\t2001\t\\N\t178\tAction,Adventure,Drama\n\
                          tt1375666\tmovie\tInception\tInception\t0\t2010\t\\N\t148\tAction,Adventure,Sci-Fi\n\
                          tt0068646\tmovie\tThe Godfather\tThe Godfather\t0\t1972\t\\N\t175\tCrime,Drama\n\
                          tt0109830\tmovie\tForrest Gump\tForrest Gump\t0\t1994\t\\N\t142\tDrama,Romance\n\
                          tt0133093\tmovie\tThe Matrix\tThe Matrix\t0\t1999\t\\N\t136\tAction,Sci-Fi\n\
                          tt0075314\tmovie\tThe Ascent\tVoskhozhdeniye\t0\t1977\t\\N\t111\tDrama,War\n\
                          tt9999001\tmovie\tInterstellar Short\tInterstellar Short\t0\t2021\t\\N\t5\tShort\n\
                          tt9999002\tmovie\tDark Knight Fan\tDark Knight Fan\t0\t2022\t\\N\t10\tShort\n";
    write_mock_tsv_gz(downloads_dir.join(DUMP_BASICS), basics_content)?;

    let mut config = Config::default();
    config.storage.data_dir = temp_dir.path().to_path_buf();

    let mut manager = IndexManager::open_or_create(temp_dir.path().join("indices"))?;
    let pipeline = IngestionPipeline::new(config);

    let temp_store = TempIngestStore::create(temp_dir.path().join("temp.redb"))?;
    pipeline.run_internal_stream(&downloads_dir, &temp_store, &mut manager)?;

    Ok((temp_dir, manager))
}

trait PipelineTestExt {
    fn run_internal_stream(
        &self,
        downloads_dir: &Path,
        temp_store: &TempIngestStore,
        manager: &mut IndexManager,
    ) -> anyhow::Result<()>;
}

impl PipelineTestExt for IngestionPipeline {
    fn run_internal_stream(
        &self,
        downloads_dir: &Path,
        temp_store: &TempIngestStore,
        manager: &mut IndexManager,
    ) -> anyhow::Result<()> {
        // Stream ratings
        let ratings_file = File::open(downloads_dir.join(DUMP_RATINGS))?;
        let gz = flate2::read::GzDecoder::new(std::io::BufReader::new(ratings_file));
        let mut rdr = csv::ReaderBuilder::new().delimiter(b'\t').has_headers(true).from_reader(gz);
        let mut batch = Vec::new();
        for res in rdr.byte_records() {
            let record = res?;
            if let Some(id) = imdb_indexer::ingestion::temp_store::parse_tconst_id(std::str::from_utf8(&record[0])?) {
                let rating: f32 = std::str::from_utf8(&record[1])?.parse().unwrap_or(0.0);
                let votes: u32 = std::str::from_utf8(&record[2])?.parse().unwrap_or(0);
                batch.push((id, (rating * 10.0) as u8, votes));
            }
        }
        temp_store.insert_ratings_batch(&batch)?;

        // Stream akas
        let akas_file = File::open(downloads_dir.join(DUMP_AKAS))?;
        let gz = flate2::read::GzDecoder::new(std::io::BufReader::new(akas_file));
        let mut rdr = csv::ReaderBuilder::new().delimiter(b'\t').has_headers(true).from_reader(gz);
        let mut akas_batch = Vec::new();
        for res in rdr.byte_records() {
            let record = res?;
            let reg = &record[3];
            let lang = &record[4];
            if reg == b"RU" || reg == b"SU" || lang == b"ru" {
                if let Some(id) = imdb_indexer::ingestion::temp_store::parse_tconst_id(std::str::from_utf8(&record[0])?) {
                    let title = std::str::from_utf8(&record[2])?.to_string();
                    akas_batch.push((id, title));
                }
            }
        }
        temp_store.insert_akas_batch(&akas_batch)?;

        // Stream basics
        let (_idx, mut writer, gen) = manager.create_new_generation(50)?;
        let schema = manager.schema().clone();
        let reader = temp_store.begin_reader()?;

        let basics_file = File::open(downloads_dir.join(DUMP_BASICS))?;
        let gz = flate2::read::GzDecoder::new(std::io::BufReader::new(basics_file));
        let mut rdr = csv::ReaderBuilder::new().delimiter(b'\t').has_headers(true).from_reader(gz);
        for res in rdr.byte_records() {
            let record = res?;
            let tconst = std::str::from_utf8(&record[0])?;
            let title_type = std::str::from_utf8(&record[1])?;
            let primary = std::str::from_utf8(&record[2])?;
            let orig = std::str::from_utf8(&record[3])?;
            let year = std::str::from_utf8(&record[5])?.parse::<u32>().ok();
            let runtime = std::str::from_utf8(&record[7])?.parse::<u32>().ok();
            let genres: Vec<String> = std::str::from_utf8(&record[8])?.split(',').map(|s| s.to_string()).collect();

            let id = imdb_indexer::ingestion::temp_store::parse_tconst_id(tconst).unwrap();
            let (rating_info, ru_titles) = reader.lookup(id);
            let (rating, votes) = match rating_info {
                Some((r, v)) => (Some(r), v),
                None => (None, 0),
            };

            let ru_list = ru_titles.unwrap_or_default();
            let first_ru = ru_list.first().cloned();

            let doc = imdb_indexer::index::schema::MovieDoc {
                tconst: tconst.to_string(),
                title_ru: first_ru,
                title_orig: orig.to_string(),
                title_primary: primary.to_string(),
                russian_titles: ru_list,
                year,
                title_type: title_type.to_string(),
                rating,
                num_votes: votes,
                genres,
                runtime_minutes: runtime,
            };
            writer.add_document(schema.to_tantivy_doc(&doc))?;
        }
        writer.commit()?;
        manager.activate_generation(&gen)?;
        Ok(())
    }
}

#[tokio::test]
async fn test_search_cases() -> anyhow::Result<()> {
    let (_temp_dir, manager) = setup_test_environment()?;
    let engine = SearchEngine::new(manager.reader(), manager.schema().clone());

    // 1. Exact Russian search
    let hits = engine.search(&SearchParams {
        query: "Побег из Шоушенка".to_string(),
        limit: 5,
        ..Default::default()
    })?;
    assert!(!hits.is_empty());
    assert_eq!(hits[0].movie.tconst, "tt0111161");

    // 2. ё vs е test: searching "Темный рыцарь" should find "Тёмный рыцарь"
    let hits = engine.search(&SearchParams {
        query: "Темный рыцарь".to_string(),
        limit: 5,
        ..Default::default()
    })?;
    assert!(!hits.is_empty());
    assert_eq!(hits[0].movie.tconst, "tt0468569");
    assert_eq!(hits[0].movie.title_ru.as_deref(), Some("Тёмный рыцарь"));

    // 3. Search by English original title: "The Dark Knight"
    let hits = engine.search(&SearchParams {
        query: "The Dark Knight".to_string(),
        limit: 5,
        ..Default::default()
    })?;
    assert!(!hits.is_empty());
    assert_eq!(hits[0].movie.tconst, "tt0468569");

    // 4. Typo in Russian query: "интерстелар" (missing second 'л')
    // Must return "Интерстеллар" as #1, beating the obscure short "Интерстеларчик" (3 votes)!
    let hits = engine.search(&SearchParams {
        query: "интерстелар".to_string(),
        limit: 5,
        ..Default::default()
    })?;
    assert!(!hits.is_empty());
    assert_eq!(
        hits[0].movie.tconst, "tt0816692",
        "Interstellar (2M votes) must be ranked #1 despite typo!"
    );
    assert_eq!(hits[0].movie.title_ru.as_deref(), Some("Интерстеллар"));

    // 5. Typo in Russian query matching an exact typo in an obscure movie:
    // User types "темный рыцырь" (typo 'ы' instead of 'а')
    // Obscure movie (5 votes) literally has "Темный рыцырь", but blockbuster (2.85M votes)
    // has "Тёмный рыцарь". The blockbuster MUST rank #1 due to smart popularity ranking!
    let hits = engine.search(&SearchParams {
        query: "темный рыцырь".to_string(),
        limit: 5,
        ..Default::default()
    })?;
    assert!(!hits.is_empty());
    assert_eq!(
        hits[0].movie.tconst, "tt0468569",
        "The Dark Knight must be #1 over obscure 5-vote film despite typo!"
    );

    // 6. Soviet movie with region "SU": "Восхождение"
    let hits = engine.search(&SearchParams {
        query: "Восхождение".to_string(),
        limit: 5,
        ..Default::default()
    })?;
    assert!(!hits.is_empty());
    assert_eq!(hits[0].movie.tconst, "tt0075314");

    // 7. Year filter
    let hits = engine.search(&SearchParams {
        query: "Матрица".to_string(),
        year_from: Some(1990),
        year_to: Some(2000),
        limit: 5,
        ..Default::default()
    })?;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].movie.year, Some(1999));

    Ok(())
}

#[tokio::test]
async fn test_axum_http_endpoints() -> anyhow::Result<()> {
    let (_temp_dir, manager) = setup_test_environment()?;

    let config = Config::default();
    let pipeline = IngestionPipeline::new(config.clone());
    let poster_service = imdb_indexer::poster::PosterService::new(config.tmdb.clone());
    let state = AppState {
        config,
        manager: Arc::new(RwLock::new(manager)),
        pipeline: Arc::new(pipeline),
        is_indexing: Arc::new(AtomicBool::new(false)),
        poster_service: Arc::new(poster_service),
    };

    let router = create_router(state);

    // 1. Test GET /health
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // 2. Test GET /status
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["status"], "ready");
    assert_eq!(json["is_indexing"], false);
    assert!(json["total_documents"].as_u64().unwrap() >= 10);

    // 3. Test GET /search?q=бойцовский%20клуб
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/search?q=%D0%B1%D0%BE%D0%B9%D1%86%D0%BE%D0%B2%D1%81%D0%BA%D0%B8%D0%B9%20%D0%BA%D0%BB%D1%83%D0%B1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["total_hits"].as_u64().unwrap(), 1);
    assert_eq!(json["hits"][0]["movie"]["tconst"], "tt0137523");
    assert_eq!(json["hits"][0]["movie"]["title_ru"], "Бойцовский клуб");

    // 4. Test GET /search?q=fight%20club (English original title)
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/search?q=fight%20club")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["hits"][0]["movie"]["tconst"], "tt0137523");

    Ok(())
}
