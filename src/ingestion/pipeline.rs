use crate::config::Config;
use crate::downloader::{ImdbDownloader, DUMP_AKAS, DUMP_BASICS, DUMP_RATINGS};
use crate::index::manager::IndexManager;
use crate::index::schema::MovieDoc;
use crate::ingestion::temp_store::{parse_tconst_id, TempIngestStore};
use anyhow::{Context, Result};
use flate2::read::GzDecoder;
use std::fs::{self, File};
use std::io::BufReader;
use std::path::Path;
use tracing::info;

pub struct IngestionPipeline {
    config: Config,
    downloader: ImdbDownloader,
}

impl IngestionPipeline {
    pub fn new(config: Config) -> Self {
        let downloader = ImdbDownloader::new(
            &config.imdb.base_url,
            &config.storage.data_dir,
        );
        Self { config, downloader }
    }

    pub fn downloader(&self) -> &ImdbDownloader {
        &self.downloader
    }

    /// Execute the full ingestion pipeline with streaming to ensure minimal RAM usage
    pub async fn run_indexing(&self, manager: &mut IndexManager, force: bool) -> Result<bool> {
        info!("=== Starting IMDb Ingestion Pipeline ===");

        // 1. Download or verify dumps
        let updated = self.downloader.download_all(force).await?;
        if !updated && !force {
            info!("No dump updates found and force=false. Index is already up to date.");
            return Ok(false);
        }

        let temp_db_path = self.config.storage.data_dir.join("temp_ingest.redb");
        if temp_db_path.exists() {
            let _ = fs::remove_file(&temp_db_path);
        }

        info!("Creating temporary disk KV store at {:?}", temp_db_path);
        let temp_store = TempIngestStore::create(&temp_db_path)?;

        // 2. Stream title.ratings.tsv.gz
        let ratings_path = self.downloader.get_dump_path(DUMP_RATINGS);
        self.stream_ratings(&ratings_path, &temp_store)?;

        // 3. Stream title.akas.tsv.gz (Russian localizations only)
        let akas_path = self.downloader.get_dump_path(DUMP_AKAS);
        self.stream_russian_akas(&akas_path, &temp_store)?;

        // 4. Stream title.basics.tsv.gz and index into Tantivy
        let basics_path = self.downloader.get_dump_path(DUMP_BASICS);
        self.stream_basics_into_tantivy(&basics_path, &temp_store, manager)?;

        // Clean up temporary database
        drop(temp_store);
        let _ = fs::remove_file(&temp_db_path);
        let _ = self.downloader.mark_indexed();

        info!("=== IMDb Ingestion Pipeline Finished Successfully! ===");
        Ok(true)
    }

    fn stream_ratings(&self, path: &Path, temp_store: &TempIngestStore) -> Result<()> {
        info!("Streaming ratings from {:?}...", path);
        let file = File::open(path).with_context(|| format!("Failed to open {:?}", path))?;
        let gz = GzDecoder::new(BufReader::with_capacity(512 * 1024, file));
        let mut rdr = csv::ReaderBuilder::new()
            .delimiter(b'\t')
            .has_headers(true)
            .flexible(true)
            .from_reader(BufReader::with_capacity(512 * 1024, gz));

        let mut batch: Vec<(u32, u8, u32)> = Vec::with_capacity(10_000);
        let mut total: u64 = 0;

        let mut record = csv::ByteRecord::new();
        while rdr.read_byte_record(&mut record)? {
            if record.len() < 3 {
                continue;
            }
            let tconst = std::str::from_utf8(&record[0]).unwrap_or_default();
            if let Some(id) = parse_tconst_id(tconst) {
                let rating_str = std::str::from_utf8(&record[1]).unwrap_or_default();
                let votes_str = std::str::from_utf8(&record[2]).unwrap_or_default();

                let rating_f: f32 = rating_str.parse().unwrap_or(0.0);
                let rating_u8: u8 = (rating_f * 10.0).round().clamp(0.0, 100.0) as u8;
                let votes: u32 = votes_str.parse().unwrap_or(0);

                batch.push((id, rating_u8, votes));
                total += 1;

                if batch.len() >= 10_000 {
                    temp_store.insert_ratings_batch(&batch)?;
                    batch.clear();
                }
            }
        }

        if !batch.is_empty() {
            temp_store.insert_ratings_batch(&batch)?;
            batch.clear();
        }

        info!("Processed and stored {} ratings.", total);
        Ok(())
    }

    fn stream_russian_akas(&self, path: &Path, temp_store: &TempIngestStore) -> Result<()> {
        info!("Streaming Russian titles from {:?}...", path);
        let file = File::open(path).with_context(|| format!("Failed to open {:?}", path))?;
        let gz = GzDecoder::new(BufReader::with_capacity(512 * 1024, file));
        let mut rdr = csv::ReaderBuilder::new()
            .delimiter(b'\t')
            .has_headers(true)
            .flexible(true)
            .from_reader(BufReader::with_capacity(512 * 1024, gz));

        let mut batch: Vec<(u32, String)> = Vec::with_capacity(10_000);
        let mut total_ru: u64 = 0;
        let mut total_scanned: u64 = 0;

        let mut record = csv::ByteRecord::new();
        while rdr.read_byte_record(&mut record)? {
            total_scanned += 1;
            if record.len() < 5 {
                continue;
            }

            // Columns: 0:titleId, 1:ordering, 2:title, 3:region, 4:language
            let region = &record[3];
            let language = &record[4];

            let is_ru = region == b"RU" || region == b"SU" || language == b"ru";
            if !is_ru {
                continue;
            }

            let tconst = std::str::from_utf8(&record[0]).unwrap_or_default();
            if let Some(id) = parse_tconst_id(tconst) {
                let title = std::str::from_utf8(&record[2]).unwrap_or_default();
                if !title.is_empty() && title != "\\N" {
                    batch.push((id, title.to_string()));
                    total_ru += 1;

                    if batch.len() >= 10_000 {
                        temp_store.insert_akas_batch(&batch)?;
                        batch.clear();
                    }
                }
            }
        }

        if !batch.is_empty() {
            temp_store.insert_akas_batch(&batch)?;
            batch.clear();
        }

        info!(
            "Scanned {} rows, indexed {} Russian localizations.",
            total_scanned, total_ru
        );
        Ok(())
    }

    fn stream_basics_into_tantivy(
        &self,
        path: &Path,
        temp_store: &TempIngestStore,
        manager: &mut IndexManager,
    ) -> Result<()> {
        info!("Streaming basics into Tantivy index...");

        let memory_budget_mb = self.config.indexing.writer_memory_budget_mb;
        info!("Configuring Tantivy writer with memory budget: {} MB", memory_budget_mb);
        let (_new_index, mut writer, gen_path) = manager.create_new_generation(memory_budget_mb)?;
        let schema = manager.schema().clone();

        let reader = temp_store.begin_reader()?;

        let file = File::open(path).with_context(|| format!("Failed to open {:?}", path))?;
        let gz = GzDecoder::new(BufReader::with_capacity(512 * 1024, file));
        let mut rdr = csv::ReaderBuilder::new()
            .delimiter(b'\t')
            .has_headers(true)
            .flexible(true)
            .from_reader(BufReader::with_capacity(512 * 1024, gz));

        let mut indexed_count: u64 = 0;
        let mut skipped_noise_count: u64 = 0;

        let mut record = csv::ByteRecord::new();
        while rdr.read_byte_record(&mut record)? {
            if record.len() < 9 {
                continue;
            }

            // Columns:
            // 0: tconst
            // 1: titleType
            // 2: primaryTitle
            // 3: originalTitle
            // 4: isAdult
            // 5: startYear
            // 6: endYear
            // 7: runtimeMinutes
            // 8: genres

            let tconst = std::str::from_utf8(&record[0]).unwrap_or_default();
            let title_type = std::str::from_utf8(&record[1]).unwrap_or_default();
            let primary_title = std::str::from_utf8(&record[2]).unwrap_or_default();
            let original_title = std::str::from_utf8(&record[3]).unwrap_or_default();
            let _is_adult_str = std::str::from_utf8(&record[4]).unwrap_or_default();
            let start_year_str = std::str::from_utf8(&record[5]).unwrap_or_default();
            let runtime_str = std::str::from_utf8(&record[7]).unwrap_or_default();
            let genres_str = std::str::from_utf8(&record[8]).unwrap_or_default();

            // Type filter (if configured)
            if !self.config.indexing.allowed_title_types.is_empty()
                && !self
                    .config
                    .indexing
                    .allowed_title_types
                    .iter()
                    .any(|t| t == title_type)
            {
                continue;
            }

            let id = match parse_tconst_id(tconst) {
                Some(i) => i,
                None => continue,
            };

            let (rating_info, ru_titles) = reader.lookup(id);
            let (rating, num_votes) = match rating_info {
                Some((r, v)) => (Some(r), v),
                None => (None, 0),
            };

            // Noise filter
            if self.config.indexing.noise_filter.enabled {
                let has_primary = !primary_title.is_empty() && primary_title != "\\N";
                let has_orig = !original_title.is_empty() && original_title != "\\N";
                let has_ru = ru_titles.as_ref().map_or(false, |v| !v.is_empty());
                let has_year = !start_year_str.is_empty() && start_year_str != "\\N";

                // Filter empty titles
                if self.config.indexing.noise_filter.filter_empty_titles
                    && !has_primary
                    && !has_orig
                    && !has_ru
                {
                    skipped_noise_count += 1;
                    continue;
                }

                // Filter zero votes with no year and no Russian localization
                if self.config.indexing.noise_filter.filter_zero_votes_no_year
                    && num_votes == 0
                    && !has_year
                    && !has_ru
                {
                    skipped_noise_count += 1;
                    continue;
                }

                // Min votes filter
                if num_votes < self.config.indexing.noise_filter.min_votes && !has_ru {
                    skipped_noise_count += 1;
                    continue;
                }
            }

            let year = start_year_str.parse::<u32>().ok();
            let runtime_minutes = runtime_str.parse::<u32>().ok();
            let genres = if genres_str != "\\N" {
                genres_str.split(',').map(|s| s.trim().to_string()).collect()
            } else {
                vec![]
            };

            let primary = if primary_title == "\\N" { "" } else { primary_title };
            let orig = if original_title == "\\N" { primary } else { original_title };

            let ru_list = ru_titles.unwrap_or_default();
            let first_ru = ru_list.first().cloned();

            let movie_doc = MovieDoc {
                tconst: tconst.to_string(),
                title_ru: first_ru,
                title_orig: orig.to_string(),
                title_primary: primary.to_string(),
                russian_titles: ru_list,
                year,
                title_type: title_type.to_string(),
                rating,
                num_votes,
                genres,
                runtime_minutes,
            };

            let tantivy_doc = schema.to_tantivy_doc(&movie_doc);
            writer.add_document(tantivy_doc)?;

            indexed_count += 1;
            if indexed_count % 50_000 == 0 {
                info!(
                    "Indexed {} docs into Tantivy (skipped noise: {})...",
                    indexed_count, skipped_noise_count
                );
            }
        }

        info!(
            "Committing Tantivy index (total docs: {}, skipped noise: {})...",
            indexed_count, skipped_noise_count
        );
        writer.commit()?;
        drop(writer);

        // Atomic generation activation
        manager.activate_generation(&gen_path)?;

        info!("Activated new index generation at {:?}", gen_path);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;

    fn write_mock_tsv_gz<P: AsRef<Path>>(path: P, content: &str) -> Result<()> {
        let file = File::create(path)?;
        let mut encoder = GzEncoder::new(file, Compression::default());
        encoder.write_all(content.as_bytes())?;
        encoder.finish()?;
        Ok(())
    }

    #[tokio::test]
    async fn test_ingestion_pipeline_end_to_end() -> Result<()> {
        let test_dir = std::env::temp_dir().join(format!("test_pipeline_{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis()));
        fs::create_dir_all(&test_dir)?;

        let mut config = Config::default();
        config.storage.data_dir = test_dir.clone();

        let downloads_dir = test_dir.join("downloads");
        fs::create_dir_all(&downloads_dir)?;

        // Mock title.ratings.tsv.gz (tt9999999 is unrated / 0 votes)
        let ratings_content = "tconst\taverageRating\tnumVotes\n\
                               tt0111161\t9.3\t2800000\n\
                               tt0133093\t8.7\t2050000\n";
        write_mock_tsv_gz(downloads_dir.join(DUMP_RATINGS), ratings_content)?;

        // Mock title.akas.tsv.gz
        let akas_content = "titleId\tordering\ttitle\tregion\tlanguage\ttypes\tattributes\tisOriginalTitle\n\
                            tt0111161\t1\tThe Shawshank Redemption\tUS\ten\t\\N\t\\N\t0\n\
                            tt0111161\t2\tПобег из Шоушенка\tRU\tru\t\\N\t\\N\t0\n\
                            tt0133093\t1\tМатрица\tRU\tru\t\\N\t\\N\t0\n";
        write_mock_tsv_gz(downloads_dir.join(DUMP_AKAS), akas_content)?;

        // Mock title.basics.tsv.gz
        let basics_content = "tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenres\n\
                              tt0111161\tmovie\tThe Shawshank Redemption\tThe Shawshank Redemption\t0\t1994\t\\N\t142\tDrama\n\
                              tt0133093\tmovie\tThe Matrix\tThe Matrix\t0\t1999\t\\N\t136\tAction,Sci-Fi\n\
                              tt9999999\tmovie\tNoisy Obscure\tNoisy Obscure\t0\t\\N\t\\N\t\\N\t\\N\n";
        write_mock_tsv_gz(downloads_dir.join(DUMP_BASICS), basics_content)?;

        let mut manager = IndexManager::open_or_create(test_dir.join("indices"))?;
        let pipeline = IngestionPipeline::new(config.clone());

        let temp_store = TempIngestStore::create(test_dir.join("temp.redb"))?;
        pipeline.stream_ratings(&downloads_dir.join(DUMP_RATINGS), &temp_store)?;
        pipeline.stream_russian_akas(&downloads_dir.join(DUMP_AKAS), &temp_store)?;
        pipeline.stream_basics_into_tantivy(&downloads_dir.join(DUMP_BASICS), &temp_store, &mut manager)?;

        drop(temp_store);

        let search_engine = crate::search::SearchEngine::new(manager.reader(), manager.schema().clone());

        // Search by Russian title "Побег из Шоушенка"
        let hits = search_engine.search(&crate::search::SearchParams {
            query: "побег из шоушенка".to_string(),
            limit: 5,
            ..Default::default()
        })?;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].movie.tconst, "tt0111161");
        assert_eq!(hits[0].movie.title_ru.as_deref(), Some("Побег из Шоушенка"));

        // Search by Russian title with typo: "матрицо"
        let hits2 = search_engine.search(&crate::search::SearchParams {
            query: "матрицо".to_string(),
            limit: 5,
            ..Default::default()
        })?;
        assert_eq!(hits2.len(), 1);
        assert_eq!(hits2[0].movie.tconst, "tt0133093");

        // Verify noisy movie (tt9999999) was skipped by noise filter (0 votes + no year + no ru)
        let hits3 = search_engine.search(&crate::search::SearchParams {
            query: "Noisy Obscure".to_string(),
            limit: 5,
            ..Default::default()
        })?;
        assert_eq!(hits3.len(), 0);

        let _ = fs::remove_dir_all(&test_dir);
        Ok(())
    }
}
