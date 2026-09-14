use axum::body::Body;
use axum::http::header::CONTENT_TYPE;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use imdb_indexer::api::{create_router, AppState};
use imdb_indexer::config::Config;
use imdb_indexer::index::manager::IndexManager;
use imdb_indexer::ingestion::IngestionPipeline;
use imdb_indexer::poster::{
    normalize_poster_size, PosterService, POSTER_SIZE_LG, POSTER_SIZE_MD, POSTER_SIZE_ORIG,
    POSTER_SIZE_SM, POSTER_SIZE_XL, POSTER_SIZE_XS,
};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::sync::RwLock;
use tower::ServiceExt;

#[test]
fn test_poster_size_normalization() {
    assert_eq!(normalize_poster_size(Some("xs"), "w185"), POSTER_SIZE_XS);
    assert_eq!(normalize_poster_size(Some("w92"), "w185"), POSTER_SIZE_XS);
    assert_eq!(normalize_poster_size(Some("sm"), "w185"), POSTER_SIZE_SM);
    assert_eq!(normalize_poster_size(Some("w154"), "w185"), POSTER_SIZE_SM);
    assert_eq!(normalize_poster_size(Some("md"), "w185"), POSTER_SIZE_MD);
    assert_eq!(normalize_poster_size(Some("w185"), "w185"), POSTER_SIZE_MD);
    assert_eq!(normalize_poster_size(Some("lg"), "w185"), POSTER_SIZE_LG);
    assert_eq!(normalize_poster_size(Some("w342"), "w185"), POSTER_SIZE_LG);
    assert_eq!(normalize_poster_size(Some("xl"), "w185"), POSTER_SIZE_XL);
    assert_eq!(normalize_poster_size(Some("w500"), "w185"), POSTER_SIZE_XL);
    assert_eq!(normalize_poster_size(Some("original"), "w185"), POSTER_SIZE_ORIG);
    assert_eq!(normalize_poster_size(None, "w185"), POSTER_SIZE_MD);
}

#[tokio::test]
async fn test_poster_fallback_svg() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let mut config = Config::default();
    config.storage.data_dir = temp_dir.path().to_path_buf();

    let manager = IndexManager::open_or_create(temp_dir.path().join("indices"))?;
    let pipeline = IngestionPipeline::new(config.clone());
    let poster_service = PosterService::new(config.tmdb.clone(), None);

    let state = AppState {
        config,
        manager: Arc::new(RwLock::new(manager)),
        pipeline: Arc::new(pipeline),
        is_indexing: Arc::new(AtomicBool::new(false)),
        poster_service: Arc::new(poster_service),
    };

    let router = create_router(state);

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/poster/tt0111161")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "image/svg+xml; charset=utf-8"
    );

    let body = response.into_body().collect().await?.to_bytes();
    let body_str = std::str::from_utf8(&body)?;
    assert!(body_str.contains("<svg"));
    assert!(body_str.contains("tt0111161"));
    assert!(body_str.contains("Нет постера"));

    Ok(())
}

#[tokio::test]
async fn test_redb_persistent_poster_cache() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("poster_paths.redb");

    let mut config = Config::default();
    config.storage.data_dir = temp_dir.path().to_path_buf();

    // 1. First service instance: save paths into redb
    {
        let poster_service = PosterService::new(config.tmdb.clone(), Some(db_path.clone()));
        poster_service.save_paths_to_cache(
            "tt1375666",
            Some("/piQXcdOGgv1O9HQ07pI0tnjkGJw.jpg"),
            Some("/ii8QGacT3MXESqBckQlyrATY0lT.jpg"),
        );

        let cached = poster_service.get_cached_paths("tt1375666");
        assert!(cached.is_some());
        let (poster, backdrop) = cached.unwrap();
        assert_eq!(poster.as_deref(), Some("/piQXcdOGgv1O9HQ07pI0tnjkGJw.jpg"));
        assert_eq!(backdrop.as_deref(), Some("/ii8QGacT3MXESqBckQlyrATY0lT.jpg"));
    }

    // 2. Second service instance (fresh memory, simulates server restart): read from persistent redb!
    {
        let poster_service_restart = PosterService::new(config.tmdb.clone(), Some(db_path.clone()));
        let cached = poster_service_restart.get_cached_paths("tt1375666");
        assert!(cached.is_some(), "Path must persist across service restarts in redb!");
        let (poster, backdrop) = cached.unwrap();
        assert_eq!(poster.as_deref(), Some("/piQXcdOGgv1O9HQ07pI0tnjkGJw.jpg"));
        assert_eq!(backdrop.as_deref(), Some("/ii8QGacT3MXESqBckQlyrATY0lT.jpg"));
    }

    Ok(())
}

