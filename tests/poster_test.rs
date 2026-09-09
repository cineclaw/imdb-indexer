use axum::body::Body;
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE, ETAG, IF_NONE_MATCH, LAST_MODIFIED};
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
use std::fs;
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
async fn test_poster_cdn_fallback_svg() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let mut config = Config::default();
    config.storage.data_dir = temp_dir.path().to_path_buf();
    config.tmdb.cache_dir = temp_dir.path().join("posters");

    let manager = IndexManager::open_or_create(temp_dir.path().join("indices"))?;
    let pipeline = IngestionPipeline::new(config.clone());
    let poster_service = PosterService::new(config.tmdb.clone());

    let state = AppState {
        config,
        manager: Arc::new(RwLock::new(manager)),
        pipeline: Arc::new(pipeline),
        is_indexing: Arc::new(AtomicBool::new(false)),
        poster_service: Arc::new(poster_service),
    };

    let router = create_router(state);

    // Request poster when not in cache and no TMDB key
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
    assert!(response.headers().get(CACHE_CONTROL).unwrap().to_str()?.contains("no-cache"));

    let body = response.into_body().collect().await?.to_bytes();
    let body_str = std::str::from_utf8(&body)?;
    assert!(body_str.contains("<svg"));
    assert!(body_str.contains("tt0111161"));
    assert!(body_str.contains("Нет постера"));

    Ok(())
}

#[tokio::test]
async fn test_poster_cdn_cached_file_and_304_not_modified() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let mut config = Config::default();
    config.storage.data_dir = temp_dir.path().to_path_buf();
    config.tmdb.cache_dir = temp_dir.path().join("posters");

    let manager = IndexManager::open_or_create(temp_dir.path().join("indices"))?;
    let pipeline = IngestionPipeline::new(config.clone());
    let poster_service = PosterService::new(config.tmdb.clone());

    // Pre-populate mock cached poster on disk: data/posters/61/tt0111161_w185.jpg
    let poster_path = poster_service.get_poster_path("tt0111161", "w185");
    fs::create_dir_all(poster_path.parent().unwrap())?;
    let dummy_jpeg_bytes = b"\xFF\xD8\xFF\xE0\x00\x10JFIF\x00mock_poster_data";
    fs::write(&poster_path, dummy_jpeg_bytes)?;

    let state = AppState {
        config,
        manager: Arc::new(RwLock::new(manager)),
        pipeline: Arc::new(pipeline),
        is_indexing: Arc::new(AtomicBool::new(false)),
        poster_service: Arc::new(poster_service),
    };

    let router = create_router(state);

    // 1. Initial Request (should return 200 OK + cached image)
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/poster/tt0111161?size=w185")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get(CONTENT_TYPE).unwrap(), "image/jpeg");
    let cache_control = response.headers().get(CACHE_CONTROL).unwrap().to_str()?;
    assert!(cache_control.contains("max-age=2592000"));
    assert!(cache_control.contains("immutable"));

    let etag = response.headers().get(ETAG).unwrap().to_str()?.to_string();
    assert!(etag.starts_with("W/\""));
    assert!(response.headers().contains_key(LAST_MODIFIED));

    let body = response.into_body().collect().await?.to_bytes();
    assert_eq!(&body[..], dummy_jpeg_bytes);

    // 2. Conditional Request with If-None-Match (should return 304 NOT MODIFIED with 0 bytes body!)
    let response_304 = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/poster/tt0111161?size=w185")
                .header(IF_NONE_MATCH, &etag)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response_304.status(), StatusCode::NOT_MODIFIED);
    assert_eq!(response_304.headers().get(ETAG).unwrap(), etag.as_str());
    let body_304 = response_304.into_body().collect().await?.to_bytes();
    assert!(body_304.is_empty(), "304 response body must be 0 bytes!");

    // 3. Request with .jpg extension alias: /poster/tt0111161.jpg
    let response_ext = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/poster/tt0111161.jpg?size=w185")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response_ext.status(), StatusCode::OK);
    assert_eq!(response_ext.headers().get(CONTENT_TYPE).unwrap(), "image/jpeg");

    Ok(())
}
