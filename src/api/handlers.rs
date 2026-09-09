use crate::config::Config;
use crate::index::manager::IndexManager;
use crate::ingestion::pipeline::IngestionPipeline;
use crate::search::{SearchEngine, SearchParams};
use crate::poster::PosterService;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE, ETAG, EXPIRES, IF_NONE_MATCH, LAST_MODIFIED, PRAGMA};
use axum::http::{HeaderMap, Response, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::io::ReaderStream;

#[derive(Clone)]
pub struct AppState {
    pub config: Config,
    pub manager: Arc<RwLock<IndexManager>>,
    pub pipeline: Arc<IngestionPipeline>,
    pub is_indexing: Arc<AtomicBool>,
    pub poster_service: Arc<PosterService>,
}

#[derive(Debug, Deserialize)]
pub struct SearchQuery {
    pub q: String,
    pub limit: Option<usize>,
    pub r#type: Option<String>,
    pub year_from: Option<u64>,
    pub year_to: Option<u64>,
    pub min_votes: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct SearchResponse {
    pub query: String,
    pub total_hits: usize,
    pub took_ms: f64,
    pub hits: Vec<crate::search::SearchHit>,
}

#[derive(Debug, Serialize)]
pub struct StatusResponse {
    pub status: String,
    pub service: String,
    pub version: String,
    pub is_indexing: bool,
    pub total_documents: u64,
    pub downloader_state: crate::downloader::DownloaderState,
}

#[derive(Debug, Deserialize)]
pub struct UpdateQuery {
    pub force: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct ActionResponse {
    pub status: String,
    pub message: String,
}

pub async fn health_check() -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "ok",
        "service": "imdb-indexer",
        "version": env!("CARGO_PKG_VERSION")
    }))
}

pub async fn get_status(State(state): State<AppState>) -> impl IntoResponse {
    let manager = state.manager.read().await;
    let searcher = manager.reader().searcher();
    let total_documents = searcher.num_docs();

    let is_indexing = state.is_indexing.load(Ordering::SeqCst);
    let downloader_state = state.pipeline.downloader().get_state();

    Json(StatusResponse {
        status: "ready".to_string(),
        service: "imdb-indexer".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        is_indexing,
        total_documents,
        downloader_state,
    })
}

pub async fn search_movies(
    State(state): State<AppState>,
    Query(query): Query<SearchQuery>,
) -> impl IntoResponse {
    let start_time = std::time::Instant::now();

    let limit = query
        .limit
        .unwrap_or(state.config.search.default_limit)
        .clamp(1, state.config.search.max_limit);

    let params = SearchParams {
        query: query.q.clone(),
        limit,
        title_type: query.r#type,
        year_from: query.year_from,
        year_to: query.year_to,
        min_votes: query.min_votes,
        popularity_boost_weight: state.config.search.popularity_boost_weight,
        rating_boost_weight: state.config.search.rating_boost_weight,
    };

    let manager = state.manager.read().await;
    let engine = SearchEngine::new(manager.reader(), manager.schema().clone());

    match engine.search(&params) {
        Ok(hits) => {
            let took_ms = start_time.elapsed().as_secs_f64() * 1000.0;
            let total_hits = hits.len();
            Json(SearchResponse {
                query: query.q,
                total_hits,
                took_ms,
                hits,
            })
            .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": e.to_string(),
                "query": query.q
            })),
        )
            .into_response(),
    }
}

pub async fn trigger_update(
    State(state): State<AppState>,
    Query(update_query): Query<UpdateQuery>,
) -> impl IntoResponse {
    if state
        .is_indexing
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return (
            StatusCode::CONFLICT,
            Json(ActionResponse {
                status: "busy".to_string(),
                message: "Indexing pipeline is already running in background".to_string(),
            }),
        );
    }

    let force = update_query.force.unwrap_or(false);
    let state_clone = state.clone();

    // Spawn background indexing task so API response returns immediately
    tokio::spawn(async move {
        let is_indexing_flag = state_clone.is_indexing.clone();
        tracing::info!("Background indexing triggered (force: {})", force);

        let mut manager = state_clone.manager.write().await;
        let result = state_clone.pipeline.run_indexing(&mut manager, force).await;

        match result {
            Ok(updated) => {
                tracing::info!("Indexing finished (updated: {})", updated);
            }
            Err(e) => {
                tracing::error!("Indexing pipeline failed: {}", e);
            }
        }

        is_indexing_flag.store(false, Ordering::SeqCst);
    });

    (
        StatusCode::ACCEPTED,
        Json(ActionResponse {
            status: "started".to_string(),
            message: "IMDb dump download and indexing started in background".to_string(),
        }),
    )
}

#[derive(Debug, Deserialize)]
pub struct PosterQuery {
    pub size: Option<String>,
}

pub async fn get_poster_handler(
    State(state): State<AppState>,
    Path(tconst_raw): Path<String>,
    Query(query): Query<PosterQuery>,
    headers: HeaderMap,
) -> Response<Body> {
    let tconst = tconst_raw.trim_end_matches(".jpg");

    // 1. Fetch or load cached poster
    let poster_meta = state
        .poster_service
        .get_or_fetch_poster(tconst, query.size.as_deref())
        .await;

    if let Some(meta) = poster_meta {
        // Check conditional headers for 304 Not Modified
        if let Some(if_none_match) = headers.get(IF_NONE_MATCH).and_then(|h| h.to_str().ok()) {
            if if_none_match.trim() == meta.etag.trim() {
                return Response::builder()
                    .status(StatusCode::NOT_MODIFIED)
                    .header(ETAG, meta.etag)
                    .header(CACHE_CONTROL, "public, max-age=2592000, immutable")
                    .body(Body::empty())
                    .unwrap_or_default();
            }
        }

        // Stream file directly from disk to socket with Zero-RAM heap allocation!
        match tokio::fs::File::open(&meta.path).await {
            Ok(file) => {
                let stream = ReaderStream::new(file);
                let last_modified_http = chrono::DateTime::<chrono::Utc>::from(meta.mtime_system).to_rfc2822();

                Response::builder()
                    .status(StatusCode::OK)
                    .header(CONTENT_TYPE, "image/jpeg")
                    .header(CACHE_CONTROL, "public, max-age=2592000, immutable")
                    .header(ETAG, meta.etag)
                    .header(LAST_MODIFIED, last_modified_http)
                    .body(Body::from_stream(stream))
                    .unwrap_or_default()
            }
            Err(e) => {
                tracing::warn!("Failed to open poster file {:?}: {}", meta.path, e);
                render_svg_fallback(tconst)
            }
        }
    } else {
        // Fallback to SVG placeholder
        render_svg_fallback(tconst)
    }
}

fn render_svg_fallback(tconst: &str) -> Response<Body> {
    let svg = PosterService::render_svg_placeholder(tconst);
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "image/svg+xml; charset=utf-8")
        .header(CACHE_CONTROL, "no-cache, no-store, must-revalidate")
        .header(PRAGMA, "no-cache")
        .header(EXPIRES, "0")
        .body(Body::from(svg))
        .unwrap_or_default()
}

pub async fn get_series_seasons_handler(
    State(state): State<AppState>,
    Path(tconst): Path<String>,
) -> impl IntoResponse {
    match state.poster_service.get_series_seasons(&tconst).await {
        Ok(Some(seasons_data)) => Json(seasons_data).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error": "Series not found on TMDB",
                "tconst": tconst
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": format!("Failed to fetch seasons: {}", e),
                "tconst": tconst
            })),
        )
            .into_response(),
    }
}

pub async fn get_series_episodes_handler(
    State(state): State<AppState>,
    Path(tconst): Path<String>,
) -> impl IntoResponse {
    match state.poster_service.get_series_episodes(&tconst).await {
        Ok(Some(episodes_data)) => Json(episodes_data).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error": "Episodes not found on TMDB",
                "tconst": tconst
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": format!("Failed to fetch episodes: {}", e),
                "tconst": tconst
            })),
        )
            .into_response(),
    }
}

pub async fn get_movie_metadata_handler(
    State(state): State<AppState>,
    Path(tconst): Path<String>,
) -> impl IntoResponse {
    match state.poster_service.get_movie_metadata(&tconst).await {
        Ok(Some(movie_data)) => Json(movie_data).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error": "Movie not found on TMDB",
                "tconst": tconst
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": format!("Failed to fetch movie metadata: {}", e),
                "tconst": tconst
            })),
        )
            .into_response(),
    }
}

pub async fn get_person_handler(
    State(state): State<AppState>,
    Path(person_id): Path<u64>,
) -> impl IntoResponse {
    match state.poster_service.get_person_details(person_id).await {
        Ok(Some(person_data)) => Json(person_data).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error": "Person not found on TMDB",
                "person_id": person_id
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": format!("Failed to fetch person details: {}", e),
                "person_id": person_id
            })),
        )
            .into_response(),
    }
}

pub async fn resolve_tmdb_handler(
    State(state): State<AppState>,
    Path((media_type, tmdb_id)): Path<(String, u64)>,
) -> impl IntoResponse {
    match state.poster_service.resolve_tmdb_media(&media_type, tmdb_id).await {
        Ok(Some(movie_doc)) => Json(movie_doc).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error": "Media not found on TMDB",
                "media_type": media_type,
                "tmdb_id": tmdb_id
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": format!("Failed to resolve TMDB media: {}", e),
                "media_type": media_type,
                "tmdb_id": tmdb_id
            })),
        )
            .into_response(),
    }
}

pub async fn get_home_feeds_handler(
    State(state): State<AppState>,
) -> impl IntoResponse {
    match state.poster_service.get_home_feeds().await {
        Ok(feeds) => Json(feeds).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": format!("Failed to fetch home feeds: {}", e)
            })),
        )
            .into_response(),
    }
}

#[derive(Debug, serde::Deserialize)]
pub struct ShelfPageQuery {
    pub page: Option<u32>,
}

pub async fn get_shelf_page_handler(
    State(state): State<AppState>,
    Path(shelf_id): Path<String>,
    Query(query): Query<ShelfPageQuery>,
) -> impl IntoResponse {
    let page = query.page.unwrap_or(1).max(1);
    match state.poster_service.get_shelf_page(&shelf_id, page).await {
        Ok(Some(shelf)) => Json(shelf).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": format!("Shelf '{}' not found", shelf_id) })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": format!("Failed to fetch shelf page: {}", e)
            })),
        )
            .into_response(),
    }
}




