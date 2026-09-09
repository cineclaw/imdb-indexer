use crate::api::handlers::{
    get_home_feeds_handler, get_movie_metadata_handler, get_person_handler, get_poster_handler,
    get_series_episodes_handler, get_series_seasons_handler, get_shelf_page_handler, get_status,
    health_check, resolve_tmdb_handler, search_movies, trigger_update, AppState,
};
use axum::routing::{get, post};
use axum::Router;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;

pub fn create_router(state: AppState) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    Router::new()
        .route("/health", get(health_check))
        .route("/status", get(get_status))
        .route("/search", get(search_movies))
        .route("/feeds", get(get_home_feeds_handler))
        .route("/api/feeds", get(get_home_feeds_handler))
        .route("/feeds/:shelf_id", get(get_shelf_page_handler))
        .route("/api/feeds/:shelf_id", get(get_shelf_page_handler))
        .route("/poster/:tconst", get(get_poster_handler))
        .route("/movie/:tconst/metadata", get(get_movie_metadata_handler))
        .route("/api/movie/:tconst/metadata", get(get_movie_metadata_handler))
        .route("/series/:tconst/seasons", get(get_series_seasons_handler))
        .route("/api/series/:tconst/seasons", get(get_series_seasons_handler))
        .route("/series/:tconst/episodes", get(get_series_episodes_handler))
        .route("/api/series/:tconst/episodes", get(get_series_episodes_handler))
        .route("/person/:person_id", get(get_person_handler))
        .route("/api/person/:person_id", get(get_person_handler))
        .route("/tmdb/:media_type/:tmdb_id/movie", get(resolve_tmdb_handler))
        .route("/api/tmdb/:media_type/:tmdb_id/movie", get(resolve_tmdb_handler))
        .route("/api/index/update", post(trigger_update))
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
