use anyhow::Result;
use imdb_indexer::api::{create_router, AppState};
use imdb_indexer::config::Config;
use imdb_indexer::index::manager::IndexManager;
use imdb_indexer::ingestion::pipeline::IngestionPipeline;
use imdb_indexer::scheduler::UpdateScheduler;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,imdb_indexer=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!("Starting imdb-indexer service...");

    // Load configuration
    let config_path = std::env::var("CONFIG_PATH").unwrap_or_else(|_| "config.yaml".to_string());
    let mut config = Config::load_or_default(&config_path);

    // Allow overriding storage and cache directories via environment variables
    if let Ok(data_dir) = std::env::var("DATA_DIR") {
        if !data_dir.trim().is_empty() {
            config.storage.data_dir = std::path::PathBuf::from(data_dir);
        }
    }
    if let Ok(tmdb_cache) = std::env::var("TMDB_CACHE_DIR") {
        if !tmdb_cache.trim().is_empty() {
            config.tmdb.cache_dir = std::path::PathBuf::from(tmdb_cache);
        }
    }
    if let Ok(tmdb_key) = std::env::var("TMDB_API_KEY") {
        if !tmdb_key.trim().is_empty() {
            config.tmdb.api_key = tmdb_key.trim().to_string();
        }
    }

    // Initialize index directory
    let indices_dir = config.storage.data_dir.join("indices");
    let manager = IndexManager::open_or_create(&indices_dir)?;

    let pipeline = IngestionPipeline::new(config.clone());
    let poster_service = imdb_indexer::poster::PosterService::new(config.tmdb.clone());

    let state = AppState {
        config: config.clone(),
        manager: Arc::new(RwLock::new(manager)),
        pipeline: Arc::new(pipeline),
        is_indexing: Arc::new(AtomicBool::new(false)),
        poster_service: Arc::new(poster_service),
    };

    // Start background update scheduler
    UpdateScheduler::start(state.clone());

    // Create Axum HTTP router
    let app = create_router(state);

    let bind_addr = format!("{}:{}", config.server.host, config.server.port);
    info!("Server listening on http://{}", bind_addr);

    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("imdb-indexer service stopped.");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!("Received Ctrl+C, shutting down gracefully...");
        },
        _ = terminate => {
            info!("Received terminate signal, shutting down gracefully...");
        },
    }
}
