use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub imdb: ImdbConfig,
    pub indexing: IndexingConfig,
    pub search: SearchConfig,
    #[serde(default)]
    pub tmdb: TmdbConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TmdbConfig {
    pub api_key: String,
    pub default_size: String,
    pub cache_dir: PathBuf,
    pub cache_ttl_days: u64,
    pub negative_cache_hours: u64,
}

impl Default for TmdbConfig {
    fn default() -> Self {
        let api_key = std::env::var("TMDB_API_KEY").unwrap_or_default();
        Self {
            api_key,
            default_size: "w185".to_string(),
            cache_dir: PathBuf::from("./data/posters"),
            cache_ttl_days: 30,
            negative_cache_hours: 24,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub data_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImdbConfig {
    pub base_url: String,
    pub auto_update: bool,
    pub check_interval_hours: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoiseFilterConfig {
    pub enabled: bool,
    pub min_votes: u32,
    pub filter_empty_titles: bool,
    pub filter_zero_votes_no_year: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexingConfig {
    pub writer_memory_budget_mb: usize,
    pub batch_size: usize,
    /// List of allowed titleTypes. Empty means ALL types.
    pub allowed_title_types: Vec<String>,
    pub noise_filter: NoiseFilterConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchConfig {
    pub default_limit: usize,
    pub max_limit: usize,
    pub popularity_boost_weight: f64,
    pub rating_boost_weight: f64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                host: "0.0.0.0".to_string(),
                port: 8090,
            },
            storage: StorageConfig {
                data_dir: PathBuf::from("./data"),
            },
            imdb: ImdbConfig {
                base_url: "https://datasets.imdbws.com".to_string(),
                auto_update: true,
                check_interval_hours: 24,
            },
            indexing: IndexingConfig {
                writer_memory_budget_mb: 50, // strictly low memory!
                batch_size: 10_000,
                allowed_title_types: vec![], // Empty = all types as requested
                noise_filter: NoiseFilterConfig {
                    enabled: true,
                    min_votes: 0,
                    filter_empty_titles: true,
                    filter_zero_votes_no_year: true,
                },
            },
            search: SearchConfig {
                default_limit: 20,
                max_limit: 100,
                popularity_boost_weight: 1.5,
                rating_boost_weight: 0.5,
            },
            tmdb: TmdbConfig::default(),
        }
    }
}

impl Config {
    pub fn load_or_default<P: AsRef<Path>>(path: P) -> Self {
        let path = path.as_ref();
        if path.exists() {
            match std::fs::read_to_string(path) {
                Ok(content) => match serde_yaml::from_str::<Config>(&content) {
                    Ok(mut cfg) => {
                        if cfg.tmdb.api_key.is_empty() {
                            if let Ok(env_key) = std::env::var("TMDB_API_KEY") {
                                cfg.tmdb.api_key = env_key;
                            }
                        }
                        tracing::info!("Loaded config from {:?}", path);
                        return cfg;
                    }
                    Err(e) => {
                        tracing::warn!("Failed to parse config file {:?}: {}, using defaults", path, e);
                    }
                },
                Err(e) => {
                    tracing::warn!("Failed to read config file {:?}: {}, using defaults", path, e);
                }
            }
        }
        let default_cfg = Self::default();
        // Save default config if not exists
        if let Ok(content) = serde_yaml::to_string(&default_cfg) {
            let _ = std::fs::write(path, content);
        }
        default_cfg
    }
}
