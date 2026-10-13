use crate::config::{KinopoiskConfig, TmdbConfig};
use crate::index::schema::MovieDoc;
use crate::search::fuzzy::SearchHit;
use anyhow::Result;
use lru::LruCache;
use redb::{Database, TableDefinition};
use serde::Deserialize;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::{Arc, Mutex as SyncMutex};
use std::time::{Duration, Instant};
use tracing::{info, warn};

pub const POSTER_SIZE_XS: &str = "w92";   // ~5-10 KB
pub const POSTER_SIZE_SM: &str = "w154";  // ~12-20 KB
pub const POSTER_SIZE_MD: &str = "w185";  // ~20-30 KB (Recommended for search UI)
pub const POSTER_SIZE_LG: &str = "w342";  // ~40-60 KB
pub const POSTER_SIZE_XL: &str = "w500";  // ~80-120 KB
pub const POSTER_SIZE_ORIG: &str = "original";

const POSTER_PATHS_TABLE: TableDefinition<&str, &str> = TableDefinition::new("poster_paths");
const BACKDROP_PATHS_TABLE: TableDefinition<&str, &str> = TableDefinition::new("backdrop_paths");

/// Normalizes requested size string or aliases to standardized TMDB size
pub fn normalize_poster_size(size_param: Option<&str>, default_size: &str) -> &'static str {
    match size_param.map(|s| s.trim().to_lowercase()).as_deref() {
        Some("xs") | Some("w92") => POSTER_SIZE_XS,
        Some("sm") | Some("w154") => POSTER_SIZE_SM,
        Some("md") | Some("w185") => POSTER_SIZE_MD,
        Some("lg") | Some("w342") => POSTER_SIZE_LG,
        Some("xl") | Some("w500") => POSTER_SIZE_XL,
        Some("orig") | Some("original") => POSTER_SIZE_ORIG,
        _ => match default_size {
            "w92" | "xs" => POSTER_SIZE_XS,
            "w154" | "sm" => POSTER_SIZE_SM,
            "w185" | "md" => POSTER_SIZE_MD,
            "w342" | "lg" => POSTER_SIZE_LG,
            "w500" | "xl" => POSTER_SIZE_XL,
            "original" => POSTER_SIZE_ORIG,
            _ => POSTER_SIZE_MD,
        },
    }
}

#[derive(Clone)]
pub struct PosterService {
    config: TmdbConfig,
    kp_config: KinopoiskConfig,
    client: reqwest::Client,
    redb: Option<Arc<Database>>,
    paths_cache: Arc<SyncMutex<LruCache<String, (Option<String>, Option<String>)>>>,
    seasons_cache: Arc<SyncMutex<LruCache<String, SeriesSeasonsResponse>>>,
    episodes_cache: Arc<SyncMutex<LruCache<String, Vec<EpisodeMetadataItem>>>>,
    movies_cache: Arc<SyncMutex<LruCache<String, MovieMetadataResponse>>>,
    persons_cache: Arc<SyncMutex<LruCache<u64, PersonDetailsResponse>>>,
    resolved_cache: Arc<SyncMutex<LruCache<String, MovieDoc>>>,
    feeds_cache: Arc<SyncMutex<HashMap<String, (Instant, Vec<FeedShelf>)>>>,
    shelf_pages_cache: Arc<SyncMutex<LruCache<String, (Instant, FeedShelf)>>>,
    trailer_stream_cache: Arc<SyncMutex<LruCache<String, (Instant, TrailerStreamResponse)>>>,
    tmdb_semaphore: Arc<tokio::sync::Semaphore>,
}

impl PosterService {
    pub fn new(config: TmdbConfig, kp_config: KinopoiskConfig, db_path: Option<PathBuf>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_default();

        let lru_capacity = NonZeroUsize::new(20_000).unwrap();
        let trailer_capacity = NonZeroUsize::new(1_000).unwrap();
        let paths_cache = Arc::new(SyncMutex::new(LruCache::new(lru_capacity)));
        let seasons_cache = Arc::new(SyncMutex::new(LruCache::new(lru_capacity)));
        let episodes_cache = Arc::new(SyncMutex::new(LruCache::new(lru_capacity)));
        let movies_cache = Arc::new(SyncMutex::new(LruCache::new(lru_capacity)));
        let persons_cache = Arc::new(SyncMutex::new(LruCache::new(lru_capacity)));
        let resolved_cache = Arc::new(SyncMutex::new(LruCache::new(lru_capacity)));
        let feeds_cache = Arc::new(SyncMutex::new(HashMap::new()));
        let shelf_pages_cache = Arc::new(SyncMutex::new(LruCache::new(lru_capacity)));
        let trailer_stream_cache = Arc::new(SyncMutex::new(LruCache::new(trailer_capacity)));
        let tmdb_semaphore = Arc::new(tokio::sync::Semaphore::new(12));

        let redb = if let Some(path) = db_path {
            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            match Database::create(&path) {
                Ok(db) => {
                    // Pre-create tables
                    if let Ok(write_txn) = db.begin_write() {
                        let _ = write_txn.open_table(POSTER_PATHS_TABLE);
                        let _ = write_txn.open_table(BACKDROP_PATHS_TABLE);
                        let _ = write_txn.commit();
                    }
                    info!("Persistent poster database opened at {:?}", path);
                    Some(Arc::new(db))
                }
                Err(e) => {
                    warn!("Failed to open poster_paths.redb at {:?}: {}", path, e);
                    None
                }
            }
        } else {
            None
        };

        Self {
            config,
            kp_config,
            client,
            redb,
            paths_cache,
            seasons_cache,
            episodes_cache,
            movies_cache,
            persons_cache,
            resolved_cache,
            feeds_cache,
            shelf_pages_cache,
            trailer_stream_cache,
            tmdb_semaphore,
        }
    }

    pub fn config(&self) -> &TmdbConfig {
        &self.config
    }

    pub fn kp_config(&self) -> &KinopoiskConfig {
        &self.kp_config
    }

    /// Read cached poster and backdrop paths from in-memory LRU or persistent redb (<1µs)
    pub fn get_cached_paths(&self, tconst: &str) -> Option<(Option<String>, Option<String>)> {
        // 1. Check in-memory LRU
        {
            let mut cache = self.paths_cache.lock().unwrap();
            if let Some(cached) = cache.get(tconst) {
                return Some(cached.clone());
            }
        }

        // 2. Check redb if available
        if let Some(ref db) = self.redb {
            if let Ok(read_txn) = db.begin_read() {
                if let Ok(p_table) = read_txn.open_table(POSTER_PATHS_TABLE) {
                    if let Ok(Some(val)) = p_table.get(tconst) {
                        let p_str = val.value().to_string();
                        let poster = if p_str.is_empty() { None } else { Some(p_str) };

                        let backdrop = if let Ok(b_table) = read_txn.open_table(BACKDROP_PATHS_TABLE) {
                            if let Ok(Some(b_val)) = b_table.get(tconst) {
                                let b_str = b_val.value().to_string();
                                if b_str.is_empty() { None } else { Some(b_str) }
                            } else {
                                None
                            }
                        } else {
                            None
                        };

                        let res = (poster, backdrop);
                        // Populate in-memory LRU
                        let mut cache = self.paths_cache.lock().unwrap();
                        cache.put(tconst.to_string(), res.clone());
                        return Some(res);
                    }
                }
            }
        }

        None
    }

    /// Save poster and backdrop paths to both in-memory LRU and persistent redb
    pub fn save_paths_to_cache(&self, tconst: &str, poster: Option<&str>, backdrop: Option<&str>) {
        let p_opt = poster.map(String::from);
        let b_opt = backdrop.map(String::from);

        // 1. Save in in-memory LRU
        {
            let mut cache = self.paths_cache.lock().unwrap();
            cache.put(tconst.to_string(), (p_opt, b_opt));
        }

        // 2. Persist to redb
        if let Some(ref db) = self.redb {
            if let Ok(write_txn) = db.begin_write() {
                let p_val = poster.unwrap_or("");
                let b_val = backdrop.unwrap_or("");
                if let Ok(mut p_table) = write_txn.open_table(POSTER_PATHS_TABLE) {
                    let _ = p_table.insert(tconst, p_val);
                }
                if let Ok(mut b_table) = write_txn.open_table(BACKDROP_PATHS_TABLE) {
                    let _ = b_table.insert(tconst, b_val);
                }
                let _ = write_txn.commit();
            }
        }
    }

    /// Single-call TMDB Find API lookup by IMDb ID (external_source=imdb_id&language=ru-RU)
    pub async fn fetch_tmdb_find_paths(&self, tconst: &str) -> Result<(Option<String>, Option<String>)> {
        if self.config.api_key.trim().is_empty() {
            return Ok((None, None));
        }

        let find_url = format!(
            "https://api.themoviedb.org/3/find/{}?external_source=imdb_id&language=ru-RU",
            tconst
        );

        let mut attempts = 0;
        loop {
            attempts += 1;
            let _permit = self.tmdb_semaphore.acquire().await;

            let mut req = self.client.get(&find_url);
            if self.config.api_key.len() > 40 {
                req = req.header("Authorization", format!("Bearer {}", self.config.api_key));
            } else {
                req = req.query(&[("api_key", &self.config.api_key)]);
            }

            let resp = match req.send().await {
                Ok(r) => r,
                Err(e) => {
                    if attempts < 3 {
                        tokio::time::sleep(Duration::from_millis(300 * attempts)).await;
                        continue;
                    }
                    return Err(e.into());
                }
            };

            if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
                if attempts < 3 {
                    tokio::time::sleep(Duration::from_millis(500 * attempts)).await;
                    continue;
                }
            }

            let resp = resp.error_for_status()?;
            let find_data: TmdbFindResponse = resp.json().await?;

            if let Some(m) = find_data.movie_results.into_iter().next() {
                return Ok((m.poster_path, m.backdrop_path));
            } else if let Some(t) = find_data.tv_results.into_iter().next() {
                return Ok((t.poster_path, t.backdrop_path));
            } else {
                return Ok((None, None));
            }
        }
    }

    /// Queries TMDB to find the best poster path (returns from cache if already known)
    pub async fn find_tmdb_poster_path(&self, tconst: &str) -> Result<Option<String>> {
        if let Some((poster, _)) = self.get_cached_paths(tconst) {
            return Ok(poster);
        }

        let (poster_opt, backdrop_opt) = self.fetch_tmdb_find_paths(tconst).await?;
        self.save_paths_to_cache(tconst, poster_opt.as_deref(), backdrop_opt.as_deref());
        Ok(poster_opt)
    }

    /// Enriches search hits concurrently in parallel (using join_all with standard timeout)
    pub async fn enrich_search_hits(&self, hits: &mut [SearchHit]) {
        let mut missing_indices = Vec::new();

        for (idx, hit) in hits.iter_mut().enumerate() {
            if let Some((poster, backdrop)) = self.get_cached_paths(&hit.movie.tconst) {
                hit.apply_paths(poster, backdrop);
            } else {
                missing_indices.push(idx);
            }
        }

        if missing_indices.is_empty() {
            return;
        }

        let futures = missing_indices.iter().map(|&idx| {
            let tconst = hits[idx].movie.tconst.clone();
            let service = self.clone();
            async move {
                let res = service.fetch_tmdb_find_paths(&tconst).await;
                (idx, tconst, res)
            }
        });

        let results = futures_util::future::join_all(futures).await;

        for (idx, tconst, res) in results {
            match res {
                Ok((poster_opt, backdrop_opt)) => {
                    self.save_paths_to_cache(
                        &tconst,
                        poster_opt.as_deref(),
                        backdrop_opt.as_deref(),
                    );
                    hits[idx].apply_paths(poster_opt, backdrop_opt);
                }
                Err(e) => {
                    warn!("Failed to fetch TMDB paths for {}: {}", tconst, e);
                }
            }
        }
    }

    /// Generates a sleek dark cinema SVG fallback placeholder
    pub fn render_svg_placeholder(tconst: &str) -> String {
        let svg = r##"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 300 450" width="100%" height="100%">
  <defs>
    <linearGradient id="bg" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0%" stop-color="#141824"/>
      <stop offset="100%" stop-color="#0b0e14"/>
    </linearGradient>
  </defs>
  <rect width="300" height="450" fill="url(#bg)"/>
  <rect x="15" y="15" width="270" height="420" rx="8" fill="none" stroke="#252c3d" stroke-width="2" stroke-dasharray="6 4"/>
  <g fill="#4e5d78" transform="translate(115, 175) scale(2.8)">
    <path d="M18 4l2 4h-3l-2-4h-2l2 4h-3l-2-4H8l2 4H7L5 4H4c-1.1 0-1.99.9-1.99 2L2 18c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V4h-4z"/>
  </g>
  <text x="150" y="270" fill="#7a8b9e" font-family="-apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif" font-size="14" font-weight="500" text-anchor="middle">Нет постера</text>
  <text x="150" y="295" fill="#424c5e" font-family="monospace" font-size="12" text-anchor="middle">__TCONST__</text>
</svg>"##;
        svg.replace("__TCONST__", tconst)
    }

    /// Fetches TV series seasons metadata from TMDB with in-memory caching
    pub async fn get_series_seasons(&self, tconst: &str) -> Result<Option<SeriesSeasonsResponse>> {
        // 1. Check in-memory cache
        {
            let mut cache = self.seasons_cache.lock().unwrap();
            if let Some(cached) = cache.get(tconst) {
                return Ok(Some(cached.clone()));
            }
        }

        if self.config.api_key.trim().is_empty() {
            warn!("TMDB API key not configured, cannot fetch seasons for {}", tconst);
            return Ok(None);
        }

        // 2. Find TMDB TV show by IMDb ID
        let find_url = format!(
            "https://api.themoviedb.org/3/find/{}?external_source=imdb_id&language=ru-RU",
            tconst
        );

        let mut req = self.client.get(&find_url);
        if self.config.api_key.len() > 40 {
            req = req.header("Authorization", format!("Bearer {}", self.config.api_key));
        } else {
            req = req.query(&[("api_key", &self.config.api_key)]);
        }

        let resp = req.send().await?.error_for_status()?;
        let find_data: TmdbFindResponse = resp.json().await?;

        let tv_item = match find_data.tv_results.into_iter().next() {
            Some(item) => item,
            None => return Ok(None),
        };

        // 3. Query TV Details from TMDB
        let details_url = format!(
            "https://api.themoviedb.org/3/tv/{}?language=ru-RU&append_to_response=images&include_image_language=ru,en,null",
            tv_item.id
        );

        let mut req_details = self.client.get(&details_url);
        if self.config.api_key.len() > 40 {
            req_details = req_details.header("Authorization", format!("Bearer {}", self.config.api_key));
        } else {
            req_details = req_details.query(&[("api_key", &self.config.api_key)]);
        }

        let resp_details = req_details.send().await?.error_for_status()?;
        let details: TmdbTvDetails = resp_details.json().await?;

        let seasons = details
            .seasons
            .into_iter()
            .map(|s| SeriesSeasonItem {
                season_number: s.season_number,
                name: s.name,
                episode_count: s.episode_count,
                air_date: s.air_date,
                poster_path: s.poster_path,
                overview: s.overview,
                vote_average: s.vote_average,
            })
            .collect();

        let logo_path = details.images.as_ref().and_then(|img| {
            if let Some(l) = img.logos.iter().find(|l| l.iso_639_1.as_deref() == Some("ru")) {
                return Some(l.file_path.clone());
            }
            if let Some(l) = img.logos.iter().find(|l| l.iso_639_1.as_deref() == Some("en")) {
                return Some(l.file_path.clone());
            }
            img.logos.first().map(|l| l.file_path.clone())
        });

        let backdrop_path = details.backdrop_path.or_else(|| {
            details.images.as_ref().and_then(|img| img.backdrops.first().map(|b| b.file_path.clone()))
        });

        let studio = details.networks.into_iter().next().map(|n| n.name);
        let genres = details.genres.into_iter().map(|g| g.name).collect();

        let response = SeriesSeasonsResponse {
            tconst: tconst.to_string(),
            tmdb_id: details.id,
            name: details.name,
            original_name: details.original_name,
            overview: details.overview,
            premiered: details.first_air_date,
            rating: details.vote_average,
            genres,
            studio,
            status: details.status,
            poster_path: details.poster_path,
            backdrop_path,
            logo_path,
            number_of_seasons: details.number_of_seasons,
            number_of_episodes: details.number_of_episodes,
            seasons,
        };

        // 4. Cache and return
        {
            let mut cache = self.seasons_cache.lock().unwrap();
            cache.put(tconst.to_string(), response.clone());
        }

        Ok(Some(response))
    }

    /// Fetches all episodes metadata across all seasons for a TV series
    pub async fn get_series_episodes(&self, tconst: &str) -> Result<Option<Vec<EpisodeMetadataItem>>> {
        {
            let mut cache = self.episodes_cache.lock().unwrap();
            if let Some(cached) = cache.get(tconst) {
                return Ok(Some(cached.clone()));
            }
        }

        let seasons_resp = match self.get_series_seasons(tconst).await? {
            Some(s) => s,
            None => return Ok(None),
        };

        let tmdb_id = seasons_resp.tmdb_id;
        let valid_seasons: Vec<_> = seasons_resp
            .seasons
            .into_iter()
            .filter(|s| s.season_number > 0)
            .collect();

        let fetches = valid_seasons.into_iter().map(|season| {
            let season_url = format!(
                "https://api.themoviedb.org/3/tv/{}/season/{}?language=ru-RU",
                tmdb_id, season.season_number
            );

            let mut req = self.client.get(&season_url);
            if self.config.api_key.len() > 40 {
                req = req.header("Authorization", format!("Bearer {}", self.config.api_key));
            } else {
                req = req.query(&[("api_key", &self.config.api_key)]);
            }

            async move {
                if let Ok(resp) = req.send().await {
                    if let Ok(detail) = resp.json::<TmdbSeasonDetailRaw>().await {
                        return detail.episodes;
                    }
                }
                Vec::new()
            }
        });

        let results = futures_util::future::join_all(fetches).await;
        let mut all_episodes = Vec::new();
        for eps in results {
            for ep in eps {
                all_episodes.push(EpisodeMetadataItem {
                    season_number: ep.season_number,
                    episode_number: ep.episode_number,
                    name: ep.name,
                    overview: ep.overview,
                    air_date: ep.air_date,
                    still_path: ep.still_path,
                    vote_average: ep.vote_average,
                    vote_count: ep.vote_count,
                    runtime: ep.runtime,
                    episode_type: ep.episode_type,
                });
            }
        }
        all_episodes.sort_by_key(|e| (e.season_number, e.episode_number));

        {
            let mut cache = self.episodes_cache.lock().unwrap();
            cache.put(tconst.to_string(), all_episodes.clone());
        }

        Ok(Some(all_episodes))
    }

    /// Fetches movie or TV series metadata from TMDB with Russian localization priority, cast, crew, trailers, and in-memory caching
    pub async fn get_movie_metadata(&self, tconst: &str) -> Result<Option<MovieMetadataResponse>> {
        {
            let mut cache = self.movies_cache.lock().unwrap();
            if let Some(cached) = cache.get(tconst) {
                return Ok(Some(cached.clone()));
            }
        }

        if self.config.api_key.trim().is_empty() {
            warn!("TMDB API key not configured, cannot fetch movie metadata for {}", tconst);
            return Ok(None);
        }

        let find_url = format!(
            "https://api.themoviedb.org/3/find/{}?external_source=imdb_id&language=ru-RU",
            tconst
        );

        let mut req = self.client.get(&find_url);
        if self.config.api_key.len() > 40 {
            req = req.header("Authorization", format!("Bearer {}", self.config.api_key));
        } else {
            req = req.query(&[("api_key", &self.config.api_key)]);
        }

        let resp = req.send().await?.error_for_status()?;
        let find_data: TmdbFindResponse = resp.json().await?;

        let (media_id, is_tv) = if let Some(m) = find_data.movie_results.into_iter().next() {
            (m.id, false)
        } else if let Some(tv) = find_data.tv_results.into_iter().next() {
            (tv.id, true)
        } else {
            return Ok(None);
        };

        let details_url = if is_tv {
            format!(
                "https://api.themoviedb.org/3/tv/{}?language=ru-RU&append_to_response=aggregate_credits,credits,videos,images,recommendations,similar&include_image_language=ru,en,null&include_video_language=ru,en,null",
                media_id
            )
        } else {
            format!(
                "https://api.themoviedb.org/3/movie/{}?language=ru-RU&append_to_response=credits,videos,images,recommendations,similar,release_dates&include_image_language=ru,en,null&include_video_language=ru,en,null",
                media_id
            )
        };

        let mut req_details = self.client.get(&details_url);
        if self.config.api_key.len() > 40 {
            req_details = req_details.header("Authorization", format!("Bearer {}", self.config.api_key));
        } else {
            req_details = req_details.query(&[("api_key", &self.config.api_key)]);
        }

        let resp_details = req_details.send().await?.error_for_status()?;
        let raw_json: serde_json::Value = resp_details.json().await?;

        let title = raw_json.get("title")
            .or_else(|| raw_json.get("name"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let original_title = raw_json.get("original_title")
            .or_else(|| raw_json.get("original_name"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let mut overview = raw_json.get("overview")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .filter(|s| !s.trim().is_empty());

        // If Russian overview is missing, fallback to English overview
        if overview.is_none() {
            let en_url = format!(
                "https://api.themoviedb.org/3/{}/{}?language=en-US",
                if is_tv { "tv" } else { "movie" },
                media_id
            );
            let mut en_req = self.client.get(&en_url);
            if self.config.api_key.len() > 40 {
                en_req = en_req.header("Authorization", format!("Bearer {}", self.config.api_key));
            } else {
                en_req = en_req.query(&[("api_key", &self.config.api_key)]);
            }
            if let Ok(en_resp) = en_req.send().await {
                if let Ok(en_json) = en_resp.json::<serde_json::Value>().await {
                    overview = en_json.get("overview")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                        .filter(|s| !s.trim().is_empty());
                }
            }
        }

        let release_date = raw_json.get("release_date")
            .or_else(|| raw_json.get("first_air_date"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let year = release_date.as_ref().and_then(|d| {
            d.split('-').next().and_then(|y| y.parse::<u32>().ok())
        });

        let rating = raw_json.get("vote_average")
            .and_then(|v| v.as_f64())
            .map(|v| v as f32);

        let poster_path = raw_json.get("poster_path")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let backdrop_path = raw_json.get("backdrop_path")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let runtime_minutes = raw_json.get("runtime")
            .and_then(|v| v.as_u64())
            .map(|v| v as u32)
            .or_else(|| {
                raw_json.get("episode_run_time")
                    .and_then(|v| v.as_array())
                    .and_then(|arr| arr.first())
                    .and_then(|v| v.as_u64())
                    .map(|v| v as u32)
            });

        let genres: Vec<String> = raw_json.get("genres")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|g| g.get("name").and_then(|n| n.as_str()).map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let logo_path = raw_json.get("images")
            .and_then(|img| img.get("logos"))
            .and_then(|logos| logos.as_array())
            .and_then(|arr| {
                arr.iter().find(|l| l.get("iso_639_1").and_then(|s| s.as_str()) == Some("ru"))
                    .or_else(|| arr.iter().find(|l| l.get("iso_639_1").and_then(|s| s.as_str()) == Some("en")))
                    .or_else(|| arr.first())
                    .and_then(|l| l.get("file_path").and_then(|s| s.as_str()))
                    .map(|s| s.to_string())
            });

        let backdrops: Vec<String> = raw_json.get("images")
            .and_then(|img| img.get("backdrops"))
            .and_then(|b| b.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|item| item.get("file_path").and_then(|p| p.as_str()).map(|s| s.to_string()))
                    .take(25)
                    .collect()
            })
            .unwrap_or_default();

        let mut cast = Vec::new();
        if let Some(cast_arr) = raw_json.get("credits")
            .and_then(|c| c.get("cast"))
            .and_then(|ca| ca.as_array())
        {
            for c in cast_arr.iter().take(15) {
                if let Some(name) = c.get("name").and_then(|n| n.as_str()) {
                    cast.push(CastMember {
                        id: c.get("id").and_then(|v| v.as_u64()).unwrap_or(0),
                        name: name.to_string(),
                        character: c.get("character").and_then(|v| v.as_str()).map(|s| s.to_string()),
                        profile_path: c.get("profile_path").and_then(|v| v.as_str()).map(|s| s.to_string()),
                        order: c.get("order").and_then(|v| v.as_u64()).map(|v| v as u32),
                    });
                }
            }
        }

        let mut crew = Vec::new();
        let mut seen_crew = std::collections::HashSet::new();

        // 1. TV Series: Extract creators from top-level "created_by"
        if let Some(created_by_arr) = raw_json.get("created_by").and_then(|cb| cb.as_array()) {
            for cb in created_by_arr {
                if let Some(name) = cb.get("name").and_then(|n| n.as_str()) {
                    let key = format!("{}:Creator", name);
                    if seen_crew.insert(key) {
                        crew.push(CrewMember {
                            id: cb.get("id").and_then(|v| v.as_u64()).unwrap_or(0),
                            name: name.to_string(),
                            job: "Creator".to_string(),
                            department: Some("Writing".to_string()),
                            profile_path: cb.get("profile_path").and_then(|v| v.as_str()).map(|s| s.to_string()),
                        });
                    }
                }
            }
        }

        // 2. TV Series: Extract episodic Directors and Writers from aggregate_credits
        if is_tv {
            if let Some(agg_crew) = raw_json.get("aggregate_credits")
                .and_then(|ac| ac.get("crew"))
                .and_then(|ca| ca.as_array())
            {
                let mut dir_candidates: Vec<(u32, u64, String, Option<String>)> = Vec::new();
                let mut writer_candidates: Vec<(u32, u64, String, Option<String>)> = Vec::new();

                for cr in agg_crew {
                    let id = cr.get("id").and_then(|v| v.as_u64()).unwrap_or(0);
                    let name = match cr.get("name").and_then(|n| n.as_str()) {
                        Some(n) => n.to_string(),
                        None => continue,
                    };
                    let profile_path = cr.get("profile_path").and_then(|v| v.as_str()).map(|s| s.to_string());

                    if let Some(jobs_arr) = cr.get("jobs").and_then(|j| j.as_array()) {
                        for j in jobs_arr {
                            let job_name = j.get("job").and_then(|s| s.as_str()).unwrap_or("");
                            let eps = j.get("episode_count").and_then(|v| v.as_u64()).unwrap_or(1) as u32;
                            if job_name.eq_ignore_ascii_case("Director") {
                                dir_candidates.push((eps, id, name.clone(), profile_path.clone()));
                            } else if job_name.eq_ignore_ascii_case("Writer") || job_name.eq_ignore_ascii_case("Screenplay") {
                                writer_candidates.push((eps, id, name.clone(), profile_path.clone()));
                            }
                        }
                    }
                }

                dir_candidates.sort_by(|a, b| b.0.cmp(&a.0));
                writer_candidates.sort_by(|a, b| b.0.cmp(&a.0));

                for (_eps, id, name, profile_path) in dir_candidates.into_iter().take(4) {
                    let key = format!("{}:Director", name);
                    if seen_crew.insert(key) {
                        crew.push(CrewMember {
                            id,
                            name,
                            job: "Director".to_string(),
                            department: Some("Directing".to_string()),
                            profile_path,
                        });
                    }
                }

                for (_eps, id, name, profile_path) in writer_candidates.into_iter().take(4) {
                    let key = format!("{}:Writer", name);
                    if seen_crew.insert(key) {
                        crew.push(CrewMember {
                            id,
                            name,
                            job: "Writer".to_string(),
                            department: Some("Writing".to_string()),
                            profile_path,
                        });
                    }
                }
            }
        }

        // 3. Extract remaining crew from "credits.crew" (Executive Producer, Producer, Composer, DP, etc.)
        if let Some(crew_arr) = raw_json.get("credits")
            .and_then(|c| c.get("crew"))
            .and_then(|ca| ca.as_array())
        {
            let priority_jobs = [
                "Director", "Creator", "Screenplay", "Writer", "Executive Producer", "Producer", "Original Music Composer", "Director of Photography"
            ];
            // Track creator, director, and writer IDs so they don't crowd out Executive Producer
            let prominent_ids: std::collections::HashSet<u64> = crew.iter()
                .filter(|c| c.job == "Creator" || c.job == "Director" || c.job == "Writer")
                .map(|c| c.id)
                .collect();

            for job_filter in priority_jobs {
                for cr in crew_arr {
                    if let Some(job) = cr.get("job").and_then(|j| j.as_str()) {
                        if job.eq_ignore_ascii_case(job_filter) {
                            let cr_id = cr.get("id").and_then(|v| v.as_u64()).unwrap_or(0);
                            // Avoid duplicating already-featured creators or directors in Executive Producer
                            if job.eq_ignore_ascii_case("Executive Producer") && prominent_ids.contains(&cr_id) {
                                continue;
                            }

                            if let Some(name) = cr.get("name").and_then(|n| n.as_str()) {
                                let key = format!("{}:{}", name, job);
                                if seen_crew.insert(key) {
                                    crew.push(CrewMember {
                                        id: cr_id,
                                        name: name.to_string(),
                                        job: job.to_string(),
                                        department: cr.get("department").and_then(|v| v.as_str()).map(|s| s.to_string()),
                                        profile_path: cr.get("profile_path").and_then(|v| v.as_str()).map(|s| s.to_string()),
                                    });
                                }
                            }
                        }
                    }
                    if crew.len() >= 24 {
                        break;
                    }
                }
                if crew.len() >= 24 {
                    break;
                }
            }
        }

        let mut tmdb_premiere_ru: Option<String> = None;
        let mut tmdb_digital_release: Option<String> = None;

        if let Some(rd_results) = raw_json.get("release_dates").and_then(|r| r.get("results")).and_then(|arr| arr.as_array()) {
            for country_obj in rd_results {
                let iso = country_obj.get("iso_3166_1").and_then(|s| s.as_str()).unwrap_or("");
                if let Some(dates_arr) = country_obj.get("release_dates").and_then(|d| d.as_array()) {
                    for entry in dates_arr {
                        let r_type = entry.get("type").and_then(|t| t.as_u64()).unwrap_or(0);
                        let date_str = entry.get("release_date").and_then(|d| d.as_str()).map(|s| {
                            if s.len() >= 10 { s[..10].to_string() } else { s.to_string() }
                        });
                        if iso == "RU" {
                            if (r_type == 3 || r_type == 1 || r_type == 2) && tmdb_premiere_ru.is_none() {
                                tmdb_premiere_ru = date_str.clone();
                            }
                            if (r_type == 4 || r_type == 5) && tmdb_digital_release.is_none() {
                                tmdb_digital_release = date_str.clone();
                            }
                        } else if iso == "US" {
                            if (r_type == 4 || r_type == 5) && tmdb_digital_release.is_none() {
                                tmdb_digital_release = date_str.clone();
                            }
                        }
                    }
                }
            }
        }

        let mut kp_id: Option<u64> = None;
        let mut kp_rating: Option<f32> = None;
        let mut kp_tickets_available: Option<bool> = None;
        let mut kp_premiere_ru: Option<String> = None;
        let mut kp_premiere_world: Option<String> = None;
        let mut kp_digital_release: Option<String> = None;
        let mut kp_videos: Vec<VideoItem> = Vec::new();

        let kp_key = self.kp_config.api_key.trim();
        if !kp_key.is_empty() {
            let kp_search_url = format!("https://kinopoiskapiunofficial.tech/api/v2.2/films?imdbId={}", tconst);
            if let Ok(kp_res) = self.client.get(&kp_search_url)
                .header("X-API-KEY", kp_key)
                .header("Content-Type", "application/json")
                .send()
                .await
            {
                if let Ok(kp_json) = kp_res.json::<serde_json::Value>().await {
                    if let Some(first_item) = kp_json.get("items").and_then(|it| it.as_array()).and_then(|arr| arr.first()) {
                        if let Some(id) = first_item.get("kinopoiskId").and_then(|v| v.as_u64()) {
                            kp_id = Some(id);
                            kp_rating = first_item.get("ratingKinopoisk").and_then(|v| v.as_f64()).map(|v| v as f32);

                            // 1. Fetch details (tickets availability)
                            let kp_film_url = format!("https://kinopoiskapiunofficial.tech/api/v2.2/films/{}", id);
                            if let Ok(f_res) = self.client.get(&kp_film_url)
                                .header("X-API-KEY", kp_key)
                                .send()
                                .await
                            {
                                if let Ok(f_json) = f_res.json::<serde_json::Value>().await {
                                    kp_tickets_available = f_json.get("isTicketsAvailable").and_then(|b| b.as_bool());
                                    if kp_rating.is_none() {
                                        kp_rating = f_json.get("ratingKinopoisk").and_then(|v| v.as_f64()).map(|v| v as f32);
                                    }
                                }
                            }

                            // 2. Fetch distributions (premieres in RU / World / Digital)
                            let kp_dist_url = format!("https://kinopoiskapiunofficial.tech/api/v2.2/films/{}/distributions", id);
                            if let Ok(d_res) = self.client.get(&kp_dist_url)
                                .header("X-API-KEY", kp_key)
                                .send()
                                .await
                            {
                                if let Ok(d_json) = d_res.json::<serde_json::Value>().await {
                                    if let Some(items) = d_json.get("items").and_then(|it| it.as_array()) {
                                        for d in items {
                                            let d_type = d.get("type").and_then(|s| s.as_str()).unwrap_or("");
                                            let sub_type = d.get("subType").and_then(|s| s.as_str()).unwrap_or("");
                                            let d_date = d.get("date").and_then(|s| s.as_str()).map(|s| s.to_string());
                                            let country = d.get("country").and_then(|c| c.get("country")).and_then(|s| s.as_str()).unwrap_or("");

                                            if (d_type == "PREMIERE" || d_type == "COUNTRY_SPECIFIC") && country == "Россия" && kp_premiere_ru.is_none() {
                                                kp_premiere_ru = d_date.clone();
                                            }
                                            if d_type == "WORLD_PREMIER" && kp_premiere_world.is_none() {
                                                kp_premiere_world = d_date.clone();
                                            }
                                            if sub_type == "DIGITAL" && kp_digital_release.is_none() {
                                                kp_digital_release = d_date.clone();
                                            }
                                        }
                                    }
                                }
                            }

                            // 3. Fetch videos (official Russian trailers)
                            let kp_vids_url = format!("https://kinopoiskapiunofficial.tech/api/v2.2/films/{}/videos", id);
                            if let Ok(v_res) = self.client.get(&kp_vids_url)
                                .header("X-API-KEY", kp_key)
                                .send()
                                .await
                            {
                                if let Ok(v_json) = v_res.json::<serde_json::Value>().await {
                                    if let Some(items) = v_json.get("items").and_then(|it| it.as_array()) {
                                        for v in items {
                                            let v_url = v.get("url").and_then(|s| s.as_str()).unwrap_or("");
                                            let v_name = v.get("name").and_then(|s| s.as_str()).unwrap_or("Трейлер").to_string();

                                            let trailer_id = if let Some(pos) = v_url.find("/trailer/") {
                                                let rest = &v_url[pos + 9..];
                                                rest.split('?').next().unwrap_or("").to_string()
                                            } else {
                                                String::new()
                                            };

                                            if !trailer_id.is_empty() {
                                                kp_videos.push(VideoItem {
                                                    id: format!("kp_{}", trailer_id),
                                                    name: v_name,
                                                    key: trailer_id,
                                                    site: "Kinopoisk".to_string(),
                                                    video_type: "Trailer".to_string(),
                                                    official: true,
                                                    published_at: None,
                                                    stream_url: None,
                                                    thumbnail_url: None,
                                                });
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let final_premiere_world = release_date.or(kp_premiere_world);
        let final_premiere_ru = kp_premiere_ru.or(tmdb_premiere_ru);
        let final_digital_release = kp_digital_release.or(tmdb_digital_release);

        // Theatrical status calculation
        let mut in_theaters = kp_tickets_available.unwrap_or(false);
        let today_utc = chrono::Utc::now().naive_utc().date();

        let effective_theatrical_date = final_premiere_ru.as_deref().or(final_premiere_world.as_deref());
        if let Some(t_str) = effective_theatrical_date {
            if let Ok(t_date) = chrono::NaiveDate::parse_from_str(&t_str[..10.min(t_str.len())], "%Y-%m-%d") {
                let days_since = (today_utc - t_date).num_days();
                // Active theatrical window: between 3 days before premiere and 55 days after
                if days_since >= -3 && days_since <= 55 {
                    let digital_passed = final_digital_release.as_ref().map(|d| {
                        if let Ok(d_date) = chrono::NaiveDate::parse_from_str(&d[..10.min(d.len())], "%Y-%m-%d") {
                            today_utc > d_date
                        } else {
                            false
                        }
                    }).unwrap_or(false);

                    if !digital_passed {
                        in_theaters = true;
                    }
                }
            }
        }

        let mut tmdb_videos = Vec::new();
        if let Some(vids_arr) = raw_json.get("videos")
            .and_then(|v| v.get("results"))
            .and_then(|va| va.as_array())
        {
            for v in vids_arr {
                let site = v.get("site").and_then(|s| s.as_str()).unwrap_or("");
                if site.eq_ignore_ascii_case("YouTube") {
                    let key = v.get("key").and_then(|k| k.as_str()).unwrap_or("");
                    let name = v.get("name").and_then(|n| n.as_str()).unwrap_or("");
                    let video_type = v.get("type").and_then(|t| t.as_str()).unwrap_or("Trailer");
                    let official = v.get("official").and_then(|o| o.as_bool()).unwrap_or(false);
                    let iso_lang = v.get("iso_639_1").and_then(|l| l.as_str()).unwrap_or("");

                    if !key.is_empty() {
                        tmdb_videos.push((
                            iso_lang == "ru",
                            official,
                            video_type == "Trailer",
                            VideoItem {
                                id: v.get("id").and_then(|i| i.as_str()).unwrap_or("").to_string(),
                                name: name.to_string(),
                                key: key.to_string(),
                                site: site.to_string(),
                                video_type: video_type.to_string(),
                                official,
                                published_at: v.get("published_at").and_then(|p| p.as_str()).map(|s| s.to_string()),
                                stream_url: None,
                                thumbnail_url: None,
                            }
                        ));
                    }
                }
            }

            tmdb_videos.sort_by(|a, b| {
                b.0.cmp(&a.0)
                    .then_with(|| b.2.cmp(&a.2))
                    .then_with(|| b.1.cmp(&a.1))
            });
        }

        let sorted_tmdb_videos: Vec<VideoItem> = tmdb_videos.into_iter().map(|(_, _, _, item)| item).collect();

        // Combine videos: Prioritize Kinopoisk Russian dubbed trailers first, then TMDB videos
        let mut combined_videos = Vec::new();
        for kv in kp_videos {
            combined_videos.push(kv);
        }
        for tv in sorted_tmdb_videos {
            if !combined_videos.iter().any(|v| v.key == tv.key) {
                combined_videos.push(tv);
            }
        }
        let final_videos: Vec<VideoItem> = combined_videos.into_iter().take(8).collect();

        let mut recommendations = if let Some(rec_val) = raw_json.get("recommendations") {
            parse_tmdb_results_to_feed_items(rec_val, if is_tv { "tv" } else { "movie" })
        } else {
            Vec::new()
        };

        if recommendations.len() < 4 {
            if let Some(sim_val) = raw_json.get("similar") {
                let sim_items = parse_tmdb_results_to_feed_items(sim_val, if is_tv { "tv" } else { "movie" });
                let existing_ids: std::collections::HashSet<u64> = recommendations.iter().map(|item| item.id).collect();
                for item in sim_items {
                    if !existing_ids.contains(&item.id) {
                        recommendations.push(item);
                        if recommendations.len() >= 12 {
                            break;
                        }
                    }
                }
            }
        }

        let response = MovieMetadataResponse {
            tconst: tconst.to_string(),
            tmdb_id: media_id,
            title,
            original_title,
            overview,
            premiered: final_premiere_world,
            premiered_ru: final_premiere_ru,
            digital_release: final_digital_release,
            in_theaters,
            kinopoisk_id: kp_id,
            kinopoisk_rating: kp_rating,
            year,
            rating,
            genres,
            poster_path,
            backdrop_path,
            logo_path,
            cast,
            crew,
            videos: final_videos,
            runtime_minutes,
            backdrops,
            recommendations,
        };

        {
            let mut cache = self.movies_cache.lock().unwrap();
            cache.put(tconst.to_string(), response.clone());
        }

        Ok(Some(response))
    }

    /// Extract 1080p unencrypted HLS stream and preview thumbnail from Kinopoisk trailer widget
    pub async fn get_kinopoisk_trailer_stream(&self, trailer_id: &str) -> Result<Option<TrailerStreamResponse>> {
        let clean_id = trailer_id.trim();
        if clean_id.is_empty() {
            return Ok(None);
        }

        // 1. Check in-memory LRU cache (3-hour TTL)
        {
            let mut cache = self.trailer_stream_cache.lock().unwrap();
            if let Some((instant, cached)) = cache.get(clean_id) {
                if instant.elapsed() < Duration::from_secs(3 * 3600) {
                    return Ok(Some(cached.clone()));
                }
            }
        }

        // 2. Fetch widget page
        let widget_url = format!("https://widgets.kinopoisk.ru/discovery/trailer/{}?onlyPlayer=1&cover=1", clean_id);
        let resp = self.client.get(&widget_url)
            .header("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            .send()
            .await?;

        if !resp.status().is_success() {
            warn!("Failed to fetch kinopoisk trailer widget {}: status {}", clean_id, resp.status());
            return Ok(None);
        }

        let html = resp.text().await?;

        // 3. Extract streamUrl (.m3u8)
        let stream_url = if let Some(idx) = html.find("streamUrl") {
            let after = &html[idx + 9..];
            if let Some(start_https) = after.find("https:") {
                let stream_part = &after[start_https..];
                let end_idx = stream_part.find(|c| c == '"' || c == '%' || c == '\\' || c == '&' || c == '\'')
                    .unwrap_or(stream_part.len());
                let raw_url = &stream_part[..end_idx];
                Some(raw_url.replace(r"\/", "/"))
            } else {
                None
            }
        } else {
            None
        };

        let stream_url = match stream_url {
            Some(s) if s.contains(".m3u8") => s,
            _ => {
                // Secondary regex attempt for direct strm.yandex.ru m3u8
                if let Some(pos) = html.find("https://strm.yandex.ru") {
                    let part = &html[pos..];
                    let end_idx = part.find(|c| c == '"' || c == '%' || c == '\\' || c == '&' || c == '\'')
                        .unwrap_or(part.len());
                    let raw = &part[..end_idx];
                    if raw.contains(".m3u8") {
                        raw.replace(r"\/", "/")
                    } else {
                        return Ok(None);
                    }
                } else {
                    return Ok(None);
                }
            }
        };

        // 4. Extract thumbnail
        let mut thumbnail_url = None;
        let thumb_pattern = "//avatars.mds.yandex.net/get-kino-vod-films-gallery/";
        if let Some(pos) = html.find(thumb_pattern) {
            let part = &html[pos..];
            let end_idx = part.find(|c| c == '"' || c == '%' || c == '\\' || c == '\'' || c == ' ' || c == '<')
                .unwrap_or(part.len());
            let raw_thumb = &part[..end_idx];
            let mut full = format!("https:{}", raw_thumb.replace(r"\/", "/"));
            if full.contains("100x56") {
                full = full.replace("100x56", "540x304");
            } else if full.contains("224x126") {
                full = full.replace("224x126", "540x304");
            }
            thumbnail_url = Some(full);
        }

        let result = TrailerStreamResponse {
            trailer_id: clean_id.to_string(),
            stream_url,
            thumbnail_url,
        };

        // 5. Store in cache
        {
            let mut cache = self.trailer_stream_cache.lock().unwrap();
            cache.put(clean_id.to_string(), (Instant::now(), result.clone()));
        }

        Ok(Some(result))
    }

    pub async fn get_movie_recommendations(&self, tconst: &str) -> Result<FeedShelf> {
        if let Some(metadata) = self.get_movie_metadata(tconst).await? {
            let total = metadata.recommendations.len() as u64;
            Ok(FeedShelf {
                id: "recommendations".to_string(),
                title: "Похожие фильмы и рекомендации".to_string(),
                icon: "sparkles".to_string(),
                items: metadata.recommendations,
                page: Some(1),
                total_pages: Some(1),
                total_results: Some(total),
            })
        } else {
            Ok(FeedShelf {
                id: "recommendations".to_string(),
                title: "Похожие фильмы и рекомендации".to_string(),
                icon: "sparkles".to_string(),
                items: Vec::new(),
                page: Some(1),
                total_pages: Some(1),
                total_results: Some(0),
            })
        }
    }

    pub async fn get_person_details(&self, person_id: u64) -> Result<Option<PersonDetailsResponse>> {
        {
            let mut cache = self.persons_cache.lock().unwrap();
            if let Some(cached) = cache.get(&person_id) {
                return Ok(Some(cached.clone()));
            }
        }

        if self.config.api_key.trim().is_empty() {
            warn!("TMDB API key not configured, cannot fetch person details for {}", person_id);
            return Ok(None);
        }

        let url = format!(
            "https://api.themoviedb.org/3/person/{}?language=ru-RU&append_to_response=combined_credits,external_ids",
            person_id
        );

        let mut req = self.client.get(&url);
        if self.config.api_key.len() > 40 {
            req = req.header("Authorization", format!("Bearer {}", self.config.api_key));
        } else {
            req = req.query(&[("api_key", &self.config.api_key)]);
        }

        let resp = req.send().await?.error_for_status()?;
        let raw_json: serde_json::Value = resp.json().await?;

        let name = raw_json.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let mut biography = raw_json
            .get("biography")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .filter(|s| !s.trim().is_empty());

        // Fallback to English biography if Russian is absent
        if biography.is_none() {
            let en_url = format!("https://api.themoviedb.org/3/person/{}?language=en-US", person_id);
            let mut en_req = self.client.get(&en_url);
            if self.config.api_key.len() > 40 {
                en_req = en_req.header("Authorization", format!("Bearer {}", self.config.api_key));
            } else {
                en_req = en_req.query(&[("api_key", &self.config.api_key)]);
            }
            if let Ok(en_resp) = en_req.send().await {
                if let Ok(en_json) = en_resp.json::<serde_json::Value>().await {
                    biography = en_json
                        .get("biography")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                        .filter(|s| !s.trim().is_empty());
                }
            }
        }

        let birthday = raw_json.get("birthday").and_then(|v| v.as_str()).map(|s| s.to_string());
        let deathday = raw_json.get("deathday").and_then(|v| v.as_str()).map(|s| s.to_string());
        let place_of_birth = raw_json.get("place_of_birth").and_then(|v| v.as_str()).map(|s| s.to_string());
        let known_for_department = raw_json.get("known_for_department").and_then(|v| v.as_str()).map(|s| s.to_string());
        let profile_path = raw_json.get("profile_path").and_then(|v| v.as_str()).map(|s| s.to_string());
        let imdb_id = raw_json.get("external_ids").and_then(|ext| ext.get("imdb_id")).and_then(|v| v.as_str()).map(|s| s.to_string());

        let mut cast_credits = Vec::new();
        if let Some(cast_arr) = raw_json.get("combined_credits").and_then(|c| c.get("cast")).and_then(|a| a.as_array()) {
            for c in cast_arr {
                if let Some(id) = c.get("id").and_then(|v| v.as_u64()) {
                    let media_type = c.get("media_type").and_then(|v| v.as_str()).unwrap_or("movie").to_string();
                    let title = c.get("title").or_else(|| c.get("name")).and_then(|v| v.as_str()).unwrap_or("").to_string();
                    let original_title = c.get("original_title").or_else(|| c.get("original_name")).and_then(|v| v.as_str()).map(|s| s.to_string());
                    let character = c.get("character").and_then(|v| v.as_str()).map(|s| s.to_string());
                    let release_date = c.get("release_date").or_else(|| c.get("first_air_date")).and_then(|v| v.as_str()).map(|s| s.to_string());
                    let year = release_date.as_ref().and_then(|d| d.split('-').next().and_then(|y| y.parse::<u32>().ok()));
                    let vote_average = c.get("vote_average").and_then(|v| v.as_f64()).map(|v| v as f32);
                    let vote_count = c.get("vote_count").and_then(|v| v.as_u64()).unwrap_or(0);
                    let poster_path = c.get("poster_path").and_then(|v| v.as_str()).map(|s| s.to_string());
                    let backdrop_path = c.get("backdrop_path").and_then(|v| v.as_str()).map(|s| s.to_string());

                    cast_credits.push(PersonCreditItem {
                        id,
                        media_type,
                        title,
                        original_title,
                        character,
                        job: None,
                        department: None,
                        release_date,
                        year,
                        vote_average,
                        vote_count,
                        poster_path,
                        backdrop_path,
                    });
                }
            }
        }

        let mut crew_credits = Vec::new();
        if let Some(crew_arr) = raw_json.get("combined_credits").and_then(|c| c.get("crew")).and_then(|a| a.as_array()) {
            for c in crew_arr {
                if let Some(id) = c.get("id").and_then(|v| v.as_u64()) {
                    let media_type = c.get("media_type").and_then(|v| v.as_str()).unwrap_or("movie").to_string();
                    let title = c.get("title").or_else(|| c.get("name")).and_then(|v| v.as_str()).unwrap_or("").to_string();
                    let original_title = c.get("original_title").or_else(|| c.get("original_name")).and_then(|v| v.as_str()).map(|s| s.to_string());
                    let job = c.get("job").and_then(|v| v.as_str()).map(|s| s.to_string());
                    let department = c.get("department").and_then(|v| v.as_str()).map(|s| s.to_string());
                    let release_date = c.get("release_date").or_else(|| c.get("first_air_date")).and_then(|v| v.as_str()).map(|s| s.to_string());
                    let year = release_date.as_ref().and_then(|d| d.split('-').next().and_then(|y| y.parse::<u32>().ok()));
                    let vote_average = c.get("vote_average").and_then(|v| v.as_f64()).map(|v| v as f32);
                    let vote_count = c.get("vote_count").and_then(|v| v.as_u64()).unwrap_or(0);
                    let poster_path = c.get("poster_path").and_then(|v| v.as_str()).map(|s| s.to_string());
                    let backdrop_path = c.get("backdrop_path").and_then(|v| v.as_str()).map(|s| s.to_string());

                    crew_credits.push(PersonCreditItem {
                        id,
                        media_type,
                        title,
                        original_title,
                        character: None,
                        job,
                        department,
                        release_date,
                        year,
                        vote_average,
                        vote_count,
                        poster_path,
                        backdrop_path,
                    });
                }
            }
        }

        let response = PersonDetailsResponse {
            id: person_id,
            name,
            biography,
            birthday,
            deathday,
            place_of_birth,
            known_for_department,
            profile_path,
            imdb_id,
            cast: cast_credits,
            crew: crew_credits,
        };

        {
            let mut cache = self.persons_cache.lock().unwrap();
            cache.put(person_id, response.clone());
        }

        Ok(Some(response))
    }

    pub async fn resolve_tmdb_media(&self, media_type: &str, tmdb_id: u64) -> Result<Option<MovieDoc>> {
        let cache_key = format!("{}:{}", media_type, tmdb_id);
        {
            let mut cache = self.resolved_cache.lock().unwrap();
            if let Some(cached) = cache.get(&cache_key) {
                return Ok(Some(cached.clone()));
            }
        }

        let url = format!(
            "https://api.themoviedb.org/3/{}/{}?language=ru-RU&append_to_response=external_ids",
            media_type, tmdb_id
        );

        let mut req = self.client.get(&url);
        if self.config.api_key.len() > 40 {
            req = req.header("Authorization", format!("Bearer {}", self.config.api_key));
        } else {
            req = req.query(&[("api_key", &self.config.api_key)]);
        }

        let resp = req.send().await?.error_for_status()?;
        let raw_json: serde_json::Value = resp.json().await?;

        let imdb_id = raw_json
            .get("external_ids")
            .and_then(|ext| ext.get("imdb_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let tconst = if imdb_id.is_empty() {
            format!("tmdb-{}", tmdb_id)
        } else {
            imdb_id
        };

        let title = raw_json
            .get("title")
            .or_else(|| raw_json.get("name"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let original_title = raw_json
            .get("original_title")
            .or_else(|| raw_json.get("original_name"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let release_date = raw_json
            .get("release_date")
            .or_else(|| raw_json.get("first_air_date"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let year = release_date
            .as_ref()
            .and_then(|d| d.split('-').next().and_then(|y| y.parse::<u32>().ok()));

        let rating = raw_json
            .get("vote_average")
            .and_then(|v| v.as_f64())
            .map(|v| v as f32);

        let num_votes = raw_json
            .get("vote_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u32;

        let runtime_minutes = raw_json
            .get("runtime")
            .and_then(|v| v.as_u64())
            .map(|v| v as u32);

        let genres: Vec<String> = raw_json
            .get("genres")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|g| g.get("name").and_then(|n| n.as_str()).map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let doc = MovieDoc {
            tconst,
            title_ru: if title.is_empty() { None } else { Some(title.clone()) },
            title_orig: original_title.clone(),
            title_primary: if title.is_empty() { original_title.clone() } else { title.clone() },
            russian_titles: if title.is_empty() { vec![] } else { vec![title] },
            year,
            title_type: if media_type == "tv" { "tvSeries".to_string() } else { "movie".to_string() },
            rating,
            num_votes,
            genres,
            runtime_minutes,
        };

        {
            let mut cache = self.resolved_cache.lock().unwrap();
            cache.put(cache_key, doc.clone());
        }

        Ok(Some(doc))
    }

    pub async fn get_home_feeds(&self, profile: Option<&str>) -> Result<Vec<FeedShelf>> {
        let profile_key = profile.unwrap_or("default");
        {
            let cache = self.feeds_cache.lock().unwrap();
            if let Some((ts, ref shelves)) = cache.get(profile_key) {
                if ts.elapsed() < Duration::from_secs(7200) {
                    return Ok(shelves.clone());
                }
            }
        }

        if self.config.api_key.trim().is_empty() {
            warn!("TMDB API key not configured, cannot fetch home feeds");
            return Ok(Vec::new());
        }

        let urls = if profile_key == "kids" {
            vec![
                (
                    "kids_magic",
                    "Волшебные миры Disney & Pixar",
                    "sparkles",
                    "https://api.themoviedb.org/3/discover/movie?language=ru-RU&with_companies=2|3&with_genres=16|10751&sort_by=popularity.desc".to_string(),
                    "movie",
                ),
                (
                    "kids_family",
                    "Семейное кино и сказки",
                    "heart",
                    "https://api.themoviedb.org/3/discover/movie?language=ru-RU&with_genres=10751,14&without_genres=27,53&sort_by=popularity.desc&vote_count.gte=50".to_string(),
                    "movie",
                ),
                (
                    "kids_russian",
                    "Российская анимация и сказки",
                    "star",
                    "https://api.themoviedb.org/3/discover/movie?language=ru-RU&with_original_language=ru&with_genres=16|10751&sort_by=popularity.desc".to_string(),
                    "movie",
                ),
                (
                    "kids_series",
                    "Любимые мультсериалы",
                    "tv",
                    "https://api.themoviedb.org/3/discover/tv?language=ru-RU&with_genres=16,10762&without_genres=10763,10764,10767&sort_by=popularity.desc&vote_count.gte=30".to_string(),
                    "tv",
                ),
                (
                    "kids_action",
                    "Экшен, аниме и супергерои",
                    "flame",
                    "https://api.themoviedb.org/3/discover/movie?language=ru-RU&with_genres=16,28&sort_by=popularity.desc".to_string(),
                    "movie",
                ),
            ]
        } else {
            vec![
                (
                    "trending",
                    "В тренде на этой неделе",
                    "flame",
                    "https://api.themoviedb.org/3/trending/all/week?language=ru-RU".to_string(),
                    "movie",
                ),
                (
                    "apple_tv",
                    "Apple TV+ Originals",
                    "star",
                    "https://api.themoviedb.org/3/discover/tv?language=ru-RU&with_networks=2552&sort_by=popularity.desc".to_string(),
                    "tv",
                ),
                (
                    "popular_series",
                    "Популярные сериалы",
                    "tv",
                    "https://api.themoviedb.org/3/discover/tv?language=ru-RU&sort_by=popularity.desc&without_genres=10763,10764,10766,10767&vote_count.gte=50".to_string(),
                    "tv",
                ),
                (
                    "hbo_max",
                    "HBO / Max Originals",
                    "star",
                    "https://api.themoviedb.org/3/discover/tv?language=ru-RU&with_networks=49&sort_by=popularity.desc".to_string(),
                    "tv",
                ),
            ]
        };

        let fetches = urls.into_iter().map(|(id, title, icon, url, def_type)| {
            let mut req = self.client.get(&url);
            if self.config.api_key.len() > 40 {
                req = req.header("Authorization", format!("Bearer {}", self.config.api_key));
            } else {
                req = req.query(&[("api_key", &self.config.api_key)]);
            }

            async move {
                let items = match req.send().await {
                    Ok(resp) => {
                        if let Ok(val) = resp.json::<serde_json::Value>().await {
                            parse_tmdb_results_to_feed_items(&val, def_type)
                        } else {
                            Vec::new()
                        }
                    }
                    Err(e) => {
                        warn!("Failed to fetch feed {}: {}", id, e);
                        Vec::new()
                    }
                };

                FeedShelf {
                    id: id.to_string(),
                    title: title.to_string(),
                    icon: icon.to_string(),
                    items,
                    page: Some(1),
                    total_pages: None,
                    total_results: None,
                }
            }
        });

        let shelves = futures_util::future::join_all(fetches).await;

        {
            let mut cache = self.feeds_cache.lock().unwrap();
            cache.insert(profile_key.to_string(), (Instant::now(), shelves.clone()));
        }

        Ok(shelves)
    }

    /// Fetches a specific page (20 items) for a given shelf ID with in-memory caching
    pub async fn get_shelf_page(&self, shelf_id: &str, media_type: Option<&str>, page: u32) -> Result<Option<FeedShelf>> {
        let type_key = media_type.unwrap_or("default");
        let cache_key = format!("{}:{}:{}", shelf_id, type_key, page);
        {
            let mut cache = self.shelf_pages_cache.lock().unwrap();
            if let Some((ts, shelf)) = cache.get(&cache_key) {
                if ts.elapsed() < Duration::from_secs(3600) {
                    return Ok(Some(shelf.clone()));
                }
            }
        }

        if self.config.api_key.trim().is_empty() {
            warn!("TMDB API key not configured, cannot fetch shelf page");
            return Ok(None);
        }

        let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
        let is_tv = media_type == Some("tv");

        let (title, icon, url, def_type) = match shelf_id {
            "trending" => {
                if is_tv {
                    (
                        "В тренде на этой неделе",
                        "flame",
                        format!("https://api.themoviedb.org/3/trending/tv/week?language=ru-RU&page={}", page),
                        "tv",
                    )
                } else {
                    (
                        "В тренде на этой неделе",
                        "flame",
                        format!("https://api.themoviedb.org/3/trending/movie/week?language=ru-RU&page={}", page),
                        "movie",
                    )
                }
            }
            "digital" => {
                if is_tv {
                    (
                        "Свежие сезоны сериалов",
                        "film",
                        format!(
                            "https://api.themoviedb.org/3/discover/tv?language=ru-RU&sort_by=first_air_date.desc&first_air_date.lte={}&without_genres=10763,10764,10766,10767&vote_count.gte=30&page={}",
                            today, page
                        ),
                        "tv",
                    )
                } else {
                    (
                        "Свежие цифровые релизы",
                        "film",
                        format!(
                            "https://api.themoviedb.org/3/discover/movie?language=ru-RU&sort_by=primary_release_date.desc&release_date.lte={}&with_release_type=4%7C5&vote_count.gte=30&page={}",
                            today, page
                        ),
                        "movie",
                    )
                }
            }
            "popular_series" => (
                "Популярные сериалы",
                "tv",
                format!(
                    "https://api.themoviedb.org/3/discover/tv?language=ru-RU&sort_by=popularity.desc&without_genres=10763,10764,10766,10767&vote_count.gte=50&page={}",
                    page
                ),
                "tv",
            ),
            "top_rated" => {
                if is_tv {
                    (
                        "Шедевры сериалов (Топ)",
                        "star",
                        format!(
                            "https://api.themoviedb.org/3/tv/top_rated?language=ru-RU&without_genres=10763,10764,10766,10767&vote_count.gte=300&page={}",
                            page
                        ),
                        "tv",
                    )
                } else {
                    (
                        "Шедевры всех времён",
                        "star",
                        format!("https://api.themoviedb.org/3/movie/top_rated?language=ru-RU&vote_count.gte=1000&page={}", page),
                        "movie",
                    )
                }
            }
            "apple_tv" => {
                if !is_tv && media_type == Some("movie") {
                    (
                        "Apple TV+ Фильмы",
                        "film",
                        format!(
                            "https://api.themoviedb.org/3/discover/movie?language=ru-RU&with_watch_providers=350&watch_region=US&sort_by=popularity.desc&vote_count.gte=20&page={}",
                            page
                        ),
                        "movie",
                    )
                } else {
                    (
                        "Apple TV+ Originals",
                        "tv",
                        format!("https://api.themoviedb.org/3/discover/tv?language=ru-RU&with_networks=2552&sort_by=popularity.desc&page={}", page),
                        "tv",
                    )
                }
            }
            "hbo_max" => {
                if !is_tv && media_type == Some("movie") {
                    (
                        "HBO / Max Фильмы",
                        "film",
                        format!(
                            "https://api.themoviedb.org/3/discover/movie?language=ru-RU&with_watch_providers=1899%7C384&watch_region=US&sort_by=popularity.desc&vote_count.gte=30&page={}",
                            page
                        ),
                        "movie",
                    )
                } else {
                    (
                        "HBO / Max Originals",
                        "tv",
                        format!("https://api.themoviedb.org/3/discover/tv?language=ru-RU&with_networks=49&sort_by=popularity.desc&page={}", page),
                        "tv",
                    )
                }
            }
            "netflix" => {
                if !is_tv && media_type == Some("movie") {
                    (
                        "Netflix Фильмы",
                        "film",
                        format!(
                            "https://api.themoviedb.org/3/discover/movie?language=ru-RU&with_watch_providers=8&watch_region=US&sort_by=popularity.desc&vote_count.gte=30&page={}",
                            page
                        ),
                        "movie",
                    )
                } else {
                    (
                        "Netflix Хиты",
                        "tv",
                        format!(
                            "https://api.themoviedb.org/3/discover/tv?language=ru-RU&with_networks=213&sort_by=popularity.desc&without_genres=10763,10764,10767&vote_count.gte=50&page={}",
                            page
                        ),
                        "tv",
                    )
                }
            }
            "amazon_prime" => {
                if !is_tv && media_type == Some("movie") {
                    (
                        "Amazon Prime Фильмы",
                        "film",
                        format!(
                            "https://api.themoviedb.org/3/discover/movie?language=ru-RU&with_watch_providers=119%7C9&watch_region=US&sort_by=popularity.desc&vote_count.gte=30&page={}",
                            page
                        ),
                        "movie",
                    )
                } else {
                    (
                        "Amazon Prime Video",
                        "tv",
                        format!("https://api.themoviedb.org/3/discover/tv?language=ru-RU&with_networks=1024&sort_by=popularity.desc&vote_count.gte=50&page={}", page),
                        "tv",
                    )
                }
            }
            "anime_hub" => {
                if !is_tv && media_type == Some("movie") {
                    (
                        "Аниме & Мультфильмы",
                        "tv",
                        format!("https://api.themoviedb.org/3/discover/movie?language=ru-RU&with_genres=16&with_original_language=ja&sort_by=popularity.desc&vote_count.gte=20&page={}", page),
                        "movie",
                    )
                } else {
                    (
                        "Аниме & Мультипликация",
                        "tv",
                        format!("https://api.themoviedb.org/3/discover/tv?language=ru-RU&with_genres=16&with_original_language=ja&sort_by=popularity.desc&vote_count.gte=20&page={}", page),
                        "tv",
                    )
                }
            }
            "doc_hub" => {
                if is_tv {
                    (
                        "Документальные сериалы",
                        "film",
                        format!("https://api.themoviedb.org/3/discover/tv?language=ru-RU&with_genres=99&sort_by=popularity.desc&vote_count.gte=10&page={}", page),
                        "tv",
                    )
                } else {
                    (
                        "Документальное кино",
                        "film",
                        format!("https://api.themoviedb.org/3/discover/movie?language=ru-RU&with_genres=99&sort_by=popularity.desc&vote_count.gte=20&page={}", page),
                        "movie",
                    )
                }
            }
            "kids_cartoons" => (
                "Популярные мультфильмы",
                "sparkles",
                format!("https://api.themoviedb.org/3/discover/movie?language=ru-RU&with_genres=16&sort_by=popularity.desc&vote_count.gte=50&page={}", page),
                "movie",
            ),
            "kids_russian" => (
                "Российская анимация и сказки",
                "star",
                format!("https://api.themoviedb.org/3/discover/movie?language=ru-RU&with_original_language=ru&with_genres=16%7C10751&sort_by=popularity.desc&page={}", page),
                "movie",
            ),
            "kids_series" => (
                "Любимые мультсериалы",
                "tv",
                format!("https://api.themoviedb.org/3/discover/tv?language=ru-RU&with_genres=16,10762&without_genres=10763,10764,10767&sort_by=popularity.desc&vote_count.gte=30&page={}", page),
                "tv",
            ),
            "kids_magic" => (
                "Волшебные миры Disney & Pixar",
                "sparkles",
                format!("https://api.themoviedb.org/3/discover/movie?language=ru-RU&with_companies=2%7C3&with_genres=16%7C10751&sort_by=popularity.desc&page={}", page),
                "movie",
            ),
            "kids_action" => (
                "Экшен, аниме и супергерои",
                "flame",
                format!("https://api.themoviedb.org/3/discover/movie?language=ru-RU&with_genres=16,28&sort_by=popularity.desc&page={}", page),
                "movie",
            ),
            "kids_family" => (
                "Семейное кино и сказки",
                "heart",
                format!("https://api.themoviedb.org/3/discover/movie?language=ru-RU&with_genres=10751,14&without_genres=27,53&sort_by=popularity.desc&vote_count.gte=50&page={}", page),
                "movie",
            ),
            _ => return Ok(None),
        };


        let _permit = self.tmdb_semaphore.acquire().await.ok();

        let mut req = self.client.get(&url);
        if self.config.api_key.len() > 40 {
            req = req.header("Authorization", format!("Bearer {}", self.config.api_key));
        } else {
            req = req.query(&[("api_key", &self.config.api_key)]);
        }

        let resp = req.send().await?.error_for_status()?;
        let val: serde_json::Value = resp.json().await?;

        let page_num = val.get("page").and_then(|p| p.as_u64()).map(|p| p as u32).unwrap_or(page);
        let total_pages = val.get("total_pages").and_then(|p| p.as_u64()).map(|p| p as u32);
        let total_results = val.get("total_results").and_then(|p| p.as_u64());
        let items = parse_tmdb_results_to_feed_items(&val, def_type);

        let shelf = FeedShelf {
            id: shelf_id.to_string(),
            title: title.to_string(),
            icon: icon.to_string(),
            items,
            page: Some(page_num),
            total_pages,
            total_results,
        };

        {
            let mut cache = self.shelf_pages_cache.lock().unwrap();
            cache.put(cache_key, (Instant::now(), shelf.clone()));
        }

        Ok(Some(shelf))
    }

    /// Flexible discovery with multi-criteria filters: network, genres, countries, years, rating
    pub async fn discover_catalog(&self, params: &DiscoverParams) -> Result<FeedShelf> {
        let is_tv = params.r#type.as_deref() == Some("tv");
        let def_type = if is_tv { "tv" } else { "movie" };
        let page = params.page.unwrap_or(1).max(1);

        let mut base_url = if is_tv {
            format!("https://api.themoviedb.org/3/discover/tv?language=ru-RU&page={}", page)
        } else {
            format!("https://api.themoviedb.org/3/discover/movie?language=ru-RU&page={}", page)
        };

        // Networks / Providers
        if let Some(ref net) = params.network {
            match net.to_lowercase().as_str() {
                "apple" | "apple_tv" => {
                    if is_tv {
                        base_url.push_str("&with_networks=2552");
                    } else {
                        base_url.push_str("&with_watch_providers=350&watch_region=US");
                    }
                }
                "hbo" | "hbo_max" | "max" => {
                    if is_tv {
                        base_url.push_str("&with_networks=49");
                    } else {
                        base_url.push_str("&with_watch_providers=1899|384&watch_region=US");
                    }
                }
                "netflix" => {
                    if is_tv {
                        base_url.push_str("&with_networks=213");
                    } else {
                        base_url.push_str("&with_watch_providers=8&watch_region=US");
                    }
                }
                "amazon" | "prime" => {
                    if is_tv {
                        base_url.push_str("&with_networks=1024");
                    } else {
                        base_url.push_str("&with_watch_providers=119|9&watch_region=US");
                    }
                }
                "disney" => {
                    if is_tv {
                        base_url.push_str("&with_networks=2739");
                    } else {
                        base_url.push_str("&with_watch_providers=337&watch_region=US");
                    }
                }
                _ => {}
            }
        }

        // Clean garbage genres for TV
        if is_tv {
            base_url.push_str("&without_genres=10763,10764,10766,10767");
        }

        // Genres
        if let Some(ref genres) = params.genres {
            if !genres.trim().is_empty() {
                base_url.push_str(&format!("&with_genres={}", genres.trim()));
            }
        }

        // Countries
        if let Some(ref countries) = params.countries {
            if !countries.trim().is_empty() {
                base_url.push_str(&format!("&with_origin_country={}", countries.trim()));
            }
        }

        // Years
        if is_tv {
            if let Some(yf) = params.year_from {
                base_url.push_str(&format!("&first_air_date.gte={}-01-01", yf));
            }
            if let Some(yt) = params.year_to {
                base_url.push_str(&format!("&first_air_date.lte={}-12-31", yt));
            }
        } else {
            if let Some(yf) = params.year_from {
                base_url.push_str(&format!("&primary_release_date.gte={}-01-01", yf));
            }
            if let Some(yt) = params.year_to {
                base_url.push_str(&format!("&primary_release_date.lte={}-12-31", yt));
            }
        }

        // Rating
        if let Some(r) = params.min_rating {
            base_url.push_str(&format!("&vote_average.gte={}", r));
            base_url.push_str("&vote_count.gte=50");
        } else {
            base_url.push_str("&vote_count.gte=30");
        }

        // Sort by
        let sort = params.sort_by.as_deref().unwrap_or("popularity.desc");
        base_url.push_str(&format!("&sort_by={}", sort));

        let _permit = self.tmdb_semaphore.acquire().await.ok();

        let mut req = self.client.get(&base_url);
        if self.config.api_key.len() > 40 {
            req = req.header("Authorization", format!("Bearer {}", self.config.api_key));
        } else {
            req = req.query(&[("api_key", &self.config.api_key)]);
        }

        let resp = req.send().await?.error_for_status()?;
        let val: serde_json::Value = resp.json().await?;

        let page_num = val.get("page").and_then(|p| p.as_u64()).map(|p| p as u32).unwrap_or(page);
        let total_pages = val.get("total_pages").and_then(|p| p.as_u64()).map(|p| p as u32);
        let total_results = val.get("total_results").and_then(|p| p.as_u64());
        let items = parse_tmdb_results_to_feed_items(&val, def_type);

        Ok(FeedShelf {
            id: "catalog_discover".to_string(),
            title: "Каталог фильмов и сериалов".to_string(),
            icon: if is_tv { "tv".to_string() } else { "film".to_string() },
            items,
            page: Some(page_num),
            total_pages,
            total_results,
        })
    }
}

fn parse_tmdb_results_to_feed_items(val: &serde_json::Value, default_media_type: &str) -> Vec<FeedItem> {
    let mut items = Vec::new();
    if let Some(arr) = val.get("results").and_then(|r| r.as_array()) {
        for v in arr {
            if let Some(id) = v.get("id").and_then(|i| i.as_u64()) {
                let media_type = v.get("media_type")
                    .and_then(|m| m.as_str())
                    .unwrap_or(default_media_type)
                    .to_string();

                let title = v.get("title")
                    .or_else(|| v.get("name"))
                    .and_then(|t| t.as_str())
                    .unwrap_or("")
                    .to_string();

                if title.is_empty() {
                    continue;
                }

                let original_title = v.get("original_title")
                    .or_else(|| v.get("original_name"))
                    .and_then(|t| t.as_str())
                    .map(|s| s.to_string());

                let date_str = v.get("release_date")
                    .or_else(|| v.get("first_air_date"))
                    .and_then(|d| d.as_str());

                let year = date_str.and_then(|d| d.split('-').next().and_then(|y| y.parse::<u32>().ok()));

                let rating = v.get("vote_average")
                    .and_then(|r| r.as_f64())
                    .map(|r| r as f32);

                let vote_count = v.get("vote_count")
                    .and_then(|vc| vc.as_u64())
                    .unwrap_or(0);

                let poster_path = v.get("poster_path")
                    .and_then(|p| p.as_str())
                    .map(|s| s.to_string());

                let backdrop_path = v.get("backdrop_path")
                    .and_then(|b| b.as_str())
                    .map(|s| s.to_string());

                let overview = v.get("overview")
                    .and_then(|o| o.as_str())
                    .map(|s| s.to_string())
                    .filter(|s| !s.trim().is_empty());

                items.push(FeedItem {
                    id,
                    media_type,
                    title,
                    original_title,
                    year,
                    rating,
                    vote_count,
                    poster_path,
                    backdrop_path,
                    overview,
                });
            }
        }
    }
    items
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FeedItem {
    pub id: u64,
    pub media_type: String,
    pub title: String,
    pub original_title: Option<String>,
    pub year: Option<u32>,
    pub rating: Option<f32>,
    pub vote_count: u64,
    pub poster_path: Option<String>,
    pub backdrop_path: Option<String>,
    pub overview: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FeedShelf {
    pub id: String,
    pub title: String,
    pub icon: String,
    pub items: Vec<FeedItem>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_pages: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_results: Option<u64>,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct DiscoverParams {
    pub r#type: Option<String>,
    pub network: Option<String>,
    pub genres: Option<String>,
    pub countries: Option<String>,
    pub year_from: Option<u32>,
    pub year_to: Option<u32>,
    pub min_rating: Option<f32>,
    pub sort_by: Option<String>,
    pub page: Option<u32>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PersonDetailsResponse {
    pub id: u64,
    pub name: String,
    pub biography: Option<String>,
    pub birthday: Option<String>,
    pub deathday: Option<String>,
    pub place_of_birth: Option<String>,
    pub known_for_department: Option<String>,
    pub profile_path: Option<String>,
    pub imdb_id: Option<String>,
    #[serde(default)]
    pub cast: Vec<PersonCreditItem>,
    #[serde(default)]
    pub crew: Vec<PersonCreditItem>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PersonCreditItem {
    pub id: u64,
    pub media_type: String,
    pub title: String,
    pub original_title: Option<String>,
    pub character: Option<String>,
    pub job: Option<String>,
    pub department: Option<String>,
    pub release_date: Option<String>,
    pub year: Option<u32>,
    pub vote_average: Option<f32>,
    pub vote_count: u64,
    pub poster_path: Option<String>,
    pub backdrop_path: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MovieMetadataResponse {
    pub tconst: String,
    pub tmdb_id: u64,
    pub title: String,
    pub original_title: String,
    pub overview: Option<String>,
    pub premiered: Option<String>,
    pub premiered_ru: Option<String>,
    pub digital_release: Option<String>,
    #[serde(default)]
    pub in_theaters: bool,
    pub kinopoisk_id: Option<u64>,
    pub kinopoisk_rating: Option<f32>,
    pub year: Option<u32>,
    pub rating: Option<f32>,
    pub genres: Vec<String>,
    pub poster_path: Option<String>,
    pub backdrop_path: Option<String>,
    pub logo_path: Option<String>,
    #[serde(default)]
    pub cast: Vec<CastMember>,
    #[serde(default)]
    pub crew: Vec<CrewMember>,
    #[serde(default)]
    pub videos: Vec<VideoItem>,
    #[serde(default)]
    pub runtime_minutes: Option<u32>,
    #[serde(default)]
    pub backdrops: Vec<String>,
    #[serde(default)]
    pub recommendations: Vec<FeedItem>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CastMember {
    pub id: u64,
    pub name: String,
    pub character: Option<String>,
    pub profile_path: Option<String>,
    pub order: Option<u32>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CrewMember {
    pub id: u64,
    pub name: String,
    pub job: String,
    pub department: Option<String>,
    pub profile_path: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VideoItem {
    pub id: String,
    pub name: String,
    pub key: String,
    pub site: String,
    #[serde(rename = "type")]
    pub video_type: String,
    pub official: bool,
    pub published_at: Option<String>,
    #[serde(default)]
    pub stream_url: Option<String>,
    #[serde(default)]
    pub thumbnail_url: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TrailerStreamResponse {
    pub trailer_id: String,
    pub stream_url: String,
    pub thumbnail_url: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EpisodeMetadataItem {
    pub season_number: u32,
    pub episode_number: u32,
    pub name: String,
    pub overview: Option<String>,
    pub air_date: Option<String>,
    pub still_path: Option<String>,
    pub vote_average: Option<f32>,
    pub vote_count: Option<u32>,
    pub runtime: Option<u32>,
    pub episode_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TmdbSeasonDetailRaw {
    #[serde(default)]
    pub episodes: Vec<TmdbEpisodeRaw>,
}

#[derive(Debug, Deserialize)]
struct TmdbEpisodeRaw {
    pub season_number: u32,
    pub episode_number: u32,
    #[serde(default)]
    pub name: String,
    pub overview: Option<String>,
    pub air_date: Option<String>,
    pub still_path: Option<String>,
    pub vote_average: Option<f32>,
    pub vote_count: Option<u32>,
    pub runtime: Option<u32>,
    pub episode_type: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SeriesSeasonItem {
    pub season_number: u32,
    pub name: String,
    pub episode_count: u32,
    pub air_date: Option<String>,
    pub poster_path: Option<String>,
    pub overview: Option<String>,
    pub vote_average: Option<f32>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SeriesSeasonsResponse {
    pub tconst: String,
    pub tmdb_id: u64,
    pub name: String,
    pub original_name: String,
    pub overview: Option<String>,
    pub premiered: Option<String>,
    pub rating: Option<f32>,
    pub genres: Vec<String>,
    pub studio: Option<String>,
    pub status: Option<String>,
    pub poster_path: Option<String>,
    pub backdrop_path: Option<String>,
    pub logo_path: Option<String>,
    pub number_of_seasons: u32,
    pub number_of_episodes: u32,
    pub seasons: Vec<SeriesSeasonItem>,
}

#[derive(Debug, Deserialize)]
struct TmdbTvDetails {
    pub id: u64,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub original_name: String,
    pub overview: Option<String>,
    pub first_air_date: Option<String>,
    pub vote_average: Option<f32>,
    pub backdrop_path: Option<String>,
    pub poster_path: Option<String>,
    #[serde(default)]
    pub number_of_seasons: u32,
    #[serde(default)]
    pub number_of_episodes: u32,
    #[serde(default)]
    pub genres: Vec<TmdbNamedItem>,
    #[serde(default)]
    pub networks: Vec<TmdbNamedItem>,
    pub status: Option<String>,
    #[serde(default)]
    pub seasons: Vec<TmdbSeasonRaw>,
    pub images: Option<TmdbImagesBlock>,
}

#[derive(Debug, Deserialize)]
struct TmdbNamedItem {
    #[serde(default)]
    pub name: String,
}

#[derive(Debug, Deserialize)]
struct TmdbImagesBlock {
    #[serde(default)]
    pub logos: Vec<TmdbLogoItem>,
    #[serde(default)]
    pub backdrops: Vec<TmdbBackdropItem>,
}

#[derive(Debug, Deserialize)]
struct TmdbLogoItem {
    pub file_path: String,
    pub iso_639_1: Option<String>,
    #[serde(default)]
    pub vote_average: f32,
}

#[derive(Debug, Deserialize)]
struct TmdbBackdropItem {
    pub file_path: String,
    pub iso_639_1: Option<String>,
    #[serde(default)]
    pub vote_average: f32,
}

#[derive(Debug, Deserialize)]
struct TmdbSeasonRaw {
    pub season_number: u32,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub episode_count: u32,
    pub air_date: Option<String>,
    pub poster_path: Option<String>,
    pub overview: Option<String>,
    pub vote_average: Option<f32>,
}

#[derive(Debug, Deserialize)]
struct TmdbFindResponse {
    #[serde(default)]
    movie_results: Vec<TmdbMediaItem>,
    #[serde(default)]
    tv_results: Vec<TmdbMediaItem>,
}

#[derive(Debug, Deserialize)]
struct TmdbMediaItem {
    id: u64,
    poster_path: Option<String>,
    #[serde(default)]
    backdrop_path: Option<String>,
}
