use crate::config::TmdbConfig;
use crate::index::schema::MovieDoc;
use crate::ingestion::temp_store::parse_tconst_id;
use anyhow::Result;
use futures_util::StreamExt;
use lru::LruCache;
use serde::Deserialize;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as SyncMutex};
use std::time::{Duration, Instant, SystemTime};
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;
use tokio::sync::{broadcast, Mutex as AsyncMutex};
use tracing::{info, warn};

pub const POSTER_SIZE_XS: &str = "w92";   // ~5-10 KB
pub const POSTER_SIZE_SM: &str = "w154";  // ~12-20 KB
pub const POSTER_SIZE_MD: &str = "w185";  // ~20-30 KB (Recommended for search UI)
pub const POSTER_SIZE_LG: &str = "w342";  // ~40-60 KB
pub const POSTER_SIZE_XL: &str = "w500";  // ~80-120 KB
pub const POSTER_SIZE_ORIG: &str = "original";

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

pub struct PosterFileMetadata {
    pub path: PathBuf,
    pub size: u64,
    pub mtime_system: SystemTime,
    pub etag: String,
}

#[derive(Clone)]
pub struct PosterService {
    config: TmdbConfig,
    client: reqwest::Client,
    in_flight: Arc<AsyncMutex<HashMap<String, broadcast::Sender<bool>>>>,
    negative_cache: Arc<SyncMutex<LruCache<u32, Instant>>>,
    seasons_cache: Arc<SyncMutex<LruCache<String, SeriesSeasonsResponse>>>,
    episodes_cache: Arc<SyncMutex<LruCache<String, Vec<EpisodeMetadataItem>>>>,
    movies_cache: Arc<SyncMutex<LruCache<String, MovieMetadataResponse>>>,
    persons_cache: Arc<SyncMutex<LruCache<u64, PersonDetailsResponse>>>,
    resolved_cache: Arc<SyncMutex<LruCache<String, MovieDoc>>>,
    feeds_cache: Arc<SyncMutex<Option<(Instant, Vec<FeedShelf>)>>>,
    shelf_pages_cache: Arc<SyncMutex<LruCache<String, (Instant, FeedShelf)>>>,
    poster_paths_cache: Arc<SyncMutex<LruCache<String, Option<String>>>>,
    tmdb_semaphore: Arc<tokio::sync::Semaphore>,
}

impl PosterService {
    pub fn new(config: TmdbConfig) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(6))
            .build()
            .unwrap_or_default();

        let lru_capacity = NonZeroUsize::new(10_000).unwrap();
        let negative_cache = Arc::new(SyncMutex::new(LruCache::new(lru_capacity)));
        let seasons_cache = Arc::new(SyncMutex::new(LruCache::new(lru_capacity)));
        let episodes_cache = Arc::new(SyncMutex::new(LruCache::new(lru_capacity)));
        let movies_cache = Arc::new(SyncMutex::new(LruCache::new(lru_capacity)));
        let persons_cache = Arc::new(SyncMutex::new(LruCache::new(lru_capacity)));
        let resolved_cache = Arc::new(SyncMutex::new(LruCache::new(lru_capacity)));
        let feeds_cache = Arc::new(SyncMutex::new(None));
        let shelf_pages_cache = Arc::new(SyncMutex::new(LruCache::new(lru_capacity)));
        let poster_paths_cache = Arc::new(SyncMutex::new(LruCache::new(lru_capacity)));
        let tmdb_semaphore = Arc::new(tokio::sync::Semaphore::new(6));
        let in_flight = Arc::new(AsyncMutex::new(HashMap::new()));

        Self {
            config,
            client,
            in_flight,
            negative_cache,
            seasons_cache,
            episodes_cache,
            movies_cache,
            persons_cache,
            resolved_cache,
            feeds_cache,
            shelf_pages_cache,
            poster_paths_cache,
            tmdb_semaphore,
        }
    }

    pub fn config(&self) -> &TmdbConfig {
        &self.config
    }

    /// Sharded file path: data/posters/{shard}/{tconst}_{size}.jpg
    pub fn get_poster_path(&self, tconst: &str, size: &str) -> PathBuf {
        let num_id = parse_tconst_id(tconst).unwrap_or(0);
        let shard = format!("{:02}", num_id % 100);
        self.config
            .cache_dir
            .join(shard)
            .join(format!("{}_{}.jpg", tconst, size))
    }

    /// Check if cached poster exists on disk and returns its metadata
    pub async fn get_cached_metadata(&self, path: &Path) -> Option<PosterFileMetadata> {
        if let Ok(meta) = fs::metadata(path).await {
            if meta.is_file() && meta.len() > 0 {
                let size = meta.len();
                let mtime_system = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
                let mtime_duration = mtime_system
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default();
                let etag = format!("W/\"{:x}-{:x}\"", mtime_duration.as_nanos(), size);

                return Some(PosterFileMetadata {
                    path: path.to_path_buf(),
                    size,
                    mtime_system,
                    etag,
                });
            }
        }
        None
    }

    /// Retrieve or download poster for IMDb ID
    pub async fn get_or_fetch_poster(
        &self,
        tconst: &str,
        size_param: Option<&str>,
    ) -> Option<PosterFileMetadata> {
        let size = normalize_poster_size(size_param, &self.config.default_size);
        let poster_path = self.get_poster_path(tconst, size);

        // 1. Check local disk cache first
        if let Some(meta) = self.get_cached_metadata(&poster_path).await {
            return Some(meta);
        }

        // 2. Check negative cache
        if let Some(id) = parse_tconst_id(tconst) {
            let mut neg = self.negative_cache.lock().unwrap();
            if let Some(time) = neg.get(&id) {
                if time.elapsed() < Duration::from_secs(self.config.negative_cache_hours * 3600) {
                    return None;
                }
            }
        }

        // 3. If TMDB API key is not configured, cannot fetch from TMDB
        if self.config.api_key.trim().is_empty() {
            warn!("TMDB API key is not configured. Cannot download poster for {}", tconst);
            return None;
        }

        // 4. Request Coalescing (Single-Flight)
        let flight_key = format!("{}_{}", tconst, size);
        let mut rx = {
            let mut in_flight_lock = self.in_flight.lock().await;
            if let Some(sender) = in_flight_lock.get(&flight_key) {
                // Another coroutine is already fetching this exact poster! Subscribe to result.
                sender.subscribe()
            } else {
                let (tx, rx) = broadcast::channel(1);
                in_flight_lock.insert(flight_key.clone(), tx);
                drop(in_flight_lock);

                // We are the fetcher task!
                let service_clone = self.clone();
                let tconst_str = tconst.to_string();
                let flight_key_clone = flight_key.clone();

                tokio::spawn(async move {
                    let success = service_clone.fetch_and_cache(&tconst_str, size).await;

                    let mut in_flight_lock = service_clone.in_flight.lock().await;
                    if let Some(sender) = in_flight_lock.remove(&flight_key_clone) {
                        let _ = sender.send(success);
                    }
                });

                rx
            }
        };

        // Wait for fetch completion signal
        let _ = rx.recv().await;

        // Check if file was successfully written
        self.get_cached_metadata(&poster_path).await
    }

    /// Fetches poster URL from TMDB with Russian localization priority and streams image to disk
    async fn fetch_and_cache(&self, tconst: &str, size: &str) -> bool {
        // Limit concurrent calls to TMDB to prevent rate-limiting and connection stalls
        let _permit = match self.tmdb_semaphore.acquire().await {
            Ok(p) => p,
            Err(_) => return false,
        };

        let poster_path_result = self.find_tmdb_poster_path(tconst).await;

        match poster_path_result {
            Ok(Some(tmdb_poster_path)) => {
                let image_url = format!("https://image.tmdb.org/t/p/{}{}", size, tmdb_poster_path);
                info!("Downloading poster for {} ({}) from {}", tconst, size, image_url);

                match self.download_image_to_disk(&image_url, tconst, size).await {
                    Ok(()) => true,
                    Err(e) => {
                        warn!("Failed to download image for {}: {}", tconst, e);
                        false
                    }
                }
            }
            Ok(None) => {
                info!("No poster found on TMDB for {}", tconst);
                if let Some(id) = parse_tconst_id(tconst) {
                    self.negative_cache.lock().unwrap().put(id, Instant::now());
                }
                false
            }
            Err(e) => {
                warn!("TMDB find query error for {}: {}", tconst, e);
                false
            }
        }
    }

    /// Queries TMDB to find the best poster path with Russian localization priority
    async fn find_tmdb_poster_path(&self, tconst: &str) -> Result<Option<String>> {
        // Fast-path: Check in-memory poster_paths_cache first
        {
            let mut cache = self.poster_paths_cache.lock().unwrap();
            if let Some(cached) = cache.get(tconst) {
                return Ok(cached.clone());
            }
        }

        // Step 1: Query Find API by IMDb ID with language=ru-RU
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

        // Extract media info
        let (tmdb_id, media_type, default_poster) = if let Some(m) = find_data.movie_results.into_iter().next() {
            (m.id, "movie", m.poster_path)
        } else if let Some(t) = find_data.tv_results.into_iter().next() {
            (t.id, "tv", t.poster_path)
        } else {
            let mut cache = self.poster_paths_cache.lock().unwrap();
            cache.put(tconst.to_string(), None);
            return Ok(None);
        };

        // Step 2: Query /images with Russian priority
        let images_url = format!(
            "https://api.themoviedb.org/3/{}/{}/images?include_image_language=ru,en,null",
            media_type, tmdb_id
        );

        let mut req_img = self.client.get(&images_url);
        if self.config.api_key.len() > 40 {
            req_img = req_img.header("Authorization", format!("Bearer {}", self.config.api_key));
        } else {
            req_img = req_img.query(&[("api_key", &self.config.api_key)]);
        }

        let resolved_path = if let Ok(resp_img) = req_img.send().await {
            if let Ok(img_data) = resp_img.json::<TmdbImagesResponse>().await {
                // 1st Priority: Russian localized poster
                let ru_poster = img_data
                    .posters
                    .iter()
                    .filter(|p| p.iso_639_1.as_deref() == Some("ru"))
                    .max_by(|a, b| {
                        a.vote_average
                            .partial_cmp(&b.vote_average)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    });

                if let Some(poster) = ru_poster {
                    info!("Found Russian localized poster for {}: {}", tconst, poster.file_path);
                    Some(poster.file_path.clone())
                } else {
                    // 2nd Priority: English or untyped poster
                    let fallback_poster = img_data
                        .posters
                        .iter()
                        .filter(|p| p.iso_639_1.as_deref() == Some("en") || p.iso_639_1.is_none())
                        .max_by(|a, b| {
                            a.vote_average
                                .partial_cmp(&b.vote_average)
                                .unwrap_or(std::cmp::Ordering::Equal)
                        });

                    fallback_poster.map(|p| p.file_path.clone()).or(default_poster)
                }
            } else {
                default_poster
            }
        } else {
            default_poster
        };

        // Cache in memory for all future size requests
        {
            let mut cache = self.poster_paths_cache.lock().unwrap();
            cache.put(tconst.to_string(), resolved_path.clone());
        }

        Ok(resolved_path)
    }

    /// Streams image chunks directly from HTTP response to disk (Zero-RAM!)
    async fn download_image_to_disk(&self, url: &str, tconst: &str, size: &str) -> Result<()> {
        let final_path = self.get_poster_path(tconst, size);
        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        static FILE_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let counter = FILE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let temp_path = final_path.with_extension(format!("tmp.{}.{}", std::process::id(), counter));

        let resp = self.client.get(url).send().await?.error_for_status()?;
        let mut file = File::create(&temp_path).await?;
        let mut stream = resp.bytes_stream();

        while let Some(chunk_result) = stream.next().await {
            let chunk = chunk_result?;
            file.write_all(&chunk).await?;
        }

        file.flush().await?;
        drop(file);

        fs::rename(&temp_path, &final_path).await?;
        Ok(())
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
                "https://api.themoviedb.org/3/tv/{}?language=ru-RU&append_to_response=aggregate_credits,credits,videos,images&include_image_language=ru,en,null&include_video_language=ru,en,null",
                media_id
            )
        } else {
            format!(
                "https://api.themoviedb.org/3/movie/{}?language=ru-RU&append_to_response=credits,videos,images&include_image_language=ru,en,null&include_video_language=ru,en,null",
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

        let mut videos = Vec::new();
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
                        videos.push((
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
                            }
                        ));
                    }
                }
            }

            videos.sort_by(|a, b| {
                b.0.cmp(&a.0)
                    .then_with(|| b.2.cmp(&a.2))
                    .then_with(|| b.1.cmp(&a.1))
            });
        }

        let sorted_videos: Vec<VideoItem> = videos.into_iter().map(|(_, _, _, item)| item).take(6).collect();

        let response = MovieMetadataResponse {
            tconst: tconst.to_string(),
            tmdb_id: media_id,
            title,
            original_title,
            overview,
            premiered: release_date,
            year,
            rating,
            genres,
            poster_path,
            backdrop_path,
            logo_path,
            cast,
            crew,
            videos: sorted_videos,
        };

        {
            let mut cache = self.movies_cache.lock().unwrap();
            cache.put(tconst.to_string(), response.clone());
        }

        Ok(Some(response))
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

    pub async fn get_home_feeds(&self) -> Result<Vec<FeedShelf>> {
        {
            let cache = self.feeds_cache.lock().unwrap();
            if let Some((ts, ref shelves)) = *cache {
                if ts.elapsed() < Duration::from_secs(7200) {
                    return Ok(shelves.clone());
                }
            }
        }

        if self.config.api_key.trim().is_empty() {
            warn!("TMDB API key not configured, cannot fetch home feeds");
            return Ok(Vec::new());
        }

        let today = chrono::Utc::now().format("%Y-%m-%d").to_string();

        let urls = vec![
            (
                "trending",
                "В тренде на этой неделе",
                "flame",
                "https://api.themoviedb.org/3/trending/all/week?language=ru-RU".to_string(),
                "movie",
            ),
            (
                "digital",
                "Свежие цифровые релизы",
                "film",
                format!(
                    "https://api.themoviedb.org/3/discover/movie?language=ru-RU&sort_by=primary_release_date.desc&release_date.lte={}&with_release_type=4%7C5&vote_count.gte=30",
                    today
                ),
                "movie",
            ),
            (
                "popular_series",
                "Популярные сериалы",
                "tv",
                "https://api.themoviedb.org/3/tv/popular?language=ru-RU".to_string(),
                "tv",
            ),
            (
                "top_rated",
                "Шедевры всех времён",
                "star",
                "https://api.themoviedb.org/3/movie/top_rated?language=ru-RU&vote_count.gte=1000".to_string(),
                "movie",
            ),
        ];

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
            *cache = Some((Instant::now(), shelves.clone()));
        }

        Ok(shelves)
    }

    /// Fetches a specific page (20 items) for a given shelf ID with in-memory caching
    pub async fn get_shelf_page(&self, shelf_id: &str, page: u32) -> Result<Option<FeedShelf>> {
        let cache_key = format!("{}:{}", shelf_id, page);
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

        let (title, icon, url, def_type) = match shelf_id {
            "trending" => (
                "В тренде на этой неделе",
                "flame",
                format!("https://api.themoviedb.org/3/trending/all/week?language=ru-RU&page={}", page),
                "movie",
            ),
            "digital" => (
                "Свежие цифровые релизы",
                "film",
                format!(
                    "https://api.themoviedb.org/3/discover/movie?language=ru-RU&sort_by=primary_release_date.desc&release_date.lte={}&with_release_type=4%7C5&vote_count.gte=30&page={}",
                    today, page
                ),
                "movie",
            ),
            "popular_series" => (
                "Популярные сериалы",
                "tv",
                format!("https://api.themoviedb.org/3/tv/popular?language=ru-RU&page={}", page),
                "tv",
            ),
            "top_rated" => (
                "Шедевры всех времён",
                "star",
                format!("https://api.themoviedb.org/3/movie/top_rated?language=ru-RU&vote_count.gte=1000&page={}", page),
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
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EpisodeMetadataItem {
    pub season_number: u32,
    pub episode_number: u32,
    pub name: String,
    pub overview: Option<String>,
    pub air_date: Option<String>,
    pub still_path: Option<String>,
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
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SeriesSeasonItem {
    pub season_number: u32,
    pub name: String,
    pub episode_count: u32,
    pub air_date: Option<String>,
    pub poster_path: Option<String>,
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
}

#[derive(Debug, Deserialize)]
struct TmdbImagesResponse {
    #[serde(default)]
    posters: Vec<TmdbPosterItem>,
}

#[derive(Debug, Deserialize)]
struct TmdbPosterItem {
    file_path: String,
    iso_639_1: Option<String>,
    #[serde(default)]
    vote_average: f32,
}
