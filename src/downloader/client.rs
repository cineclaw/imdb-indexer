use anyhow::{Context, Result};
use futures_util::StreamExt;
use reqwest::header::{ETAG, LAST_MODIFIED};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use tracing::{info, warn};

pub const DUMP_BASICS: &str = "title.basics.tsv.gz";
pub const DUMP_AKAS: &str = "title.akas.tsv.gz";
pub const DUMP_RATINGS: &str = "title.ratings.tsv.gz";

pub const ALL_DUMPS: [&str; 3] = [DUMP_RATINGS, DUMP_AKAS, DUMP_BASICS];

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DumpMetadata {
    pub filename: String,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub content_length: Option<u64>,
    pub downloaded_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DownloaderState {
    pub dumps: HashMap<String, DumpMetadata>,
    pub last_checked_at: Option<chrono::DateTime<chrono::Utc>>,
    pub last_indexed_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl DownloaderState {
    pub fn load(path: &Path) -> Self {
        if path.exists() {
            if let Ok(content) = fs::read_to_string(path) {
                if let Ok(state) = serde_json::from_str(&content) {
                    return state;
                }
            }
        }
        Self::default()
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let json = serde_json::to_string_pretty(self)?;
        let tmp_path = path.with_extension("tmp");
        fs::write(&tmp_path, json)?;
        fs::rename(&tmp_path, path)?;
        Ok(())
    }
}

pub struct ImdbDownloader {
    base_url: String,
    download_dir: PathBuf,
    state_file: PathBuf,
    client: reqwest::Client,
}

impl ImdbDownloader {
    pub fn new<P: AsRef<Path>>(base_url: &str, storage_dir: P) -> Self {
        let download_dir = storage_dir.as_ref().join("downloads");
        let state_file = storage_dir.as_ref().join("state.json");

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(600))
            .build()
            .unwrap_or_default();

        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            download_dir,
            state_file,
            client,
        }
    }

    pub fn download_dir(&self) -> &Path {
        &self.download_dir
    }

    pub fn get_dump_path(&self, filename: &str) -> PathBuf {
        self.download_dir.join(filename)
    }

    /// Check if remote dumps have changed via HTTP HEAD requests
    pub async fn check_for_updates(&self) -> Result<bool> {
        let mut state = DownloaderState::load(&self.state_file);
        let mut has_updates = false;

        for filename in ALL_DUMPS {
            let target_path = self.get_dump_path(filename);
            if !target_path.exists() {
                info!("Dump {:?} does not exist locally, update needed", filename);
                return Ok(true);
            }

            let url = format!("{}/{}", self.base_url, filename);
            let head_resp = self.client.head(&url).send().await;

            match head_resp {
                Ok(resp) => {
                    let etag = resp
                        .headers()
                        .get(ETAG)
                        .and_then(|h| h.to_str().ok())
                        .map(|s| s.to_string());
                    let last_modified = resp
                        .headers()
                        .get(LAST_MODIFIED)
                        .and_then(|h| h.to_str().ok())
                        .map(|s| s.to_string());

                    if let Some(cached) = state.dumps.get(filename) {
                        let etag_changed = etag.is_some() && etag != cached.etag;
                        let mod_changed = last_modified.is_some() && last_modified != cached.last_modified;

                        if etag_changed || mod_changed {
                            info!(
                                "Dump {} has updated on server (etag: {:?}, last_mod: {:?})",
                                filename, etag, last_modified
                            );
                            has_updates = true;
                            break;
                        }
                    } else {
                        has_updates = true;
                        break;
                    }
                }
                Err(e) => {
                    warn!("Failed to HEAD {}: {}", url, e);
                }
            }
        }

        state.last_checked_at = Some(chrono::Utc::now());
        let _ = state.save(&self.state_file);

        Ok(has_updates)
    }

    /// Downloads all necessary dumps with streaming (low RAM), checking ETags
    pub async fn download_all(&self, force: bool) -> Result<bool> {
        fs::create_dir_all(&self.download_dir)?;
        let mut state = DownloaderState::load(&self.state_file);
        let mut downloaded_any = false;

        for filename in ALL_DUMPS {
            let target_path = self.get_dump_path(filename);
            let url = format!("{}/{}", self.base_url, filename);

            let head_resp = self.client.head(&url).send().await?;
            let etag = head_resp
                .headers()
                .get(ETAG)
                .and_then(|h| h.to_str().ok())
                .map(|s| s.to_string());
            let last_modified = head_resp
                .headers()
                .get(LAST_MODIFIED)
                .and_then(|h| h.to_str().ok())
                .map(|s| s.to_string());
            let content_len = head_resp.content_length();

            let needs_download = if force || !target_path.exists() {
                true
            } else if let Some(cached) = state.dumps.get(filename) {
                (etag.is_some() && etag != cached.etag)
                    || (last_modified.is_some() && last_modified != cached.last_modified)
            } else {
                true
            };

            if !needs_download {
                info!("Dump {} is up to date, skipping download", filename);
                continue;
            }

            info!("Starting download of {} from {}...", filename, url);
            let resp = self.client.get(&url).send().await?.error_for_status()?;

            let temp_file_path = self.download_dir.join(format!("{}.download", filename));
            let file = File::create(&temp_file_path)
                .with_context(|| format!("Failed to create temp file {:?}", temp_file_path))?;
            let mut writer = BufWriter::with_capacity(1024 * 1024, file); // 1 MB buffer

            let mut stream = resp.bytes_stream();
            let mut downloaded_bytes: u64 = 0;

            while let Some(chunk_result) = stream.next().await {
                let chunk = chunk_result?;
                writer.write_all(&chunk)?;
                downloaded_bytes += chunk.len() as u64;
            }

            writer.flush()?;
            drop(writer);

            fs::rename(&temp_file_path, &target_path)?;
            info!(
                "Successfully downloaded {} ({} MB)",
                filename,
                downloaded_bytes / (1024 * 1024)
            );

            state.dumps.insert(
                filename.to_string(),
                DumpMetadata {
                    filename: filename.to_string(),
                    etag,
                    last_modified,
                    content_length: content_len.or(Some(downloaded_bytes)),
                    downloaded_at: Some(chrono::Utc::now()),
                },
            );
            downloaded_any = true;
        }

        state.last_checked_at = Some(chrono::Utc::now());
        state.save(&self.state_file)?;

        Ok(downloaded_any)
    }

    pub fn mark_indexed(&self) -> Result<()> {
        let mut state = DownloaderState::load(&self.state_file);
        state.last_indexed_at = Some(chrono::Utc::now());
        state.save(&self.state_file)
    }

    pub fn get_state(&self) -> DownloaderState {
        DownloaderState::load(&self.state_file)
    }
}
