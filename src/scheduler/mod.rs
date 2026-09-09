use crate::api::AppState;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tracing::{error, info};

pub struct UpdateScheduler;

impl UpdateScheduler {
    pub fn start(state: AppState) {
        if !state.config.imdb.auto_update {
            info!("Automated IMDb updates are disabled in configuration.");
            return;
        }

        let interval_hours = state.config.imdb.check_interval_hours.max(1);
        let check_duration = Duration::from_secs(interval_hours * 3600);

        info!(
            "Starting automated IMDb update scheduler (interval: {} hours)",
            interval_hours
        );

        tokio::spawn(async move {
            // Initial check after 10 seconds of startup
            tokio::time::sleep(Duration::from_secs(10)).await;

            loop {
                info!("Scheduler: checking for IMDb dump updates...");
                let has_updates = match state.pipeline.downloader().check_for_updates().await {
                    Ok(upd) => upd,
                    Err(e) => {
                        error!("Scheduler: failed to check for updates: {}", e);
                        false
                    }
                };

                if has_updates {
                    if state
                        .is_indexing
                        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                    {
                        info!("Scheduler: new dump updates available, launching indexing pipeline...");
                        let is_indexing_flag = state.is_indexing.clone();
                        let mut manager = state.manager.write().await;
                        let result = state.pipeline.run_indexing(&mut manager, false).await;

                        match result {
                            Ok(updated) => {
                                info!("Scheduler: indexing completed (updated: {})", updated);
                            }
                            Err(e) => {
                                error!("Scheduler: indexing pipeline failed: {}", e);
                            }
                        }
                        is_indexing_flag.store(false, Ordering::SeqCst);
                    } else {
                        info!("Scheduler: another indexing process is already running, skipping trigger.");
                    }
                } else {
                    info!("Scheduler: all IMDb dumps are up to date.");
                }

                // Sleep until next check interval
                tokio::time::sleep(check_duration).await;
            }
        });
    }
}
