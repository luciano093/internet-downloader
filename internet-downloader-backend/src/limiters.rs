use std::sync::{Arc, Weak};

use dashmap::DashMap;
use url::Host;

use crate::app_settings::DownloadSettings;
use crate::download::items::{DownloadId, FileId};
use crate::utils::network_utils::BandwidthLimiter;

pub struct DownloadLimiterGroup {
    download_limiter: Arc<BandwidthLimiter>,
    file_limiters: DashMap<FileId, Arc<BandwidthLimiter>>,
}

impl DownloadLimiterGroup {
    pub fn new() -> Self {
        let download_limiter = BandwidthLimiter::new(0);
        download_limiter.set_unlimited(true);

        Self { 
            download_limiter: Arc::new(download_limiter),
            file_limiters: DashMap::new()
        }
    }

    pub fn from_settings(settings: Option<&DownloadSettings>) -> Self {
        let group = Self::new();

        if let Some(settings) = settings {
            if let Some(limit) = settings.speed_limit {
                group.download_limiter.set_unlimited(false);
                group.download_limiter.set_limit(limit);
            }

            for (&file_id, file_setting) in &settings.file_settings {
                if let Some(limit) = file_setting.speed_limit {
                    let f_limit = BandwidthLimiter::new(limit);
                    f_limit.set_unlimited(false);
                    group.file_limiters.insert(file_id, Arc::new(f_limit));
                }
            }
        }

        group
    }

    pub fn download_limiter(&self) -> Arc<BandwidthLimiter> {
        self.download_limiter.clone()
    }

    pub fn file_limiters(&self) -> &DashMap<FileId, Arc<BandwidthLimiter>> {
        &self.file_limiters
    }
}

pub struct LimiterRegistry {
    global_limit: Arc<BandwidthLimiter>,
    host_limits: DashMap<Host, Weak<BandwidthLimiter>>,
    downloads: DashMap<DownloadId, Weak<DownloadLimiterGroup>>,
}

impl LimiterRegistry {
    pub fn new() -> Self {
        let global_limit = BandwidthLimiter::new(0);
        global_limit.set_unlimited(true);

        Self {
            global_limit: Arc::new(global_limit),
            host_limits: DashMap::new(),
            downloads: DashMap::new(),
        }
    }

    pub fn global_limit(&self) -> Arc<BandwidthLimiter> {
        self.global_limit.clone()
    }

    pub fn host_limits(&self) -> &DashMap<Host, Weak<BandwidthLimiter>> {
        &self.host_limits
    }

    pub fn downloads(&self) -> &DashMap<DownloadId, Weak<DownloadLimiterGroup>> {
        &self.downloads
    }
}
