use std::collections::HashMap; 
use std::sync::atomic::{AtomicUsize, Ordering};

use indexmap::IndexMap;

use crate::db::state_manager::StateManager;
use crate::download::items::{Download, DownloadId};

pub struct DownloadRegistry {
    url_map: HashMap<String, DownloadId>,
    id_map: HashMap<DownloadId, String>,
    next_id: AtomicUsize,
    removed_downloads: HashMap<DownloadId, bool>,
}

impl DownloadRegistry {
    pub fn new() -> Self {
        Self {
            url_map: HashMap::new(),
            id_map: HashMap::new(),
            next_id: AtomicUsize::new(0),
            removed_downloads: HashMap::new(),
        }
    }

    pub async fn from_db(db_manager: &StateManager) -> Self {
        let existing_urls = db_manager.get_all_download_urls().await.unwrap();
        let next_id = existing_urls.iter().map(|(id, _url)| id).max().copied().map(|max_id| max_id + 1).unwrap_or(0);

        let mut registry = Self { 
            url_map: HashMap::new(),
            id_map: HashMap::new(),
            next_id: AtomicUsize::new(next_id),
            removed_downloads: HashMap::new(),
        };

        for (id, url) in existing_urls {
            registry.url_map.insert(url.clone(), DownloadId(id));
            registry.id_map.insert(DownloadId(id), url);
        }

        registry
    }

    pub fn add_downloads(&mut self, downloads: &IndexMap<DownloadId, Download>) {
        for (id, download) in downloads {
            self.url_map.insert(download.url().to_string(), *id);
            self.id_map.insert(*id, download.url().to_string());
        }
    }
    
    pub fn register(&mut self, url: String) -> DownloadId {
        let id = self.next_id();
        self.url_map.insert(url.clone(), id);
        self.id_map.insert(id, url);

        id
    }
    
    pub fn mark_removed(&mut self, id: DownloadId, from_disk: bool) {
        self.removed_downloads.insert(id, from_disk);
    }
    
    pub fn finalize_removed(&mut self, id: &DownloadId) -> Option<bool> {
        if let Some(url) = self.id_map.remove(id) {
            self.url_map.remove(&url);
        }

        self.removed_downloads.remove(id)
    }

    pub fn is_marked_for_removal(&mut self, id: &DownloadId) -> bool {
        self.removed_downloads.contains_key(&id)
    }
    
    pub fn next_id(&self) -> DownloadId {
        DownloadId(self.next_id.fetch_add(1, Ordering::Relaxed))
    }

    pub fn url_map(&self) -> &HashMap<String, DownloadId> {
        &self.url_map
    }

    pub fn contains_url(&self, url: &str) -> bool {
        self.url_map.contains_key(url)
    }
    
    pub fn lookup_url(&self, download_id: &DownloadId) -> Option<&String> {
        self.id_map.get(download_id)
    }
}
