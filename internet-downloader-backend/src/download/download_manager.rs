use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::ops::Deref;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, Weak};
use std::sync::atomic::{AtomicUsize, Ordering};

use bitvec::order::Msb0;
use bitvec::vec::BitVec;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize, Serializer};
use strum_macros::{EnumDiscriminants, EnumString, IntoStaticStr};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{Semaphore, broadcast, mpsc};
use tracing::{debug, info, trace, warn};
use dashmap::DashMap;
use url::Host;

use crate::client_state_manager::{DownloadSnapshot, FrontendMessage, UiStateEvent, UiStateHandle, UiStateManager, get_snapshot};
use crate::context::AppContext;
use crate::db::rows::{GlobalSettingsRow, HostSettingsRow, JoinedDownloadSettingsRow};
use crate::download::items::{Download, DownloadItem, DownloadType};
use crate::download::status::{DownloadStatus, FileStatus};
use crate::download_writer_manager::DownloadWriterManager;
use crate::plugin_registry::PluginRegistryHandler;
use crate::utils::file_utils::force_delete_file;
use crate::network_manager;
use crate::network_manager::{NetworkConfig, NetworkHandle};
use crate::db::state_manager::StateManager;
use crate::utils::network_utils::BandwidthLimiter;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum DownloadUpdate {
    StatusChanged { id: DownloadId, status: DownloadStatus },
    ItemUpdated { id: DownloadId, item_update: ItemUpdate }, 
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum ItemUpdate {
    File(FileUpdate),
    Folder(FolderUpdate),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum FileUpdate {
    Status { id: usize, status: FileStatus },
    Hash { id: usize, hash: u128 },
    FileSize { id: usize, len: u64 },
    BytesDownloaded { id: usize, len: u64 },
}

impl FileUpdate {
    pub fn id(&self) -> usize {
        match self {
            FileUpdate::Status { id, .. } => *id,
            FileUpdate::Hash { id, .. } => *id,
            FileUpdate::FileSize { id, .. } => *id,
            FileUpdate::BytesDownloaded { id, .. } => *id,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum FolderUpdate {
    Status { id: usize, status: DownloadStatus }, 
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, PartialOrd, Eq, Serialize, Deserialize, Ord, sqlx::Type)]
#[serde(transparent)]
#[sqlx(transparent)]
pub struct DownloadId(pub usize);

impl Deref for DownloadId {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Display for DownloadId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, PartialOrd, Eq)]
pub enum DownloadReturnStatus {
    Completed,
    Canceled,
    Paused,
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, PartialOrd, Eq)]
pub enum DownloadCommand {
    Pause,
    Resume,
    Cancel,
}

// To maybe add in the future:
// Skip a file in a download
// Set download priority
// Force start a download? (move it to top of queue)
// Force retry a failed download
// Reload a plugin (manually)
// Set host max connections
pub enum ManagerCommand {
    QueueDownload(String),
    RemoveDownload(DownloadId, bool), // true if we want to remove from disk too
    CleanUpDownload(DownloadId),
    PauseDownload(DownloadId),
    ResumeDownload(DownloadId),
    Shutdown,
    SetGlobalSpeedLimit(Option<u64>),
    SetHostSpeedLimit(String, Option<u64>), // String can be a hostname or url
    SetDownloadSpeedLimit(DownloadId, Option<u64>),
    SetFileSpeedLimit(DownloadId, usize, Option<u64>),
}

pub struct DownloadLimiterGroup {
    download_limiter: Arc<BandwidthLimiter>,
    file_limiters: DashMap<usize, Arc<BandwidthLimiter>>,
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

    pub fn file_limiters(&self) -> &DashMap<usize, Arc<BandwidthLimiter>> {
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

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct FileSettings {
    pub speed_limit: Option<u64>,
}

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct DownloadSettings {
    pub speed_limit: Option<u64>,
    pub file_settings: HashMap<usize, FileSettings>, 
}

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct HostSettings {
    pub speed_limit: Option<u64>,
}

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct AppSettings {
    pub global_speed_limit: Option<u64>,
    pub download_settings: HashMap<DownloadId, DownloadSettings>,
    pub host_settings: HashMap<String, HostSettings>
}

impl AppSettings {
    pub fn new() -> Self {
        Self {
            global_speed_limit: None,
            download_settings: HashMap::new(),
            host_settings: HashMap::new(),
        }
    }

    pub fn global_speed_limit(&self) -> Option<u64> {
        self.global_speed_limit
    }

    pub fn set_global_speed_limit(&mut self, new_speed_limit: Option<u64>) {
        self.global_speed_limit = new_speed_limit;
    }

    pub fn get_download_settings(&self, download_id: DownloadId) -> Option<DownloadSettings> {
        self.download_settings.get(&download_id).cloned()
    }

    pub fn from_db(global_settings_row: GlobalSettingsRow, host_settings_rows: Vec<HostSettingsRow>, joined_download_settings: Vec<JoinedDownloadSettingsRow>) -> Self {
        let mut host_settings = HashMap::new();

        for row in host_settings_rows {
            let host_settings_object = HostSettings {
                speed_limit: row.speed_limit.map(|speed_limit| speed_limit as u64),
            };

            host_settings.insert(row.host, host_settings_object);
        }

        let mut download_settings = HashMap::new();

        for row in joined_download_settings {
            let download_settings_object = download_settings.entry(DownloadId(row.download_id as usize)).or_insert_with(|| 
                DownloadSettings {
                    speed_limit: row.download_speed_limit.map(|speed_limit| speed_limit as u64),
                    file_settings: HashMap::new()
                });

            if let Some(item_id) = row.item_id {
                download_settings_object.file_settings.insert(item_id as usize, 
                    FileSettings { speed_limit: row.file_speed_limit.map(|speed_limit| speed_limit as u64) }
                );
            }
        }

        Self {
            global_speed_limit: global_settings_row.global_speed_limit.map(|speed_limit| speed_limit as u64),
            download_settings: download_settings,
            host_settings: host_settings,
        }
    }
}

#[derive(Debug)]
pub struct DownloadManager {
    next_id: Option<AtomicUsize>,
    db_state_manager: StateManager,
    unprocessed_downloads: IndexMap<DownloadId, Download>,
    ui_state_handle: Option<UiStateHandle>,
    command_sender: Option<UnboundedSender<ManagerCommand>>,
    concurrency_limit: Arc<Semaphore>,
}

impl DownloadManager {
    pub fn new(db_state_manager: StateManager) -> Self {
        DownloadManager {
            next_id: Some(AtomicUsize::new(0)), 
            db_state_manager,
            unprocessed_downloads: IndexMap::new(),
            ui_state_handle: None,
            command_sender: None,
            concurrency_limit: Arc::new(Semaphore::const_new(10))
        }
    }

    pub async fn load_state(&mut self) {
        let restored_downloads = self.db_state_manager.load_downloads().await.unwrap();

        let max_id = restored_downloads.keys().max().copied().unwrap_or(DownloadId(0));

        self.next_id.as_mut().unwrap().store(*max_id + 1, Ordering::Relaxed);

        debug!(count = ?restored_downloads.len(), "Restored download from disk");
        trace!("Detailed download restore data:\n{:#?}", restored_downloads);

        for (id, download) in restored_downloads {
            self.unprocessed_downloads.insert(id, download.clone());
        }
    }

    pub async fn verify_downloads(&mut self) {
        for (_, download) in &mut self.unprocessed_downloads {
            let mut pending_changes = Vec::new();

            for (&id, download_item) in &mut download.files {

                if let DownloadType::File(file) = download_item {
                    let exists_physically = file.relative_path().exists();

                    let should_exist  = match file.status() {
                        FileStatus::Completed => true,

                        // A file should only exist on disk once metadata has been fetched (file size is not None).
                        FileStatus::Paused | FileStatus::InProgress | FileStatus::Waiting(_) | FileStatus::Retrying => {
                            file.size().is_some() 
                        },

                        FileStatus::Failed(_) |
                        FileStatus::Queued |
                        FileStatus::Initializing |
                        FileStatus::FetchingMetadata |
                        FileStatus::NotFound  => false,
                    };

                    // Check if file is missing (not queued and doesn't exist)
                    if should_exist && !exists_physically {
                        pending_changes.push((id, FileStatus::NotFound));
                    } 


                    // We check the hash only if the download is completed
                    else if file.status() == FileStatus::Completed  {
                        let hash = hash_file(file.relative_path().to_path_buf()).await;

                        if Some(hash) != file.hash() {
                            pending_changes.push((id, FileStatus::Failed(FileFailureReason::HashMismatch)));
                        }
                    }
                }
            }

            let mut state_changed = false;

            // Apply all file status changes
            for (id, new_status) in pending_changes {
                if let Some(changed_items) = download.set_file_status(id, new_status) {
                    if !changed_items.is_empty() {
                        state_changed = true;
                    }
                }
            }

            if state_changed {
                // We should always write the change to db when the state has changed
                self.db_state_manager.write_download(download).await;
            }
        }
    }

    pub async fn queue_download(&mut self, url: String) -> Result<(), ()> {
        if let Some(sender) = &self.command_sender {
            sender.send(ManagerCommand::QueueDownload(url)).unwrap();
        }

        Ok(())
    }

    pub async fn remove_download(&mut self, id: DownloadId, from_disk: bool) {
        if let Some(sender) = &self.command_sender {
            let _ = sender.send(ManagerCommand::RemoveDownload(id, from_disk));
        }
    }

    pub async fn pause_download(&mut self, download_id: DownloadId) {
        if let Some(sender) = &self.command_sender {
            let _ = sender.send(ManagerCommand::PauseDownload(download_id));
        }
    }

    pub async fn resume_download(&mut self, download_id: DownloadId) {
        if let Some(sender) = &self.command_sender {
            let _ = sender.send(ManagerCommand::ResumeDownload(download_id));
        }
    }

    pub fn set_global_limit(&self, limit: Option<u64>) {
        if let Some(sender) = &self.command_sender {
            let _ = sender.send(ManagerCommand::SetGlobalSpeedLimit(limit));
        }
    }

    pub fn set_host_limit(&self, host: String, limit: Option<u64>) {
        if let Some(sender) = &self.command_sender {
            let _ = sender.send(ManagerCommand::SetHostSpeedLimit(host, limit));
        }
    }

    pub fn set_download_limit(&self, download_id: DownloadId, limit: Option<u64>) {
        if let Some(sender) = &self.command_sender {
            let _ = sender.send(ManagerCommand::SetDownloadSpeedLimit(download_id, limit));
        }
    }

    pub fn set_file_limit(&self, download_id: DownloadId, file_id: usize, limit: Option<u64>) {
        if let Some(sender) = &self.command_sender {
            let _ = sender.send(ManagerCommand::SetFileSpeedLimit(download_id, file_id, limit));
        }
    }

    pub async fn start_processing(&mut self) {
        let (command_sender, mut command_receiver) = mpsc::unbounded_channel::<ManagerCommand>();

        self.command_sender = Some(command_sender.clone()); 

        let ui_state_manager = UiStateManager::new();
        let ui_state_handle = ui_state_manager.start();
        self.ui_state_handle = Some(ui_state_handle);

        
        // Clone shared resources
        let ui_event_sender = self.ui_state_handle.as_ref().unwrap().get_event_sender();
        let concurrency_limit = self.concurrency_limit.clone();
        let db_manager = self.db_state_manager.clone();

        let mut queue: IndexMap<DownloadId, Download> = self.unprocessed_downloads.drain(..).collect();

        let plugin_registry = PluginRegistryHandler::spawn().await;

        
        let network_config = NetworkConfig::default();
        let client = network_manager::build_global_client(&network_config);
        
        let app_context = AppContext {
            client,
            network_config,
            download_manager: command_sender.clone(),
            ui_sender: ui_event_sender.clone(),
            db_manager: db_manager.clone(),
            plugin_registry,
            writer_handle: DownloadWriterManager::new(),
        };

        let (network_manager, _) = NetworkHandle::spawn(app_context.clone()).await;

        let mut app_settings = AppSettings::new();

        // Download registry for deduplication purposes
        let mut url_registry: HashMap<String, DownloadId> = HashMap::new();
        let mut id_registry: HashMap<DownloadId, String> = HashMap::new();

        let existing_downloads = db_manager.get_all_download_urls().await; 

        // Add existing downloads to registry
        for (id, url) in existing_downloads {
            url_registry.insert(url.clone(), DownloadId(id));
            id_registry.insert(DownloadId(id), url);
        }

        // Add queued downloads to registry
        for (id, download) in &queue {
            url_registry.insert(download.url().to_string(), *id);
            id_registry.insert(*id, download.url().to_string());
        }

        let next_id = self.next_id.take().unwrap();
        
        let mut removed_downloads = HashMap::new();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(command) = command_receiver.recv() => {
                        match command {
                            ManagerCommand::QueueDownload(url) => {
                                debug!("registry: {:#?}", url_registry);
                                debug!("url: {}", url);
                                if url_registry.contains_key(&url) {
                                    debug!("Download already exists: {}", url);
                                    continue; 
                                }

                                let id = DownloadId(next_id.fetch_add(1, Ordering::Relaxed));
                                url_registry.insert(url.clone(), id);
                                id_registry.insert(id, url.clone());

                                network_manager.queue_download(url, id);
                            },
                            ManagerCommand::RemoveDownload(id, from_disk) => {
                                info!("Removing download");
                                // First, we set it as removed
                                removed_downloads.insert(id, from_disk);

                                 // Try to remove from Pending Queue
                                if queue.shift_remove(&id).is_some() {
                                    debug!("Removed pending download {}", id);
                                    let _ = command_sender.send(ManagerCommand::CleanUpDownload(id));
                                } 
                                // If not in queue, it might be running. Send Cancel signal.
                                else if let Some(url) = id_registry.get(&id) {
                                    // In this case, we have to wait for the download to finish so it sends the clean up command
                                    network_manager.cancel_download(url.clone(), DownloadId(*id));
                                }
                                // Else if it's already done or doesn't exist; just clean up
                                else {
                                    debug!("Removed completed download {}", id);
                                    let _ = command_sender.send(ManagerCommand::CleanUpDownload(id));
                                } 
                            },
                            ManagerCommand::CleanUpDownload(download_id) => {
                                // Remove from registry now that we know the download is 100% removed
                                if let Some(url) = id_registry.remove(&download_id) {
                                    url_registry.remove(&url);
                                }
                                
                                // Finally, we clean it up from the set
                                if let Some(from_disk) = removed_downloads.remove(&download_id) {
                                    if from_disk {
                                        match db_manager.load_download(download_id).await {
                                            Ok(download) => {
                                                for file_type in download.files().values() {
                                                    if let DownloadType::File(file) = file_type {
                                                        let path = file.relative_path(); 
                                                        if path.exists() {
                                                            force_delete_file(&path); 
                                                        }
                                                    }
                                                }
                                            }
                                            Err(_) => {
                                                // We couldn't load it the download from the db. 
                                                // Maybe it was never saved to the db?
                                                warn!("Could not load download {} from DB to delete physical files. Skipping file deletion.", download_id);
                                            }
                                        } 
                                    }

                                    db_manager.delete_download(download_id).await;
                                    let _ = ui_event_sender.send(UiStateEvent::RemoveDownload(*download_id));
                                }

                                info!("Download cleaned up");
                            },
                            ManagerCommand::PauseDownload(download_id) => {
                                if let Some(url) = id_registry.get(&download_id) {
                                    network_manager.pause_download(url.to_string(), download_id);
                                }
                            },
                            ManagerCommand::ResumeDownload(download_id) => if let Ok(download) = db_manager.load_download(download_id).await {
                                let download_settings = app_settings.get_download_settings(download_id);

                                network_manager.resume_download(download, download_settings);
                            },
                            ManagerCommand::Shutdown => {
                                break;
                            },
                            ManagerCommand::SetGlobalSpeedLimit(limit) => {
                                app_settings.set_global_speed_limit(limit);

                                app_context.db_manager.write_app_settings(&app_settings).await;

                                network_manager.set_global_limit(limit);
                            },
                            ManagerCommand::SetHostSpeedLimit(host, limit) => {
                                app_settings.host_settings
                                    .entry(host.clone())
                                    .or_default()
                                    .speed_limit = limit;

                                app_context.db_manager.write_app_settings(&app_settings).await;

                                network_manager.set_host_limit(host, limit);
                            },
                            ManagerCommand::SetDownloadSpeedLimit(download_id, limit) => {
                                if let Some(download_settings) = app_settings.download_settings.get_mut(&download_id) {
                                    download_settings.speed_limit = limit;
                                }

                                app_context.db_manager.write_app_settings(&app_settings).await;

                                network_manager.set_download_limit(download_id, limit);
                            },
                            ManagerCommand::SetFileSpeedLimit(download_id, file_id, limit) => {
                                if let Some(download_settings) = app_settings.download_settings.get_mut(&download_id)
                                    && app_context.db_manager.file_exists(download_id, file_id).await {
                                        let file_settings = download_settings.file_settings.entry(file_id).or_default();
                                        file_settings.speed_limit = limit;

                                        app_context.db_manager.write_app_settings(&app_settings).await;
                                        network_manager.set_file_limit(download_id, file_id, limit);
                                } else {
                                    warn!("Tried to set the file speed limit for a non-existent file. Download id: {}, file id: {}", download_id, file_id);
                                }
                            },
                        }
                    }

                    Ok(_) = concurrency_limit.clone().acquire_owned(), if !queue.is_empty() => {
                        if let Some((_, download)) = queue.shift_remove_index(0) {
                            let _ = command_sender.send(ManagerCommand::QueueDownload(download.url().to_string()));
                        }
                    }
                }
            }
        });
    }

    pub fn download_subscribe(&self) -> broadcast::Receiver<FrontendMessage> {
        self.ui_state_handle.as_ref().unwrap().subscribe()
    }

    pub async fn get_snapshot(&self) -> DownloadSnapshot {
        get_snapshot(&self.db_state_manager).await
    }
}

async fn hash_file(path: PathBuf) -> u128 {
    tokio::task::spawn_blocking(move || {
        let mut hasher = blake3::Hasher::new();
        
        hasher.update_mmap_rayon(&path).expect("Failed to hash file");

        let mut output = [0u8; 16];

        hasher.finalize_xof().fill(&mut output);

        u128::from_le_bytes(output)
    }).await.expect("Hashing task panicked")
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, IntoStaticStr, EnumString)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "state", content = "value")]
#[strum(serialize_all = "snake_case")]
pub enum FileFailureReason {
    HashMismatch,
    DiskError,
    ClientError,
    ServerError,
    MetadataFetchError,
    BadPath,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, IntoStaticStr, EnumDiscriminants, Default)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "state", content = "value")]
#[strum(serialize_all = "snake_case")]
#[strum_discriminants(derive(EnumString, IntoStaticStr))]
#[strum_discriminants(name(DownloadFailureReasonParse))] 
#[strum_discriminants(strum(serialize_all = "snake_case"))]
pub enum DownloadFailureReason {
    HashMismatch,
    DiskError,
    ClientError,
    ServerError,
    MetadataFetchError,
    MultipleErrors,
    AllFilesFailed(FileFailureReason),
    FilesMissingFromDisk,
    StateDesynchronized,
    BadPath,
    #[default]
    Unknown,
}

impl DownloadFailureReason {
    pub fn from_db_string(reason_str: &str) -> Option<Self> {
        if let Some((_prefix, inner_str)) = reason_str.split_once(':') {
            let inner_reason = FileFailureReason::from_str(inner_str).ok()?;
            return Some(Self::AllFilesFailed(inner_reason));
        }
        
        let parsed_reason = DownloadFailureReasonParse::from_str(reason_str).ok()?;

        let reason = Some(match parsed_reason {
            DownloadFailureReasonParse::HashMismatch => Self::HashMismatch,
            DownloadFailureReasonParse::DiskError => Self::DiskError,
            DownloadFailureReasonParse::ClientError => Self::ClientError,
            DownloadFailureReasonParse::ServerError => Self::ServerError,
            DownloadFailureReasonParse::MetadataFetchError => Self::MetadataFetchError,
            DownloadFailureReasonParse::MultipleErrors => Self::MultipleErrors,
            DownloadFailureReasonParse::FilesMissingFromDisk => Self::FilesMissingFromDisk,
            DownloadFailureReasonParse::StateDesynchronized => Self::StateDesynchronized,
            DownloadFailureReasonParse::Unknown => Self::Unknown,
            DownloadFailureReasonParse::BadPath => Self::BadPath,
            
            // Fallback if for some reason we still get here
            DownloadFailureReasonParse::AllFilesFailed => return None,
        });

        reason
    }
}

pub fn serialize_hash<S>(hash: &Option<u128>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if serializer.is_human_readable() {
        match hash {
            Some(v) => serializer.collect_str(v),
            None => serializer.serialize_none(),
        }
    } else {
        hash.serialize(serializer)
    }
}

pub fn serialize_chunks<S>(chunks: &BitVec<u8, Msb0>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if serializer.is_human_readable() {
        serializer.serialize_none()
    } else {
        chunks.serialize(serializer)
    }
}

#[derive(Debug, Copy, Clone, Deserialize, PartialEq, Eq)]
pub enum FileSize {
    Unknown,
    Known(u64)
}

impl Serialize for FileSize {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer {
        match self {
            FileSize::Unknown => "unknown".serialize(serializer),
            FileSize::Known(size) => size.serialize(serializer),
        }
    }
}