use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};
use std::sync::atomic::{AtomicUsize, Ordering};

use bitvec::order::Msb0;
use bitvec::vec::BitVec;
use indexmap::IndexMap;
use rkyv::munge::munge;
use rkyv::rancor::Fallible;
use rkyv::vec::{ArchivedVec, VecResolver};
use rkyv::Place;
use rkyv::with::{ArchiveWith, AsString};
use serde::{Deserialize, Serialize, Serializer};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{Semaphore, broadcast, mpsc};
use tracing::{debug, info, trace, warn};
use dashmap::DashMap;
use url::Host;

use crate::client_state_manager::{DownloadSnapshot, FrontendMessage, UiStateEvent, UiStateHandle, UiStateManager, get_snapshot};
use crate::context::AppContext;
use crate::download_writer_manager::DownloadWriterManager;
use crate::plugin_registry::PluginRegistryHandler;
use crate::utils::file_utils::force_delete_file;
use crate::network_manager;
use crate::download::hosts::{DownloadTask, FileTask, FolderTask, TaskType};
use crate::network_manager::{NetworkConfig, NetworkHandle};
use crate::state_manager::StateManager;
use crate::utils::network_utils::BandwidthLimiter;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum DownloadUpdate {
    StatusChanged { id: DownloadId, status: DownloadStatus },
    FileUpdated { id: DownloadId, file_update: FileUpdate },
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

#[derive(Debug, Clone, Copy, Hash, PartialEq, PartialOrd, Eq, Serialize, Deserialize, Ord, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
#[rkyv(derive(Hash, PartialEq, Eq))]
#[serde(transparent)]
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

#[derive(Serialize, Deserialize, Default, Clone, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
pub struct FileSettings {
    pub speed_limit: Option<u64>,
}

#[derive(Serialize, Deserialize, Default, Clone, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
pub struct DownloadSettings {
    pub speed_limit: Option<u64>,
    pub file_settings: HashMap<usize, FileSettings>, 
}

#[derive(Serialize, Deserialize, Default, Clone, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
pub struct HostSettings {
    pub speed_limit: Option<u64>,
}

#[derive(Serialize, Deserialize, Default, Clone, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
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
            let mut state_changed = false;

            for (_, download_item) in &mut download.files {
                let exists_physically = download_item.relative_path().exists();

                match download_item {
                    DownloadType::File(file) => {
                        let should_exist  = match file.status() {
                            FileStatus::Completed => true,

                            // Is only in a predownloaded if we haven't even gotten the metadata
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
                            file.set_status(FileStatus::NotFound);
                            state_changed = true;
                        } 
                        // We check the hash only if the download is completed
                        else if file.status() == FileStatus::Completed  {
                            let hash = hash_file(file.relative_path().to_path_buf()).await;

                            if Some(hash) != file.hash {
                                file.set_status(FileStatus::Failed(FileFailureReason::HashMismatch));
                                state_changed = true;
                            }
                        }
                    },
                    DownloadType::Folder(folder) => {
                        let should_exist  = match folder.status() {
                            // If the folder is fully completed, it absolutely MUST exist.
                            DownloadStatus::Completed |
                            DownloadStatus::CompletedWithErrors => true,

                            // Because of Lazy Creation, the folder might not physically 
                            // exist yet during any of these active or pending states.
                            DownloadStatus::InProgress |
                            DownloadStatus::Paused |
                            DownloadStatus::Retrying |
                            DownloadStatus::Waiting(_) |
                            DownloadStatus::FetchingMetadata |
                            DownloadStatus::Initializing |
                            DownloadStatus::Failed(_) |
                            DownloadStatus::Queued |
                            DownloadStatus::NotFound => false,
                        };

                        if should_exist && !exists_physically {
                            folder.set_status(DownloadStatus::NotFound);
                            state_changed = true;
                        }
                    },
                }
            }

            if state_changed {
                if let Some(_) = download.reconcile_status() {
                    self.db_state_manager.write_download(download).await;
                }
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
pub enum FileStatus {
    Queued,
    Initializing,
    FetchingMetadata,
    InProgress,
    Completed,
    Paused,
    Failed(FileFailureReason),
    NotFound,
    Retrying,
    Waiting(Option<u64>)
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
#[repr(u8)]
pub enum FileFailureReason {
    HashMismatch,
    DiskError,
    ClientError,
    ServerError,
    MetadataFetchError,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
pub enum DownloadStatus {
    Queued,
    Initializing,
    FetchingMetadata,
    InProgress,
    Completed,
    CompletedWithErrors,
    Paused,
    Failed(DownloadFailureReason),
    NotFound,
    Retrying,
    Waiting(Option<u64>)
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
#[repr(u8)]
pub enum DownloadFailureReason {
    HashMismatch,
    DiskError,
    ClientError,
    ServerError,
    MetadataFetchError,
    MultipleErrors,
    AllFilesFailed(FileFailureReason),
}

/// Has either a file or folder as the only item in root
#[derive(Debug, Serialize, Deserialize, Clone, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
pub struct Download {
    id: DownloadId,
    url: String,
    #[rkyv(with = AsString)]
    relative_path: PathBuf,
    status: DownloadStatus,
    pub(crate) files: IndexMap<usize, DownloadType>,
    name: String,
}

impl Download {
    pub const fn url(&self) -> &String {
        &self.url
    }

    pub fn get_file_mut(&mut self, id: &usize) -> Option<&mut FileDownload> {
        match self.files.get_mut(id) {
            Some(DownloadType::File(file)) => Some(file),
            _ => None,
        }
    }

    pub const fn id(&self) -> DownloadId {
        self.id
    }

    pub const fn files(&self) -> &IndexMap<usize, DownloadType> {
        &self.files
    }

    pub const fn files_mut(&mut self) -> &mut IndexMap<usize, DownloadType> {
        &mut self.files
    }

    pub const fn relative_path(&self) -> &PathBuf {
        &self.relative_path
    }

    pub const fn status(&self) -> DownloadStatus {
        self.status
    }

    pub const fn name(&self) -> &String {
        &self.name
    }

    pub fn is_completed(&self) -> bool {
        self.status == DownloadStatus::Completed
    }

    pub fn set_status(&mut self, status: DownloadStatus) {
        self.status = status;
    }
}

impl Download {
    pub fn new(id: usize, value: DownloadTask) -> Self {
        let relative_path = PathBuf::new();

        let mut files = IndexMap::new();
        let mut current_id = 0;
        let name;

        match value.task_type {
            TaskType::File(file_task) => {
                name = file_task.file_name().clone();
                files.insert(current_id, DownloadType::File(FileDownload::new(&file_task, &relative_path, current_id, None)));
            },
            TaskType::Folder(folder_task) => {
                name = folder_task.folder_name().clone();
                Self::process_folder_creation(&folder_task, &relative_path, &mut current_id, &mut files, None);
            },
        }

        Self { 
            id: DownloadId(id),
            url: value.url,
            relative_path: PathBuf::from("./"),
            status: DownloadStatus::Queued,
            files,
            name,
        }
    }

    /// Recalculates the download status. 
    /// Returns `Some(new_status)` if a change occurred, or `None` if it was already correct.
    pub fn reconcile_status(&mut self) -> Option<DownloadStatus> {
        let final_status: DownloadStatus = self.calculate_final_status();
        
        if self.status != final_status {
            self.status = final_status;
            return Some(self.status);
        }
        
        None
    }

    pub fn calculate_final_status(&self) -> DownloadStatus {
        let mut completed_count = 0;
        let mut failed_count = 0;
        let mut not_found_count = 0;
        let mut paused_count = 0;
        let mut active_count = 0;
        let mut total_files = 0;

        let mut first_failure_reason = None;
        let mut multiple_failure_reasons = false;

        for item in self.files.values() {
            if let DownloadType::File(file) = item {
                total_files += 1;

                match file.status() {
                    FileStatus::Completed => completed_count += 1,
                    FileStatus::NotFound => not_found_count += 1,
                    FileStatus::Paused => paused_count += 1,
                    FileStatus::Failed(reason) => {
                        failed_count += 1;
                        if first_failure_reason.is_none() {
                            first_failure_reason = Some(reason);
                        } else if first_failure_reason.as_ref() != Some(&reason) {
                            multiple_failure_reasons = true;
                        }
                    },

                    FileStatus::Queued |
                    FileStatus::Initializing |
                    FileStatus::FetchingMetadata |
                    FileStatus::InProgress |
                    FileStatus::Retrying |
                    FileStatus::Waiting(_) => {
                        active_count += 1; 
                    } 
                }
            }
        }

        if active_count > 0 {
            return match self.status {
                // If the download falsely claims to be finished, revive it!
                DownloadStatus::Completed |
                DownloadStatus::CompletedWithErrors |
                DownloadStatus::Failed(_) |
                DownloadStatus::NotFound |
                DownloadStatus::Paused => DownloadStatus::InProgress,

                // If it's already in an active state just return its current state.
                DownloadStatus::Queued |
                DownloadStatus::Initializing |
                DownloadStatus::FetchingMetadata |
                DownloadStatus::InProgress |
                DownloadStatus::Retrying |
                DownloadStatus::Waiting(_) => self.status,
            };
        }

        if completed_count == total_files {
            DownloadStatus::Completed
        } else if paused_count > 0 {
            // If any file is paused and no files are actively downloading, 
            // the download is considered to be paused.
            DownloadStatus::Paused
        } else if failed_count == total_files {
            // Every single file failed with different errors
            if multiple_failure_reasons {
                DownloadStatus::Failed(DownloadFailureReason::MultipleErrors)
            } 
            // Every single file failed with the same error
            else {
                let file_reason = first_failure_reason.unwrap();
                DownloadStatus::Failed(DownloadFailureReason::AllFilesFailed(file_reason))
            }
        } else if not_found_count == total_files {
            // Every single file was not found
            DownloadStatus::NotFound
        } else {
            // We only reach here if there are no active files, no paused files,
            // and the download is not 100% finished.
            DownloadStatus::CompletedWithErrors 
        }
    }

    fn process_folder_creation(folder_task: &FolderTask, parent_relative_path: &Path, current_id: &mut usize, files: &mut IndexMap<usize, DownloadType>, parent_id: Option<usize>) {
        let mut children = Vec::new();
        let relative_path = parent_relative_path.join(folder_task.folder_name());

        let folder_id = *current_id;
        *current_id += 1;

        for file_type in &folder_task.files {
            match file_type {
                TaskType::File(file_task) => {
                    files.insert(*current_id, DownloadType::File(FileDownload::new(file_task, &relative_path, *current_id, Some(folder_id))));
                    children.push(*current_id);
                    *current_id += 1;
                },
                TaskType::Folder(folder_task) => {
                    Self::process_folder_creation(folder_task, &relative_path, current_id, files, Some(folder_id));
                },
            }
        }

        files.insert(folder_id, DownloadType::Folder(FolderDownload::new(folder_task, parent_relative_path, folder_id, children, parent_id)));
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum DownloadType {
    File(FileDownload),
    Folder(FolderDownload),
}

impl DownloadItem for DownloadType {
    fn parent_id(&self) -> Option<usize> {
        match self {
            DownloadType::File(f) => f.parent_id(),
            DownloadType::Folder(f) => f.parent_id(),
        }
    }

    fn id(&self) -> usize {
        match self {
            DownloadType::File(f) => f.id(),
            DownloadType::Folder(f) => f.id(),
        }
    }

    fn relative_path(&self) -> &PathBuf {
        match self {
            DownloadType::File(f) => f.relative_path(),
            DownloadType::Folder(f) => f.relative_path(),
        }
    }

    fn name(&self) -> &str {
        match self {
            DownloadType::File(f) => f.name(),
            DownloadType::Folder(f) => f.name(),
        }
    }
}

pub trait DownloadItem {
    fn parent_id(&self) -> Option<usize>;

    fn id(&self) -> usize;

    fn relative_path(&self) -> &PathBuf;

    fn name(&self) -> &str;
}

#[derive(Serialize, Deserialize, Clone, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
pub struct FileDownload {
    parent_id: Option<usize>,
    id: usize,
    url: Arc<String>,
    file_name: String,
    #[rkyv(with = AsString)]
    relative_path: PathBuf,
    status: FileStatus,
    #[serde(serialize_with = "serialize_hash")] 
    hash: Option<u128>,
    #[serde(serialize_with = "serialize_chunks")]
    #[rkyv(with = AsBitVec)]
    chunks: BitVec<u8, Msb0>,
    size: Option<FileSize>, // None means we haven't gotten the size yet, unknown means the size can't be known until it
    #[serde(skip)]
    /// tracks consecutive retries
    retries: usize, 
}

pub struct AsBitVec;

pub struct BitVecResolver {
    len: u64,
    inner: VecResolver,
}

#[derive(rkyv::Portable, bytecheck::CheckBytes)]
#[repr(C)]
pub struct ArchivedBitVec {
    pub data: ArchivedVec<u8>,
    pub bit_len: rkyv::rend::u64_le,
}

impl ArchiveWith<BitVec<u8, Msb0>> for AsBitVec {
    type Archived = ArchivedBitVec;
    type Resolver = BitVecResolver;

    fn resolve_with(field: &BitVec<u8, Msb0>, resolver: Self::Resolver, out: Place<Self::Archived>) {
        munge!(let ArchivedBitVec { data, bit_len } = out);

        ArchivedVec::resolve_from_len(field.as_raw_slice().len(), resolver.inner, data);

        bit_len.write(resolver.len.into());
    }
}

impl<S: rkyv::ser::Writer + ?Sized + rkyv::rancor::Fallible + rkyv::ser::Allocator> rkyv::with::SerializeWith<BitVec<u8, Msb0>, S> for AsBitVec {
    fn serialize_with(
        field: &BitVec<u8, Msb0>,
        serializer: &mut S,
    ) -> Result<Self::Resolver, <S as rkyv::rancor::Fallible>::Error> {
        
        Ok(BitVecResolver { 
            len: field.len() as u64,
            inner: ArchivedVec::serialize_from_slice(field.as_raw_slice(), serializer)?
        })
    }
}

impl<D: rkyv::rancor::Fallible + ?Sized> rkyv::with::DeserializeWith<ArchivedBitVec, BitVec<u8, Msb0>, D> for AsBitVec 
    where <D as Fallible>::Error: rkyv::rancor::Source {
    fn deserialize_with(field: &ArchivedBitVec, deserializer: &mut D)
        -> Result<BitVec<u8, Msb0>, <D as rkyv::rancor::Fallible>::Error> {
        let bytes: Vec<u8> = rkyv::Deserialize::deserialize(&field.data, deserializer)?;
        let mut bitvec = BitVec::<u8, Msb0>::from_vec(bytes);
        
        let bit_len: u64 = field.bit_len.into();
        bitvec.truncate(bit_len as usize);
        Ok(bitvec)
    }
}

fn serialize_hash<S>(hash: &Option<u128>, serializer: S) -> Result<S::Ok, S::Error>
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

fn serialize_chunks<S>(chunks: &BitVec<u8, Msb0>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if serializer.is_human_readable() {
        serializer.serialize_none()
    } else {
        chunks.serialize(serializer)
    }
}


impl DownloadItem for FileDownload {
    fn parent_id(&self) -> Option<usize> {
        self.parent_id
    }

    fn id(&self) -> usize {
        self.id
    }

    fn relative_path(&self) -> &PathBuf {
        &self.relative_path
    }

    fn name(&self) -> &str {
        &self.file_name
    }
}

impl Debug for FileDownload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileDownload")
            .field("id", &self.id)
            .field("url", &self.url)
            .field("file_name", &self.file_name)
            .field("relative_path", &self.relative_path)
            .field("status", &self.status)
            .field("hash", &self.hash)
            .field("chunks", &self.chunks.len())
            .finish()
    }
}

impl FileDownload {
    pub(super) fn new(file_task: &FileTask, relative_path: &Path, id: usize, parent_id: Option<usize>) -> Self {
        let relative_path = relative_path.join(file_task.file_name());

        Self { 
            parent_id,
            id,
            url: Arc::new(file_task.url.clone()),
            file_name: file_task.file_name().to_owned(),
            relative_path,
            status: FileStatus::Queued,
            hash: None,
            chunks: BitVec::new(),
            size: None,
            retries: 0,
        }
    }

    pub const fn chunks(&self) -> &BitVec<u8, Msb0> {
        &self.chunks
    }

    pub fn chunks_mut(&mut self) -> &mut BitVec<u8, Msb0> {
        &mut self.chunks
    }

    pub const fn hash(&self) -> Option<u128> {
        self.hash
    }

    pub fn url(&self) -> Arc<String> {
        self.url.clone()
    }

    pub fn status(&self) -> FileStatus {
        self.status
    }

    pub fn set_status(&mut self, new_status: FileStatus) {
        self.status = new_status
    }

    pub fn size(&self) -> Option<FileSize> {
        self.size
    }

    pub fn set_size(&mut self, size: FileSize) {
        self.size = Some(size);
    }

    pub fn retries(&self) -> usize {
        self.retries
    }

    pub fn increment_retries(&mut self) {
        self.retries += 1;
    }

    pub fn reset_retries(&mut self) {
        self.retries = 0;
    }

    pub fn calculate_initial_bytes(&self, chunk_size: u64) -> u64 {
        let chunks = self.chunks();

        if chunks.is_empty() {
            return 0;
        }

        let file_size = match self.size() {
            Some(FileSize::Known(size)) => size,
            _ => return 0,
        };

        if self.status == FileStatus::Completed {
            return file_size;
        }

        let last_chunk_index = chunks.len() - 1;

        // Did we download the very last chunk?
        let has_last_chunk = chunks.get(last_chunk_index).as_deref() == Some(&true);

        let downloaded_chunks = chunks.count_ones() as u64;

        if has_last_chunk {
            // All chunks except the last one are full size
            let standard_bytes = (downloaded_chunks - 1) * chunk_size;
            
            // We calculate the size of the last chunk
            let last_chunk_bytes = self.calculate_chunk_expected_len(
                chunk_size, 
                (last_chunk_index, last_chunk_index + 1), 
                file_size
            );

            standard_bytes + last_chunk_bytes
        } else {
            // If we don't have the last chunk, every chunk we have is standard size
            downloaded_chunks * chunk_size
        }
    }

    fn calculate_chunk_expected_len(&self, chunk_size: u64, range: (usize, usize), file_size: u64) -> u64 {
        let start_byte = range.0 as u64 * chunk_size;
        let theoretical_end = range.1 as u64 * chunk_size;

        let actual_end = std::cmp::min(theoretical_end, file_size);
        let expected_len = actual_end.saturating_sub(start_byte);
        
        expected_len.min(file_size)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
pub struct FolderDownload {
    parent_id: Option<usize>,
    id: usize,
    folder_name: String,
    #[rkyv(with = AsString)]
    relative_path: PathBuf,
    status: DownloadStatus,
    children: Vec<usize>,
}

impl FolderDownload {
    pub(super) fn new(folder_task: &FolderTask, parent_relative_path: &Path, id: usize, children: Vec<usize>, parent_id: Option<usize>) -> Self {
        let relative_path = parent_relative_path.join(folder_task.folder_name());

        Self { 
            parent_id,
            id,
            folder_name: folder_task.folder_name().to_owned(),
            relative_path,
            status: DownloadStatus::Queued,
            children
        }
    }

    pub const fn children(&self) -> &Vec<usize> {
        &self.children
    }

    pub fn status(&self) -> DownloadStatus {
        self.status
    }

    pub fn set_status(&mut self, new_status: DownloadStatus) {
        self.status = new_status
    }
}

impl DownloadItem for FolderDownload {
    fn parent_id(&self) -> Option<usize> {
        self.parent_id
    }

    fn id(&self) -> usize {
        self.id
    }

    fn relative_path(&self) -> &PathBuf {
        &self.relative_path
    }

    fn name(&self) -> &str {
        &self.folder_name
    }
}

#[derive(Debug, Copy, Clone, Deserialize, PartialEq, Eq, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
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