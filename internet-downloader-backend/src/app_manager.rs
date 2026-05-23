use std::fmt::Debug;
use std::str::FromStr;
use std::sync::{Arc, Weak};

use bitvec::order::Msb0;
use bitvec::vec::BitVec;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize, Serializer};
use strum_macros::{EnumDiscriminants, EnumString, IntoStaticStr};
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, info, trace, warn};
use dashmap::DashMap;
use url::Host;

use crate::app_settings::{AppSettings, DownloadSettings};
use crate::client_state_manager::{FrontendMessage, UiManagerHandle, get_snapshot};
use crate::context::AppContext;
use crate::download::items::{ActiveOperation, Download, DownloadId, DownloadItem, FileId, FolderId};
use crate::download::status::{DownloadStatus, FileStatus};
use crate::download::verifier::VerifierHandle;
use crate::download_registry::DownloadRegistry;
use crate::download_writer_manager::DownloadWriterManager;
use crate::plugin_registry::PluginRegistryHandler;
use crate::utils::file_utils::force_delete_file;
use crate::network_manager;
use crate::network_manager::{NetworkConfig, NetworkHandle};
use crate::db::state_manager::StateManager;
use crate::utils::network_utils::BandwidthLimiter;
use crate::verification_tracker::VerificationTracker;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum DownloadUpdate {
    StatusChanged { id: DownloadId, status: DownloadStatus },
    OperationChanged { id: DownloadId, operation: Option<ActiveOperation> },
    ItemUpdated { id: DownloadId, item_update: ItemUpdate }, 
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum ItemUpdate {
    File(FileUpdate),
    Folder(FolderUpdate),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum FileUpdate {
    Status { id: FileId, status: FileStatus },
    Operation { id: FileId, operation: Option<ActiveOperation> },
    Hash { id: FileId, hash: u128 },
    FileSize { id: FileId, len: u64 },
    BytesDownloaded { id: FileId, len: u64 },
}

impl FileUpdate {
    pub fn id(&self) -> FileId {
        match self {
            FileUpdate::Status { id, .. } => *id,
            FileUpdate::Operation { id, .. } => *id,
            FileUpdate::Hash { id, .. } => *id,
            FileUpdate::FileSize { id, .. } => *id,
            FileUpdate::BytesDownloaded { id, .. } => *id,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum FolderUpdate {
    Status { id: FolderId, status: DownloadStatus },
    Operation { id: FolderId, operation: Option<ActiveOperation> },
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
pub enum AppManagerCommand {
    QueueDownload(String),
    DownloadVerified(Download),
    RemoveDownload(DownloadId, bool), // true if we want to remove from disk too
    CleanUpDownload(DownloadId),
    PauseDownload(DownloadId),
    ResumeDownload(DownloadId),
    Shutdown,
    SetGlobalSpeedLimit(Option<u64>),
    SetHostSpeedLimit(String, Option<u64>), // String can be a hostname or url
    SetDownloadSpeedLimit(DownloadId, Option<u64>),
    SetFileSpeedLimit(DownloadId, FileId, Option<u64>),
}

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

#[derive(Debug)]
pub struct AppManager {
    db_manager: StateManager,
    ui_handle: UiManagerHandle,
    sender: mpsc::Sender<AppManagerCommand>,
    receiver: mpsc::Receiver<AppManagerCommand>,
}

impl AppManager {
    pub fn new(db_manager: StateManager, sender: mpsc::Sender<AppManagerCommand>, receiver: mpsc::Receiver<AppManagerCommand>, ui_handle: UiManagerHandle) -> Self {
        AppManager {
            db_manager,
            ui_handle,
            sender,
            receiver
        }
    }

    pub async fn run(mut self) {
        // Load previous state
        let restored_downloads = self.db_manager.load_downloads().await.unwrap();

        debug!(count = ?restored_downloads.len(), "Restored download from disk");
        trace!("Detailed download restore data:\n{:#?}", restored_downloads);
    
        // Clone shared resources
        let ui_event_sender = self.ui_handle.get_event_sender();
        let db_manager = self.db_manager.clone();

        let plugin_registry = PluginRegistryHandler::spawn().await;
        
        let network_config = NetworkConfig::default();
        let client = network_manager::build_global_client(&network_config);
        
        let app_context = AppContext {
            client,
            network_config,
            app_manager: self.sender.clone(),
            ui_sender: ui_event_sender.clone(),
            db_manager: db_manager.clone(),
            plugin_registry,
            writer_handle: DownloadWriterManager::new(),
        };

        let (network_manager, _) = NetworkHandle::spawn(app_context.clone()).await;

        let mut app_settings = AppSettings::new();

        // Download registry for deduplication purposes
        let mut download_registry = DownloadRegistry::from_db(&db_manager).await;
        download_registry.add_downloads(&restored_downloads);

        let verifier = VerifierHandle::spawn(self.sender.clone(), ui_event_sender.clone(), db_manager.clone());

        let mut verification_tracker = VerificationTracker::from_downloads(verifier, restored_downloads).await;

        loop {
            tokio::select! {
                Some(command) = self.receiver.recv() => {
                    match command {
                        AppManagerCommand::QueueDownload(url) => {
                            debug!("registry: {:#?}", download_registry.url_map());
                            debug!("url: {}", url);
                            if download_registry.contains_url(&url) {
                                debug!("Download already exists: {}", url);
                                continue; 
                            }

                            let id = download_registry.register(url.clone());
                            network_manager.queue_download(url, id);
                        },
                        AppManagerCommand::RemoveDownload(id, from_disk) => {
                            info!("Removing download");
                            // First, we set it as removed
                            download_registry.mark_removed(id, from_disk);

                            // Cancel the verification if there is any
                            if verification_tracker.is_verifying(&id) {
                                let _ = verification_tracker.cancel(id).await;
                            }
                            // If it is running. Send Cancel signal.
                            else if let Some(url) = download_registry.lookup_url(&id) {
                                // In this case, we have to wait for the download to finish so it sends the clean up command
                                network_manager.cancel_download(url.clone(), DownloadId(*id));
                            }
                            // Else if it's already done or doesn't exist; just clean up
                            else {
                                debug!("Removed completed download {}", id);
                                let _ = self.sender.send(AppManagerCommand::CleanUpDownload(id)).await;
                            } 
                        },
                        AppManagerCommand::CleanUpDownload(download_id) => {
                            verification_tracker.remove(&download_id);
                            
                            // Finally, we clean it up from the set
                            // Remove from registry now that we know the download is 100% removed
                            if let Some(from_disk) = download_registry.finalize_removed(&download_id) {
                                if from_disk {
                                    match db_manager.load_download(download_id).await {
                                        Ok(Some(download)) => {
                                            for file in download.files().values() {
                                                let path = file.relative_path(); 
                                                if path.exists() {
                                                    force_delete_file(&path); 
                                                }
                                            }
                                        }
                                        Ok(None) => {
                                            warn!("Tried to load download with id {} but it was not found in DB. Skipping file deletion.", download_id);
                                        }
                                        Err(_) => {
                                            warn!("There was an error loading download {} from DB to delete physical files. Skipping file deletion.", download_id);
                                        }
                                    } 
                                }

                                db_manager.delete_download(download_id).await.unwrap();
                                let _ = self.ui_handle.remove_download(download_id);
                            }

                            info!("Download cleaned up");
                        },
                        AppManagerCommand::PauseDownload(download_id) => {
                            // Tell the Verifier to cancel if it's currently hashing
                            verification_tracker.pause(download_id).await;

                            // Otherwise the download is currently being managed by a host
                            if let Some(url) = download_registry.lookup_url(&download_id) {
                                network_manager.pause_download(url.to_string(), download_id);
                            }
                        },
                        AppManagerCommand::ResumeDownload(download_id) => if let Ok(Some(download)) = db_manager.load_download(download_id).await {
                            if verification_tracker.needs_verification(&download_id) {
                                verification_tracker.verify(download).await;
                            } else {
                                let download_settings = app_settings.get_download_settings(download_id);

                                network_manager.resume_download(download, download_settings);
                            }
                        },
                        AppManagerCommand::Shutdown => {
                            break;
                        },
                        AppManagerCommand::SetGlobalSpeedLimit(limit) => {
                            app_settings.set_global_speed_limit(limit);

                            app_context.db_manager.write_app_settings(&app_settings).await.unwrap();

                            network_manager.set_global_limit(limit);
                        },
                        AppManagerCommand::SetHostSpeedLimit(host, limit) => {
                            app_settings.host_settings
                                .entry(host.clone())
                                .or_default()
                                .speed_limit = limit;

                            app_context.db_manager.write_app_settings(&app_settings).await.unwrap();

                            network_manager.set_host_limit(host, limit);
                        },
                        AppManagerCommand::SetDownloadSpeedLimit(download_id, limit) => {
                            if let Some(download_settings) = app_settings.download_settings.get_mut(&download_id) {
                                download_settings.speed_limit = limit;
                            }

                            app_context.db_manager.write_app_settings(&app_settings).await.unwrap();

                            network_manager.set_download_limit(download_id, limit);
                        },
                        AppManagerCommand::SetFileSpeedLimit(download_id, file_id, limit) => {
                            if app_context.db_manager.file_exists(download_id, file_id).await {
                                    let download_settings = app_settings.download_settings.entry(download_id).or_default();
                                    let file_settings = download_settings.file_settings.entry(file_id).or_default();
                                    file_settings.speed_limit = limit;

                                    app_context.db_manager.write_app_settings(&app_settings).await.unwrap();
                                    network_manager.set_file_limit(download_id, file_id, limit);
                            } else {
                                warn!("Tried to set the file speed limit for a non-existent file. Download id: {}, file id: {}", download_id, file_id);
                            }
                        },
                        AppManagerCommand::DownloadVerified(download) => {
                            let download_id = download.id();

                            if !verification_tracker.is_verifying(&download_id) || download_registry.is_marked_for_removal(&download_id) {
                                debug!("Ignoring stale verification completion for {}", download_id);
                                continue;
                            }
                            
                            verification_tracker.remove(&download_id); 

                            let download_settings = app_settings.get_download_settings(download_id);
                            network_manager.resume_download(download, download_settings);
                        },
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct AppManagerHandle {
    sender: mpsc::Sender<AppManagerCommand>,
    ui_handle: UiManagerHandle,
    db_manager: StateManager,
}

impl AppManagerHandle {
    pub fn new(state_manager: StateManager) -> Self {
        let (sender, receiver) = mpsc::channel(1000);

        let ui_handle = UiManagerHandle::new();
        
        let app_manager = AppManager::new(state_manager.clone(), sender.clone(), receiver, ui_handle.clone());

        tokio::spawn(async move {
            app_manager.run().await;
        });
        
        Self {
            sender,
            ui_handle,
            db_manager: state_manager,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<FrontendMessage> {
        self.ui_handle.subscribe()
    }

    pub async fn get_snapshot(&self) -> IndexMap<DownloadId, Download> {
        get_snapshot(&self.db_manager).await
    }

    pub async fn queue_download(&self, url: String) -> Result<(), mpsc::error::SendError<AppManagerCommand>> {
        self.sender.send(AppManagerCommand::QueueDownload(url)).await
    }

    pub async fn remove_download(&self, id: DownloadId, from_disk: bool) -> Result<(), mpsc::error::SendError<AppManagerCommand>> {
        self.sender.send(AppManagerCommand::RemoveDownload(id, from_disk)).await
    }
 
    pub async fn pause_download(&self, download_id: DownloadId) -> Result<(), mpsc::error::SendError<AppManagerCommand>> {
        self.sender.send(AppManagerCommand::PauseDownload(download_id)).await
    }

    pub async fn resume_download(&self, download_id: DownloadId) -> Result<(), mpsc::error::SendError<AppManagerCommand>> {
        self.sender.send(AppManagerCommand::ResumeDownload(download_id)).await
    }

    pub async fn set_global_limit(&self, limit: Option<u64>) -> Result<(), mpsc::error::SendError<AppManagerCommand>> {
        self.sender.send(AppManagerCommand::SetGlobalSpeedLimit(limit)).await
    }

    pub async fn set_host_limit(&self, host: String, limit: Option<u64>) -> Result<(), mpsc::error::SendError<AppManagerCommand>> {
        self.sender.send(AppManagerCommand::SetHostSpeedLimit(host, limit)).await
    }

    pub async fn set_download_limit(&self, download_id: DownloadId, limit: Option<u64>) -> Result<(), mpsc::error::SendError<AppManagerCommand>> {
        self.sender.send(AppManagerCommand::SetDownloadSpeedLimit(download_id, limit)).await
    }

    pub async fn set_file_limit(&self, download_id: DownloadId, file_id: FileId, limit: Option<u64>) -> Result<(), mpsc::error::SendError<AppManagerCommand>> {
        self.sender.send(AppManagerCommand::SetFileSpeedLimit(download_id, file_id, limit)).await
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, IntoStaticStr, EnumString, Default)]
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
    #[default]
    Unknown,
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
