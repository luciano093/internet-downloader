use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace, warn};
use url::{Host, Url};

use crate::app::limiters::{DownloadLimiterGroup, LimiterRegistry};
use crate::app::registry::DownloadRegistry;
use crate::app::settings::AppSettings;
use crate::app::snapshot::AppSnapshotHandler;
use crate::client_state_manager::{DownloadSnapshot, FrontendMessage, UiManagerHandle};
use crate::app::context::AppContext;
use crate::download::hosts::{DownloadTask, parse_host_or_url};
use crate::download::items::{Download, DownloadId, DownloadItem, FileId};
use crate::download::supervisor::DownloadHandle;
use crate::download::verifier::VerifierHandle;
use crate::download::writer::DownloadWriterManager;
use crate::network_manager::{NetworkConfig, NetworkHandle, build_global_client};
use crate::plugin_registry::PluginRegistryHandler;
use crate::utils::file_utils::force_delete_file;
use crate::db::state_manager::StateManager;
use crate::utils::network_utils::BandwidthLimiter;

pub enum AppManagerEvent {
    FinishDownload(DownloadId),
    RemoveDownload(DownloadId),
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
    DownloadReady(Download),
    RemoveDownload(DownloadId, bool), // true if we want to remove from disk too
    PauseDownload(DownloadId),
    ResumeDownload(DownloadId),
    Shutdown,
    SetGlobalSpeedLimit(Option<u64>),
    SetDefaultSavePath(Option<String>),
    SetHostSpeedLimit(String, Option<u64>), // String can be a hostname or url
    SetDownloadSpeedLimit(DownloadId, Option<u64>),
    SetFileSpeedLimit(DownloadId, FileId, Option<u64>),
    GetSettings(oneshot::Sender<AppSettings>),
    RemoveHostSpeedLimit(String), // String can be a hostname or url
}

pub struct AppManager {
    db_manager: StateManager,
    ui_handle: UiManagerHandle,
    snapshot_manager: AppSnapshotHandler,
    sender: mpsc::Sender<AppManagerCommand>,
    receiver: mpsc::Receiver<AppManagerCommand>,
    supervisors: HashMap<DownloadId, DownloadHandle>,
    limiters: Arc<LimiterRegistry>,
}

impl AppManager {
    pub fn new(
        db_manager: StateManager,
        sender: mpsc::Sender<AppManagerCommand>,
        receiver: mpsc::Receiver<AppManagerCommand>,
        ui_handle: UiManagerHandle,
        snapshot_manager: AppSnapshotHandler,
    ) -> Self {
        AppManager {
            db_manager,
            ui_handle,
            snapshot_manager,
            sender,
            receiver,
            supervisors: HashMap::new(),
            limiters: Arc::new(LimiterRegistry::new()),
        }
    }

    pub async fn run(mut self) {
        // Load previous state
        let restored_downloads = self.db_manager.load_all_downloads().await.unwrap();

        debug!(count = ?restored_downloads.len(), "Restored download from disk");
        trace!("Detailed download restore data:\n{:#?}", restored_downloads);

        let plugin_registry = PluginRegistryHandler::spawn().await;
        
        let network_config = NetworkConfig::default();
        let client = build_global_client(&network_config);

        let mut app_settings = self.db_manager
            .load_app_settings()
            .await
            .unwrap()
            .unwrap_or_else(|| AppSettings::new());

        match app_settings.global_speed_limit() {
            Some(limit) => {
                self.limiters.global_limit().set_unlimited(false);
                self.limiters.global_limit().set_limit(limit);
            },
            None => self.limiters.global_limit().set_unlimited(true),
        }

        let writer = DownloadWriterManager::spawn();
        let network_handle = NetworkHandle::spawn(
            client.clone(), 
            app_settings.clone(), 
            writer.clone(), 
            self.ui_handle.clone(), 
            self.sender.clone(), 
            self.limiters.clone()
        );
        
        let app_context = AppContext {
            client,
            network_config,
            app_manager: self.sender.clone(),
            ui_handle: self.ui_handle.clone(),
            db_manager: self.db_manager.clone(),
            plugin_registry,
            writer_handle: writer,
            network_handle,
        };

        // Download registry for deduplication purposes
        let mut download_registry = DownloadRegistry::from_db(&self.db_manager).await;
        download_registry.add_downloads(&restored_downloads);

        for (_, download) in restored_downloads {
            let _ = self.sender.send(AppManagerCommand::DownloadReady(download)).await;
        }

        let (event_sender, mut event_receiver) = mpsc::unbounded_channel();
        let verifier = VerifierHandle::spawn(self.ui_handle.clone(), self.db_manager.clone());

        loop {
            tokio::select! {
                Some(event) = event_receiver.recv() => {
                    match event {
                        AppManagerEvent::FinishDownload(download_id) => {
                            self.supervisors.remove(&download_id);
                        }
                        AppManagerEvent::RemoveDownload(download_id) => {
                            self.supervisors.remove(&download_id);
                            download_registry.finalize_removed(&download_id);
                            info!("Download removed {}", download_id);
                        }
                    }
                }
                Some(command) = self.receiver.recv() => {
                    match command {
                        AppManagerCommand::QueueDownload(url) => {
                            debug!("registry: {:#?}", download_registry.url_map());
                            debug!("url: {}", url);
                            if download_registry.contains_url(&url) {
                                info!("Skipping already existing download: {}", url);
                                continue; 
                            }
    
                            let download_id = download_registry.register(url.clone());
                            let sender = self.sender.clone();
    
                            send_to_plugin(app_context.clone(), url, async move |download_task| {
                                let download = Download::new(*download_id, download_task);
                                
                                let _ = sender.send(AppManagerCommand::DownloadReady(download)).await;
                            });
                        },
                        AppManagerCommand::DownloadReady(download) => {
                            let download_id = download.id();
    
                            let download_settings = app_settings.get_download_settings(download_id);
                            let download_limiter = Arc::new(DownloadLimiterGroup::from_settings(download_settings));
            
                            for (&file_id, _file) in download.files() {
                                let limiter = BandwidthLimiter::new(0);
                                limiter.set_unlimited(true);
            
                                let file_limiter = Arc::new(limiter);
                                download_limiter.file_limiters().insert(file_id, file_limiter);
                            }
    
                            self.limiters.downloads().insert(download_id, Arc::downgrade(&download_limiter));

                            let snapshot_signal_receiver = self.snapshot_manager.subscribe();
                            let (snapshot_sender, snapshot_receiver) = mpsc::unbounded_channel();
                            
                            self.snapshot_manager.add_supervisor(snapshot_receiver);
    
                            let supervisor = DownloadHandle::spawn(
                                download, 
                                download_limiter, 
                                app_context.clone(), 
                                verifier.clone(), 
                                event_sender.clone(),
                                snapshot_signal_receiver,
                                snapshot_sender
                            );
    
                            self.supervisors.insert(download_id, supervisor);
                        }
                        AppManagerCommand::RemoveDownload(download_id, from_disk) => {
                            info!("Removing download {}", download_id);
                            download_registry.mark_removed(download_id, from_disk);
    
                            // If it is running. Send Cancel signal.
                            if let Some(supervisor) = self.supervisors.remove(&download_id) {
                                supervisor.cancel(from_disk).await;
                            } 
                            // Else if it's already done or doesn't exist; just clean up
                            else {
                                if from_disk {
                                    match self.db_manager.load_download(download_id).await {
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
    
                                download_registry.finalize_removed(&download_id);
                                let _ = self.db_manager.delete_download(download_id).await;
                                app_context.ui_handle.remove_download(download_id);
                                
                                info!("Download removed {}", download_id);
                            }
                        },
                        AppManagerCommand::PauseDownload(download_id) => {
                            // A download can only be paused if it's running
                            if let Some(supervisor) = self.supervisors.get(&download_id) {
                                supervisor.pause().await;
                            } else {
                                warn!("Attempted to pause a non-existent download: {}", download_id);
                            }
                        },
                        AppManagerCommand::ResumeDownload(download_id) => {
                            // A download can only be resumed if it's in memory
                            if let Some(supervisor) = self.supervisors.get(&download_id) {
                                supervisor.resume().await;
                            } else {
                                warn!("Attempted to resume a non-existent download: {}", download_id);
                            }
                        },
                        AppManagerCommand::Shutdown => {
                            break;
                        },
                        AppManagerCommand::SetGlobalSpeedLimit(limit) => {
                            app_settings.set_global_speed_limit(limit);
    
                            self.db_manager.write_app_settings(&app_settings).await.unwrap();
    
                            if let Some(limit) = limit {
                                self.limiters.global_limit().set_unlimited(false);
                                self.limiters.global_limit().set_limit(limit);
                            } else {
                                self.limiters.global_limit().set_unlimited(true);
                            }
                        },
                        AppManagerCommand::SetHostSpeedLimit(host_str, limit) => {
                            app_settings.host_settings
                                .entry(host_str.clone())
                                .or_default()
                                .speed_limit = limit;
    
                            self.db_manager.write_app_settings(&app_settings).await.unwrap();

                            // host string can either be a host or a url
                            let host = match parse_host_or_url(&host_str) {
                                Ok(host) => host,
                                Err(err) => {
                                    warn!("{}", err);
                                    continue;
                                },
                            };
        
                            if let Some(weak_limiter) = self.limiters.host_limits().get(&host) {
                                if let Some(live_limiter) = weak_limiter.upgrade() {
                                    if let Some(limit) = limit {
                                        live_limiter.set_unlimited(false);
                                        live_limiter.set_limit(limit);
                                    } else {
                                        live_limiter.set_unlimited(true);
                                    }
                                }
                            }
                        },
                        AppManagerCommand::SetDownloadSpeedLimit(download_id, limit) => {
                            if let Some(download_settings) = app_settings.download_settings.get_mut(&download_id) {
                                download_settings.speed_limit = limit;
                            }
    
                            self.db_manager.write_app_settings(&app_settings).await.unwrap();
    
                            if let Some(weak_group) = self.limiters.downloads().get(&download_id) {
                                if let Some(live_group) = weak_group.upgrade() {
                                    let download_limiter = &live_group.download_limiter();
                                    
                                    if let Some(limit) = limit {
                                        download_limiter.set_unlimited(false);
                                        download_limiter.set_limit(limit);
                                    } else {
                                        download_limiter.set_unlimited(true);
                                    }
                                }
                            }
                        },
                        AppManagerCommand::SetFileSpeedLimit(download_id, file_id, limit) => {
                            if self.db_manager.file_exists(download_id, file_id).await {
                                    let download_settings = app_settings.download_settings.entry(download_id).or_default();
                                    let file_settings = download_settings.file_settings.entry(file_id).or_default();
                                    file_settings.speed_limit = limit;
    
                                    app_context.db_manager.write_app_settings(&app_settings).await.unwrap();
                                    
                                    if let Some(weak_group) = self.limiters.downloads().get(&download_id) {
                                        if let Some(live_group) = weak_group.upgrade() {
                                            if let Some(file_limiter) = live_group.file_limiters().get(&file_id) {
                                                if let Some(limit) = limit {
                                                    file_limiter.set_unlimited(false);
                                                    file_limiter.set_limit(limit);
                                                } else {
                                                    file_limiter.set_unlimited(true);
                                                }
                                            }
                                        }
                                    }
                            } else {
                                warn!("Tried to set the file speed limit for a non-existent file. Download id: {}, file id: {}", download_id, file_id);
                            }
                        },
                        AppManagerCommand::GetSettings(sender) => {
                            let _ = sender.send(app_settings.clone());
                        },
                        AppManagerCommand::SetDefaultSavePath(default_save_path) => {
                            app_settings.set_default_save_path(default_save_path);
    
                            self.db_manager.write_app_settings(&app_settings).await.unwrap();
                        },
                        AppManagerCommand::RemoveHostSpeedLimit(host_str) => {
                            if let None = app_settings.host_settings.remove(&host_str) {
                                warn!("Host string {} didn't have a saved speed limit", host_str);
                            }
                            self.db_manager.write_app_settings(&app_settings).await.unwrap();

                            let host = match parse_host_or_url(&host_str) {
                                Ok(host) => host,
                                Err(err) => {
                                    warn!("{}", err);
                                    continue;
                                },
                            };
                            
                            if let Some(weak_limiter) = self.limiters.host_limits().get(&host) {
                                if let Some(live_limiter) = weak_limiter.upgrade() {
                                    live_limiter.set_unlimited(true);
                                }
                            }
                        },
                    }
                
                }
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GetSettingsError {
    #[error("app manager is unreachable")]
    ManagerUnreachable,
    #[error("app manager did not respond")]
    NoResponse,
}

#[derive(Debug, Clone)]
pub struct AppManagerHandle {
    sender: mpsc::Sender<AppManagerCommand>,
    ui_handle: UiManagerHandle,
    snapshot_manager: AppSnapshotHandler,
}

impl AppManagerHandle {
    pub fn spawn(state_manager: StateManager, snapshot_manager: AppSnapshotHandler) -> Self {
        let (sender, receiver) = mpsc::channel(1000);

        let ui_handle = UiManagerHandle::spawn();
        
        let app_manager = AppManager::new(state_manager.clone(), sender.clone(), receiver, ui_handle.clone(), snapshot_manager.clone());

        tokio::spawn(async move {
            app_manager.run().await;
        });
        
        Self {
            sender,
            ui_handle,
            snapshot_manager,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<FrontendMessage> {
        self.ui_handle.subscribe()
    }

    pub async fn get_snapshot(&self) -> HashMap<DownloadId, DownloadSnapshot> {
        self.snapshot_manager.take_snapshot().await
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
    
    pub async fn set_default_save_path(&self, default_save_path: Option<String>) -> Result<(), mpsc::error::SendError<AppManagerCommand>> {
        self.sender.send(AppManagerCommand::SetDefaultSavePath(default_save_path)).await
    }

    pub async fn set_host_limit(&self, host: String, limit: Option<u64>) -> Result<(), mpsc::error::SendError<AppManagerCommand>> {
        self.sender.send(AppManagerCommand::SetHostSpeedLimit(host, limit)).await
    }

    pub async fn remove_host_limit(&self, host: String) -> Result<(), mpsc::error::SendError<AppManagerCommand>> {
        self.sender.send(AppManagerCommand::RemoveHostSpeedLimit(host)).await
    }

    pub async fn set_download_limit(&self, download_id: DownloadId, limit: Option<u64>) -> Result<(), mpsc::error::SendError<AppManagerCommand>> {
        self.sender.send(AppManagerCommand::SetDownloadSpeedLimit(download_id, limit)).await
    }

    pub async fn set_file_limit(&self, download_id: DownloadId, file_id: FileId, limit: Option<u64>) -> Result<(), mpsc::error::SendError<AppManagerCommand>> {
        self.sender.send(AppManagerCommand::SetFileSpeedLimit(download_id, file_id, limit)).await
    }

    pub async fn get_settings(&self) -> Result<AppSettings, GetSettingsError> {
        let (sender, receiver) = oneshot::channel();
        
        self.sender
            .send(AppManagerCommand::GetSettings(sender))
            .await
            .map_err(|_| GetSettingsError::ManagerUnreachable)?;
    
        receiver.await.map_err(|_| GetSettingsError::NoResponse)
    }
}

fn send_to_plugin<Fut>(
    app_context: AppContext, 
    url: String,
    on_success: impl FnOnce(DownloadTask) -> Fut + Send + 'static,
) -> JoinHandle<()> 
where
    Fut: Future<Output = ()> + Send + 'static,
{
    let plugin_registry = app_context.plugin_registry.clone();

    tokio::spawn(async move {
        let (sender, receiver) = oneshot::channel();
        let cancel_token = CancellationToken::new();

        plugin_registry.parse(url.clone(), sender, cancel_token);

        if let Ok(message) = receiver.await {
            if let Some(download_task) = message {
                on_success(download_task).await;
            } else {
                warn!("No plugin found for url: {}", url);
            }
        } else {
            warn!("Failed to send url: {} for plugin parsing", url);
        };
    })
}
