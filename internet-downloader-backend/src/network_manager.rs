use std::{collections::HashMap, sync::Arc, time::Duration};

use reqwest::Client;
use tokio::{sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel}, task::JoinHandle};
use tracing::{debug, warn};
use url::{Host, Url};

use crate::{context::AppContext, download::{DownloadLimiterGroup, DownloadSettings, LimiterRegistry, ManagerCommand, items::{Download, DownloadId}}, host_manager::HostHandle, utils::network_utils::BandwidthLimiter};

pub enum NetworkMessage {
    QueueDownload(String, DownloadId),
    CancelDownload(String, DownloadId),
    PauseDownload(String, DownloadId),
    ResumeDownload(Download, Option<DownloadSettings>),
    SetGlobalSpeedLimit(Option<u64>),
    SetHostSpeedLimit(String, Option<u64>), // String can be a hostname or url
    SetDownloadSpeedLimit(DownloadId, Option<u64>),
    SetFileSpeedLimit(DownloadId, usize, Option<u64>),
}

/// Handles network related concerns such as connections, downloads, and rate limiting.
struct NetworkManager {
    sender: UnboundedSender<NetworkMessage>,
    receiver: UnboundedReceiver<NetworkMessage>,
    host_handle_map: HashMap<Host, HostHandle>,
    app_context: AppContext,
    limiters: Arc<LimiterRegistry>,
}

impl NetworkManager {
    pub fn new(sender: UnboundedSender<NetworkMessage>, receiver: UnboundedReceiver<NetworkMessage>, app_context: AppContext) -> Self { 
        let limiters = Arc::new(LimiterRegistry::new());

        Self {
            sender,
            receiver,
            host_handle_map: HashMap::new(),
            app_context,
            limiters,
        }
    }

    pub async fn run(mut self) {
        while let Some(message) = self.receiver.recv().await {
            match message {
                NetworkMessage::QueueDownload(url, id) => {
                    debug!("queueing download in network manager");

                    let host = Self::parse_host(&url);
                    
                    let download_limiter = Arc::new(DownloadLimiterGroup::new());
                    self.limiters.downloads().insert(id, Arc::downgrade(&download_limiter));
                    
                    debug!("sending to host manager");
                    let host_handle = self.get_or_spawn_host(host);
                    host_handle.process_download(url, id, download_limiter);
                },
                NetworkMessage::CancelDownload(url, download_id) => {
                    let host = NetworkManager::parse_host(&url);

                    if let Some(host_handle) = self.host_handle_map.get(&host) {
                        host_handle.cancel_download(download_id);
                    } else {
                        // The host manager for this download doesn't exist, so the download doesn't exist
                        let _ = self.app_context.download_manager.send(ManagerCommand::CleanUpDownload(download_id));
                    }
                },
                NetworkMessage::PauseDownload(url, download_id) => {
                    let host = NetworkManager::parse_host(&url);

                    if let Some(host_handle) = self.host_handle_map.get(&host) {
                        host_handle.pause_download(download_id);
                    } else {
                        warn!("Tried to pause a download with a host not tracked by the manager. url: {} id: {}", url, download_id);
                    }
                },
                NetworkMessage::ResumeDownload(download, download_settings) => {
                    debug!("Resuming download in network manager");

                    let url = download.url().clone();
                    let host = Self::parse_host(&url);

                    let download_limiter = Arc::new(DownloadLimiterGroup::from_settings(download_settings.as_ref()));
                    
                    self.limiters.downloads().insert(download.id(), Arc::downgrade(&download_limiter));
                    
                    let host_handle = self.get_or_spawn_host(host);
                    host_handle.queue_download(download, download_limiter);
                },
                NetworkMessage::SetGlobalSpeedLimit(limit) => {
                    if let Some(limit) = limit {
                        self.limiters.global_limit().set_unlimited(false);
                        self.limiters.global_limit().set_limit(limit);
                    } else {
                        self.limiters.global_limit().set_unlimited(true);
                    }
                },
                NetworkMessage::SetHostSpeedLimit(host, limit) => {
                    let host = Self::parse_host(&host);

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
                NetworkMessage::SetDownloadSpeedLimit(download_id, limit) => {
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
                NetworkMessage::SetFileSpeedLimit(download_id, file_id, limit) => {
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
                },
            }
        }   
    }

    pub fn parse_host(url: &str) -> Host {
        let url = Url::parse(url).expect("Invalid URL");
        url.host().unwrap_or(Host::Domain("unknown")).to_owned()
    }

    fn get_or_spawn_host(&mut self, host: Host) -> &HostHandle {
        self.host_handle_map.entry(host.clone()).or_insert_with(|| {
            debug!("Spawning new HostManager for: {}", host);

            let limiter = BandwidthLimiter::new(0);
            limiter.set_unlimited(true);

            let host_limiter = Arc::new(limiter);

            // 2. Insert the Weak pointer into the Registry for the UI to see
            self.limiters.host_limits().insert(host.clone(), Arc::downgrade(&host_limiter));

            // 3. Spawn the Host Manager, passing the strong Arcs down!
            let (handle, _) = HostHandle::spawn(
                self.app_context.clone(),
                host,
                self.limiters.global_limit().clone(), // Pass Global
                host_limiter,                       // Pass Host
            );

            handle
        })
    }
}

#[derive(Clone, Debug)]
pub struct NetworkHandle {
    sender: UnboundedSender<NetworkMessage>,
}

impl NetworkHandle {
    pub async fn spawn(app_context: AppContext) -> (Self, JoinHandle<()>) {
        let (network_sender, network_receiver) = unbounded_channel();

        let network_manager = NetworkManager::new(network_sender.clone(), network_receiver, app_context);

        let join_handle = tokio::spawn(async move {
            network_manager.run().await;
        });

        let network_handler = Self { sender: network_sender };
        
        (network_handler, join_handle)
    }

    pub fn queue_download(&self, url: String, id: DownloadId) {
        let _ = self.sender.send(NetworkMessage::QueueDownload(url, id));
    }

    pub fn cancel_download(&self, url: String, id: DownloadId) {
        let _ = self.sender.send(NetworkMessage::CancelDownload(url, id));
    }

    pub fn pause_download(&self, url: String, id: DownloadId) {
        let _ = self.sender.send(NetworkMessage::PauseDownload(url, id));
    }

    pub fn resume_download(&self, download: Download, download_settings: Option<DownloadSettings>) {
        let _ = self.sender.send(NetworkMessage::ResumeDownload(download, download_settings));
    }

    pub fn set_global_limit(&self, limit: Option<u64>) {
        let _ = self.sender.send(NetworkMessage::SetGlobalSpeedLimit(limit));
    }

    pub fn set_host_limit(&self, host: String, limit: Option<u64>) {
        let _ = self.sender.send(NetworkMessage::SetHostSpeedLimit(host, limit));
    }

    pub fn set_download_limit(&self, download_id: DownloadId, limit: Option<u64>) {
        let _ = self.sender.send(NetworkMessage::SetDownloadSpeedLimit(download_id, limit));
    }

    pub fn set_file_limit(&self, download_id: DownloadId, file_id: usize, limit: Option<u64>) {
        let _ = self.sender.send(NetworkMessage::SetFileSpeedLimit(download_id, file_id, limit));
    }
}

#[derive(Clone, Debug)]
pub struct NetworkConfig {
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(10),
        }
    }
}

pub fn build_global_client(config: &NetworkConfig) -> Client {
    reqwest::Client::builder()
        .connect_timeout(config.connect_timeout)
        .read_timeout(config.read_timeout)
        .no_gzip()
        .no_brotli()
        .no_deflate()
        .build()
        .unwrap()
}
