use std::{collections::HashMap, time::Duration};

use reqwest::Client;
use tokio::{sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel}, task::JoinHandle};
use tracing::debug;
use url::{Host, Url};

use crate::{context::AppContext, download::{DownloadId, ManagerCommand}, host_manager::HostHandle};

pub enum NetworkMessage {
    QueueDownload(String, DownloadId),
    CancelDownload(String, DownloadId),
    // HandleDownload(Download)
}

/// Handles network related concerns such as connections, downloads, and rate limiting.
struct NetworkManager {
    sender: UnboundedSender<NetworkMessage>,
    receiver: UnboundedReceiver<NetworkMessage>,
    host_handle_map: HashMap<Host, HostHandle>,
    app_context: AppContext,
}

impl NetworkManager {
    pub fn new(sender: UnboundedSender<NetworkMessage>, receiver: UnboundedReceiver<NetworkMessage>, app_context: AppContext) -> Self { 
        Self {
            sender,
            receiver,
            host_handle_map: HashMap::new(),
            app_context,
        }
    }

    pub async fn run(mut self) {
        while let Some(message) = self.receiver.recv().await {
            match message {
                NetworkMessage::QueueDownload(url, id) => {
                    debug!("queueing download in network manager");

                    let host = Self::parse_host(&url);

                    let host_handle = if self.host_handle_map.contains_key(&host) {
                        self.host_handle_map.get(&host).unwrap()
                    } else {
                        self.host_handle_map.insert(host.clone(), HostHandle::spawn(self.app_context.clone(), host.clone()).0);
                        self.host_handle_map.get(&host).unwrap()
                    };
                    
                    debug!("sending to host manager");
                    host_handle.process_download(url, id);
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
            }
        }   
    }

    pub fn parse_host(url: &str) -> Host {
        let url = Url::parse(url).expect("Invalid URL");
        url.host().unwrap_or(Host::Domain("unknown")).to_owned()
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