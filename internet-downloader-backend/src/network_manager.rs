use std::{collections::HashMap, time::Duration};
use std::sync::Arc;

use reqwest::Client;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, warn};
use url::{Host, Url};

use crate::{app::{limiters::LimiterRegistry, manager::AppManagerCommand, settings::AppSettings}, client_state_manager::UiManagerHandle, download::{hosts::manager::HostHandle, writer::DownloadWriterManager}, utils::network_utils::BandwidthLimiter};

pub enum NetworkMessage {
    GetHostHandles(Vec<Arc<String>>, oneshot::Sender<HashMap<Host, HostHandle>>)
}

pub struct NetworkManager {
    receiver: mpsc::Receiver<NetworkMessage>,
    host_handle_map: HashMap<Host, HostHandle>,
    client: Client,
    app_settings: AppSettings,
    writer: DownloadWriterManager,
    ui_handle: UiManagerHandle,
    app_manager: mpsc::Sender<AppManagerCommand>,
    limiters: Arc<LimiterRegistry>,
}

impl NetworkManager {
    pub fn new(
        receiver: mpsc::Receiver<NetworkMessage>, 
        client: Client, 
        app_settings: AppSettings, 
        writer: DownloadWriterManager,
        ui_handle: UiManagerHandle,
        app_manager: mpsc::Sender<AppManagerCommand>,
        limiters: Arc<LimiterRegistry>
    ) -> Self {
        Self {
            receiver,
            host_handle_map: HashMap::new(),
            client,
            app_settings,
            writer,
            ui_handle,
            app_manager,
            limiters,
        }
    }

    pub async fn run(mut self) {
        while let Some(message) = self.receiver.recv().await {
            match message {
                NetworkMessage::GetHostHandles(urls, reply) => {
                    let mut hosts = HashMap::new();
                    
                    for url in urls {
                        let host = Url::parse(&url)
                            .ok()
                            .and_then(|url| url.host().map(|host| host.to_owned()))
                            .unwrap_or_else(|| Host::Domain(format!("unknown-host ({})", url)));
                        
                        let host_handle = self.get_or_spawn_host(host.clone());

                        hosts.insert(host, host_handle.clone());
                    }

                    let _ = reply.send(hosts);
                },
            }
        }
    }

    fn get_or_spawn_host(&mut self, host: Host) -> &HostHandle {
        self.host_handle_map.entry(host.clone()).or_insert_with(|| {
            debug!("Spawning new HostManager for: {}", host);

            let host_limiter = Arc::new(BandwidthLimiter::new(0));
            
            let host_str = host.to_string();
            if let Some(host_settings) = self.app_settings.host_settings.get(&host_str) {
                if let Some(limit) = host_settings.speed_limit {
                    host_limiter.set_unlimited(false);
                    host_limiter.set_limit(limit);
                }
            } else {
                host_limiter.set_unlimited(true);
            }

            self.limiters.host_limits().insert(host.clone(), Arc::downgrade(&host_limiter));

            HostHandle::spawn(
                host,
                self.client.clone(),
                self.writer.clone(),
                self.ui_handle.clone(),
                self.app_manager.clone(),
                host_limiter,
                self.limiters.global_limit(),
            )
        })
    }
}

#[derive(Clone)]
pub struct NetworkHandle {
    sender: mpsc::Sender<NetworkMessage>,
}

impl NetworkHandle {
    pub fn spawn(
        client: Client, 
        app_settings: AppSettings, 
        writer: DownloadWriterManager,
        ui_handle: UiManagerHandle,
        app_manager: mpsc::Sender<AppManagerCommand>,
        limiters: Arc<LimiterRegistry>
    ) -> Self {
        let (sender, receiver) = mpsc::channel(1000);

        let network_manager = NetworkManager::new(receiver, client, app_settings, writer, ui_handle, app_manager, limiters);

        tokio::spawn(async move {
            network_manager.run().await;
        });

        Self { 
            sender,
        }
    }

    pub async fn get_host_handles(&self, urls: Vec<Arc<String>>, reply: oneshot::Sender<HashMap<Host, HostHandle>>) {
        let _ = self.sender.send(NetworkMessage::GetHostHandles(urls, reply)).await;
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
