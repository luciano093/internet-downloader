use std::{collections::HashMap, time::Duration};

use reqwest::Client;
use tokio::{sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel}, task::JoinHandle};
use url::{Host, Url};

use crate::{client_state_manager::UiStateEvent, download::DownloadId, host_manager::HostHandle, plugin_registry::PluginRegistryHandler, state_manager::StateManager};

pub enum NetworkMessage {
    QueueDownload(String, DownloadId),
    // HandleDownload(Download)
}

/// Handles network related concerns such as connections, downloads, and rate limiting.
struct NetworkManager {
    sender: UnboundedSender<NetworkMessage>,
    receiver: UnboundedReceiver<NetworkMessage>,
    host_handle_map: HashMap<Host, HostHandle>,
    client: Client,
    ui_sender: UnboundedSender<UiStateEvent>,
    db_manager: StateManager,
    plugin_registry: PluginRegistryHandler,
}

impl NetworkManager {
    pub fn new(sender: UnboundedSender<NetworkMessage>, receiver: UnboundedReceiver<NetworkMessage>, ui_sender: UnboundedSender<UiStateEvent>, db_manager: StateManager, plugin_registry: PluginRegistryHandler) -> Self {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5)) // fails to connect in 5 seconds
            .read_timeout(Duration::from_secs(10)) // no data for 10 seconds
            .no_gzip()     // prevents stripping Content-Length
            .no_brotli()   // prevents stripping Content-Length
            .no_deflate()
            .build()
            .unwrap();

        
        Self {
            sender,
            receiver,
            host_handle_map: HashMap::new(),
            client,
            ui_sender,
            db_manager,
            plugin_registry,
        }
    }

    pub async fn run(mut self) {
        while let Some(message) = self.receiver.recv().await {
            match message {
                NetworkMessage::QueueDownload(url, id) => {
                    println!("queueing download in network manager");

                    let url2 = Url::parse(&url).expect("Invalid URL");
                    let host = url2.host().unwrap_or(Host::Domain("unknown")).to_owned();

                    let host_handle = if self.host_handle_map.contains_key(&host) {
                        self.host_handle_map.get(&host).unwrap()
                    } else {
                        self.host_handle_map.insert(host.clone(), HostHandle::spawn(self.client.clone(), host.clone(), self.ui_sender.clone(), self.db_manager.clone(), self.plugin_registry.clone()).0);
                        self.host_handle_map.get(&host).unwrap()
                    };
                    
                    println!("sending to host manager");
                    host_handle.process_download(url, id);
                },
            }
        }   
    }
}

#[derive(Clone, Debug)]
pub struct NetworkHandle {
    sender: UnboundedSender<NetworkMessage>,
}

impl NetworkHandle {
    pub async fn spawn(ui_sender: UnboundedSender<UiStateEvent>, db_manager: StateManager) -> (Self, JoinHandle<()>) {
        let (network_sender, network_receiver) = unbounded_channel();

        let plugin_registry = PluginRegistryHandler::spawn().await;

        let network_manager = NetworkManager::new(network_sender.clone(), network_receiver, ui_sender, db_manager, plugin_registry);

        let join_handle = tokio::spawn(async move {
            network_manager.run().await;
        });

        let network_handler = Self { sender: network_sender };
        
        (network_handler, join_handle)
    }

    pub fn queue_download(&self, url: String, id: DownloadId) {
        let _ = self.sender.send(NetworkMessage::QueueDownload(url, id));
    }
}