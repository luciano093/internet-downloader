use std::collections::HashMap;

use reqwest::Client;
use tokio::{sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel}, task::JoinHandle};

use crate::{download::{Download, hosts::{Host, parse_host}}, host_manager::HostHandle};

pub enum NetworkMessage {
    QueueDownload(String, usize),
    HandleDownload(Download)
}

/// Handles network related concerns such as connections, downloads, and rate limiting.
struct NetworkManager {
    sender: UnboundedSender<NetworkMessage>,
    receiver: UnboundedReceiver<NetworkMessage>,
    host_handle_map: HashMap<Host, HostHandle>,
    client: Client,
}

impl NetworkManager {
    pub fn new(sender: UnboundedSender<NetworkMessage>, receiver: UnboundedReceiver<NetworkMessage>) -> Self {
        Self {
            sender,
            receiver,
            host_handle_map: HashMap::new(),
            client: Client::new(),
        }
    }

    pub async fn run(mut self) {
        while let Some(message) = self.receiver.recv().await {
            match message {
                NetworkMessage::QueueDownload(url, id) => {
                    self.process_download(url, id);
                },
                NetworkMessage::HandleDownload(download) => {
                    let host_handle = if self.host_handle_map.contains_key(&download.host()) {
                        self.host_handle_map.get(&download.host()).unwrap()
                    } else {
                        self.host_handle_map.insert(download.host(), HostHandle::spawn(self.client.clone(), download.host()).0);
                        self.host_handle_map.get(&download.host()).unwrap()
                    };
                    
                    host_handle.queue_download(download);
                },
            }
        }   
    }

    pub fn process_download(&self, url: String, id: usize) -> JoinHandle<()> {
        let sender = self.sender.clone();

        tokio::spawn(async move {
            let host = parse_host(&url).unwrap();
            let download_task = host.extract_download_info(&url).await;
            let download = Download::new(id, download_task);

            let _ = sender.send(NetworkMessage::HandleDownload(download));
        })
    }
}

#[derive(Clone, Debug)]
pub struct NetworkHandle {
    sender: UnboundedSender<NetworkMessage>,
}

impl NetworkHandle {
    pub fn spawn() -> (Self, JoinHandle<()>) {
        let (network_sender, network_receiver) = unbounded_channel();

        let network_manager = NetworkManager::new(network_sender.clone(), network_receiver);

        let join_handle = tokio::spawn(async move {
            network_manager.run().await;
        });

        let network_handler = Self { sender: network_sender };
        
        (network_handler, join_handle)
    }

    pub fn queue_download(&self, url: String, id: usize) {
        let _ = self.sender.send(NetworkMessage::QueueDownload(url, id));
    }
}