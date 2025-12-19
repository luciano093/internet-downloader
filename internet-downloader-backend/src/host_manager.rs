use std::{collections::{HashMap, VecDeque}, sync::Arc};

use reqwest::Client;
use tokio::{sync::{OwnedSemaphorePermit, Semaphore, mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel}, oneshot}, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use url::Host;

use crate::{client_state_manager::UiStateEvent, download::{Download, DownloadId, DownloadStatus}, download_task::DownloadSupervisor, plugin_registry::PluginRegistryHandler, state_manager::StateManager};

pub struct ActiveDownloadPermit {
    permit: Option<OwnedSemaphorePermit>,
    host_sender: UnboundedSender<HostMessage>,
}

impl ActiveDownloadPermit {
    pub fn new(permit: OwnedSemaphorePermit, host_sender: UnboundedSender<HostMessage>) -> Self {
        Self { permit: Some(permit), host_sender }
    }
}

impl Drop for ActiveDownloadPermit {
    fn drop(&mut self) {
        let _ = self.host_sender.send(HostMessage::PermitReleased(self.permit.take().unwrap()));
    }
}

pub enum HostMessage {
    ProcessDownload(String, DownloadId),
    QueueDownload(Download),
    DownloadFinished(DownloadId),
    PermitReleased(OwnedSemaphorePermit),
    SupervisorSaturated(DownloadId),
}

pub struct HostManager {
    client: Client,
    host: Host,
    sender: UnboundedSender<HostMessage>,
    receiver: UnboundedReceiver<HostMessage>,
    active_downloads: HashMap<DownloadId, DownloadSupervisor>, // Maybe change this to a BTreeMap for order
    connections_budget: Arc<Semaphore>,
    permit_queue: VecDeque<DownloadId>,
    ui_sender: UnboundedSender<UiStateEvent>,
    db_manager: StateManager,
    plugin_registry: PluginRegistryHandler,
}

impl HostManager {
    pub fn new(client: Client, host: Host, sender: UnboundedSender<HostMessage>, receiver: UnboundedReceiver<HostMessage>, ui_sender: UnboundedSender<UiStateEvent>, db_manager: StateManager, plugin_registry: PluginRegistryHandler) -> Self {
        Self {
            client,
            host,
            sender,
            receiver,
            active_downloads: HashMap::new(),
            connections_budget: Arc::new(Semaphore::const_new(2)),
            permit_queue: VecDeque::new(),
            ui_sender,
            db_manager,
            plugin_registry,
        }
    }

    pub async fn run(mut self) {
        while let Some(message) = self.receiver.recv().await {
            match message {
                HostMessage::ProcessDownload(url, download_id) => {
                    self.process_download(url, download_id);
                },
                HostMessage::QueueDownload(download) => {
                    println!("queueing download: {}", download.name());
                    let client = self.client.clone();

                    let _ = self.ui_sender.send(UiStateEvent::AddDownload(download.clone()));
                    self.permit_queue.push_back(download.id());
                    self.active_downloads.insert(download.id(), DownloadSupervisor::new(client, download, self.sender.clone(), self.ui_sender.clone(), self.db_manager.clone()));

                    self.distribute_permits();
                },
                HostMessage::PermitReleased(owned_semaphore_permit) => {
                    drop(owned_semaphore_permit);
                    self.distribute_permits();
                },
                HostMessage::DownloadFinished(download_id) => {
                    let _ = self.ui_sender.send(UiStateEvent::AddUpdate(crate::download::DownloadUpdate::StatusChanged { id: download_id, status: DownloadStatus::Completed }));
                    self.active_downloads.remove(&download_id);
   
                    if let Some(pos) = self.permit_queue.iter().position(|x| *x == download_id) {
                        self.permit_queue.remove(pos);
                    }

                    self.distribute_permits();
                },
                HostMessage::SupervisorSaturated(download_id) => {
                    println!("{} set to saturated", *download_id);
                    self.active_downloads.get_mut(&download_id).unwrap().set_saturated(true);
                },
            }
        }
    }

    fn distribute_permits(&mut self) {
        for download_id in &self.permit_queue {
            while self.connections_budget.available_permits() > 0 {
                let permit = match self.connections_budget.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => break, // no more permits left, so don't keep distributing
                };

                let supervisor = match self.active_downloads.get_mut(download_id) {
                    Some(supervisor) => supervisor,
                    None => break,
                };

                println!("{} saturated? {}", **download_id, supervisor.is_saturated());

                if supervisor.is_saturated() {
                    break;
                }

                supervisor.give_permit(ActiveDownloadPermit::new(permit, self.sender.clone()));
            }
        }
    }

    fn process_download(&self, url: String, id: DownloadId) -> JoinHandle<()> {
        let self_sender = self.sender.clone();
        let plugin_registry = self.plugin_registry.clone();

        tokio::spawn(async move {
            let (sender, receiver) = oneshot::channel();
            let cancel_token = CancellationToken::new();

            plugin_registry.parse(url.clone(), sender, cancel_token);

            if let Ok(message) = receiver.await {
                if let Some(download_task) = message {
                    let download = Download::new(*id, download_task);
                    let _ = self_sender.send(HostMessage::QueueDownload(download));
                }

                else {
                    eprintln!("No plugin found for url: {}", url);
                }
            };
        })
    }
}

pub struct HostHandle {
    sender: UnboundedSender<HostMessage>,
}

impl HostHandle {
    pub fn spawn(client: Client, host: Host, ui_sender: UnboundedSender<UiStateEvent>, db_manager: StateManager, plugin_registry: PluginRegistryHandler) -> (Self, JoinHandle<()>) {
        let (host_sender, host_receiver) = unbounded_channel();

        let host_manager = HostManager::new(client, host, host_sender.clone(), host_receiver, ui_sender, db_manager, plugin_registry);

        let handle = tokio::spawn(async move {
            host_manager.run().await;
        });

        let host_handle = Self { 
            sender: host_sender
        };

        (host_handle, handle)
    }

    pub fn process_download(&self, url: String, download_id: DownloadId) {
        println!("sending through handle");
        let _ = self.sender.send(HostMessage::ProcessDownload(url, download_id));
    }
}