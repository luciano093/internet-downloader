use std::{collections::{HashMap, VecDeque}, sync::Arc};

use reqwest::Client;
use tokio::{sync::{OwnedSemaphorePermit, Semaphore, mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel}}, task::JoinHandle};

use crate::{client_state_manager::UiStateEvent, download::{Download, DownloadId, DownloadStatus, hosts::Host}, download_task::DownloadSupervisor, state_manager::StateManager};

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
}

impl HostManager {
    pub fn new(client: Client, host: Host, sender: UnboundedSender<HostMessage>, receiver: UnboundedReceiver<HostMessage>, ui_sender: UnboundedSender<UiStateEvent>, db_manager: StateManager) -> Self {
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
        }
    }

    pub async fn run(mut self) {
        while let Some(message) = self.receiver.recv().await {
            match message {
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
                    // TODO: remove from queue
                },
                HostMessage::SupervisorSaturated(download_id) => {
                    self.active_downloads.get_mut(&download_id).unwrap().set_saturated(true);
                },
            }
        }
    }

    fn distribute_permits(&mut self) {
        if self.connections_budget.available_permits() > 0 {
            for download_id in &self.permit_queue {
                let permit = match self.connections_budget.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => break, // no more permits left, so don't keep distributing
                };

                let supervisor = match self.active_downloads.get_mut(download_id) {
                    Some(supervisor) => supervisor,
                    None => continue,
                };

                if supervisor.is_saturated() {
                    continue;
                }

                supervisor.give_permit(ActiveDownloadPermit::new(permit, self.sender.clone()));
            }
        }
    }
}

pub struct HostHandle {
    sender: UnboundedSender<HostMessage>,
}

impl HostHandle {
    pub fn spawn(client: Client, host: Host, ui_sender: UnboundedSender<UiStateEvent>, db_manager: StateManager) -> (Self, JoinHandle<()>) {
        let (host_sender, host_receiver) = unbounded_channel();

        let host_manager = HostManager::new(client, host, host_sender.clone(), host_receiver, ui_sender, db_manager);

        let handle = tokio::spawn(async move {
            host_manager.run().await;
        });

        let host_handle = Self { 
            sender: host_sender
        };

        (host_handle, handle)
    }

    pub fn queue_download(&self, download: Download) {
        let _ = self.sender.send(HostMessage::QueueDownload(download));
    }
}