use std::collections::HashMap;

use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::info;

use crate::client_state_manager::DownloadSnapshot;
use crate::db::state_manager::StateManager;
use crate::download::items::DownloadId;

enum SnapshotCommand {
    NewSupervisor(mpsc::UnboundedReceiver<DownloadSnapshot>),
    TakeSnapshot(oneshot::Sender<HashMap<DownloadId, DownloadSnapshot>>),
}

pub struct SnapshotManager {
    broadcast: broadcast::Sender<()>,
    supervisor_listeners: Vec<mpsc::UnboundedReceiver<DownloadSnapshot>>,
    db_manager: StateManager,
    receiver: mpsc::UnboundedReceiver<SnapshotCommand>,
}

impl SnapshotManager {
    fn new(db_manager: StateManager, receiver: mpsc::UnboundedReceiver<SnapshotCommand>) -> (Self, broadcast::Sender<()>) {
        let (broadcast_sender, _broadcast_receiver) = broadcast::channel(2);
        
        let snapshot_manager = Self {
            broadcast: broadcast_sender.clone(),
            supervisor_listeners: Vec::new(),
            db_manager,
            receiver,
        };

        (snapshot_manager, broadcast_sender)
    }
    
    pub async fn run(mut self) {
        loop {
            tokio::select! {
                Some(command) = self.receiver.recv() => {
                    match command {
                        SnapshotCommand::NewSupervisor(supervisor) => {
                            self.supervisor_listeners.push(supervisor);
                        },
                        SnapshotCommand::TakeSnapshot(reply) => {
                            info!("received TakeSnapshot");
                            let _ = self.broadcast.send(());
    
                            let mut dead_supervisors = Vec::new();
                            let mut active_downloads = Vec::new();
                            
                            for (index, listener) in self.supervisor_listeners.iter_mut().enumerate() {
                                match listener.recv().await {
                                    Some(download) => active_downloads.push(download),
                                    None => {
                                        dead_supervisors.push(index);
                                    }
                                }
                            }
    
                            for dead_supervisor in dead_supervisors.into_iter().rev() {
                                let _ = self.supervisor_listeners.remove(dead_supervisor);
                            }
    
                            let completed_downloads = self.db_manager.load_completed_downloads().await.unwrap();
                            let mut snapshot: HashMap<DownloadId, DownloadSnapshot> = completed_downloads
                                .into_iter()
                                .map(|(id, download)| (id, DownloadSnapshot::from(download)))
                                .collect();
                            
                            for download in active_downloads {
                                snapshot.insert(download.id, download);
                            }
    
                            let _ = reply.send(snapshot);
                            info!("TakeSnapshot finished");
                        },
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct AppSnapshotHandler {
    sender: mpsc::UnboundedSender<SnapshotCommand>,
    broadcast_sender: broadcast::Sender<()>,
}

impl AppSnapshotHandler {
    pub fn spawn(db_manager: StateManager) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        let (snapshot_manager, broadcast_sender) = SnapshotManager::new(db_manager, receiver);

        tokio::spawn(async move {
            snapshot_manager.run().await;
        });

        Self {
            sender,
            broadcast_sender,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<()> {
        self.broadcast_sender.subscribe()
    }

    pub async fn take_snapshot(&self) -> HashMap<DownloadId, DownloadSnapshot> {
        info!("called snapshot's take_snapshot function");
        let (reply_sender, reply_receiver) = oneshot::channel();

        let _ = self.sender.send(SnapshotCommand::TakeSnapshot(reply_sender));
        
        reply_receiver.await.unwrap()
    }
    
    pub fn add_supervisor(&self, supervisor: mpsc::UnboundedReceiver<DownloadSnapshot>) {
        let _ = self.sender.send(SnapshotCommand::NewSupervisor(supervisor));
    }
}
