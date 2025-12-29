use std::{collections::{HashMap, VecDeque}, sync::{Arc, Weak, atomic::{AtomicUsize, Ordering}}, time::Duration};

use reqwest::Client;
use tokio::{sync::{OwnedSemaphorePermit, Semaphore, mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel}, oneshot}, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use url::Host;

use crate::{client_state_manager::UiStateEvent, download::{Download, DownloadId, DownloadStatus}, download_task::DownloadSupervisor, plugin_registry::PluginRegistryHandler, state_manager::StateManager};

struct PermitGuard {
    counter: Arc<AtomicUsize>,
}

impl PermitGuard {
    fn new(counter: Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::SeqCst);
        Self { counter }
    }
}

impl Drop for PermitGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::SeqCst);
    }
}

pub struct ActiveDownloadPermit {
    permit: Option<OwnedSemaphorePermit>,
    host_sender: UnboundedSender<HostMessage>,
    authority: Weak<()>,
    _guard: Option<PermitGuard>,
}

impl ActiveDownloadPermit {
    pub fn new(permit: OwnedSemaphorePermit, host_sender: UnboundedSender<HostMessage>, authority: Weak<()>, _guard: PermitGuard) -> Self {
        Self { permit: Some(permit), host_sender, authority, _guard: Some(_guard) }
    }

    pub fn validate(mut self) -> Option<ValidDownloadPermit> {
        self.authority.upgrade()?; 
        
        let permit = self.permit.take()?;

        Some(ValidDownloadPermit { 
            permit: Some(permit), 
            host_sender: self.host_sender.clone(), 
            authority: self.authority.clone(),
            _guard: Some(self._guard.take()?),
        })
    }
}

impl Drop for ActiveDownloadPermit {
    fn drop(&mut self) {
        if let Some(permit) = self.permit.take() {
            let _ = self.host_sender.send(HostMessage::PermitReleased(permit));
        }
    }
}

pub struct ValidDownloadPermit {
    permit: Option<OwnedSemaphorePermit>,
    host_sender: UnboundedSender<HostMessage>,
    authority: Weak<()>,
    _guard: Option<PermitGuard>,
}

impl ValidDownloadPermit {
    pub fn downgrade(mut self) -> ActiveDownloadPermit {
        ActiveDownloadPermit::new(self.permit.take().unwrap(), self.host_sender.clone(), self.authority.clone(), self._guard.take().unwrap())
    }
}

impl Drop for ValidDownloadPermit {
    fn drop(&mut self) {
        if let Some(permit) = self.permit.take() {
            let _ = self.host_sender.send(HostMessage::PermitReleased(permit));
        }
    }
}

pub enum HostMessage {
    ProcessDownload(String, DownloadId),
    QueueDownload(Download),
    DownloadFinished(DownloadId),
    PermitReleased(OwnedSemaphorePermit),
    RequestPermits(DownloadId),
    RateLimited(Option<u64>),
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
    download_supervisors_debt: Arc<AtomicUsize>, // How many permits the supervisors still have to return
    authority: Arc<()>,
    rate_limited: bool,
}

impl HostManager {
    pub fn new(client: Client, host: Host, sender: UnboundedSender<HostMessage>, receiver: UnboundedReceiver<HostMessage>, ui_sender: UnboundedSender<UiStateEvent>, db_manager: StateManager, plugin_registry: PluginRegistryHandler) -> Self {
        Self {
            client,
            host,
            sender,
            receiver,
            active_downloads: HashMap::new(),
            connections_budget: Arc::new(Semaphore::const_new(16)),
            permit_queue: VecDeque::new(),
            ui_sender,
            db_manager,
            plugin_registry,
            download_supervisors_debt: Arc::new(0.into()),
            authority: Arc::new(()),
            rate_limited: false,
        }
    }

    pub async fn run(mut self) {
        let mut rate_limit_timer = std::pin::pin!(tokio::time::sleep(Duration::ZERO));

        loop {
            tokio::select! {
                _ = &mut rate_limit_timer, if self.rate_limited => {
                    println!("Rate limit lifted, resuming downloads.");
                    self.rate_limited = false;
                    self.distribute_permits().await;
                }
                Some(message) = self.receiver.recv() => {
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

                            self.distribute_permits().await;
                        },
                        HostMessage::PermitReleased(owned_semaphore_permit) => {
                            drop(owned_semaphore_permit);
                            self.distribute_permits().await;
                        },
                        HostMessage::DownloadFinished(download_id) => {
                            let _ = self.ui_sender.send(UiStateEvent::AddUpdate(crate::download::DownloadUpdate::StatusChanged { id: download_id, status: DownloadStatus::Completed }));
                            self.active_downloads.remove(&download_id);
        
                            if let Some(pos) = self.permit_queue.iter().position(|x| *x == download_id) {
                                self.permit_queue.remove(pos);
                            }

                            self.distribute_permits().await;
                        },
                        HostMessage::RequestPermits(download_id) => {
                            println!("{} is requesting permits", *download_id);
                            self.distribute_permits().await;
                        },
                        HostMessage::RateLimited(retry_after) => {
                            let delay = retry_after.map(Duration::from_secs).unwrap_or(Duration::from_secs(5));
                            let deadline = tokio::time::Instant::now() + delay;

                            // revoke all active permits
                            // drops old authority and creates a new one
                            self.authority = Arc::new(());
                            self.rate_limited = true;

                            rate_limit_timer.as_mut().reset(deadline);
                        },
                    }
                }
            }
        }
    }

    fn remove_permits(&mut self, amount: usize) {
        let forgotten = self.connections_budget.forget_permits(amount);

        let remaining = match amount.checked_sub(forgotten) {
            Some(remaining) => remaining,
            None => {
                // Probably impossible for there to be more forgotten permits than the amount set
                // but just in case, we return gracefully
                return;
            },
        };

        if remaining > 0 {
            self.download_supervisors_debt.fetch_add(remaining, Ordering::SeqCst);
        }
    }

    async fn distribute_permits(&mut self) {
        if self.rate_limited {
            return;
        }

        for download_id in &self.permit_queue {
            while self.connections_budget.available_permits() > 0 {
                println!("available permis: {}", self.connections_budget.available_permits());
                let permit = match self.connections_budget.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => break, // no more permits left, so don't keep distributing
                };

                let supervisor = match self.active_downloads.get_mut(download_id) {
                    Some(supervisor) => supervisor,
                    None => break,
                };

                println!("{} saturated? {}", **download_id, supervisor.is_saturated());
                tokio::task::yield_now().await; 

                if supervisor.is_saturated() {
                    break;
                }

                let guard = PermitGuard::new(supervisor.permit_count());

                println!("giving permit to supervisor");
                supervisor.give_permit(ActiveDownloadPermit::new(permit, self.sender.clone(), Arc::downgrade(&self.authority), guard));
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