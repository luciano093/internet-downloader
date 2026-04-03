use std::{collections::{HashMap, VecDeque}, sync::{Arc, Weak, atomic::{AtomicUsize, Ordering}}, time::Duration};

use tokio::{sync::{OwnedSemaphorePermit, Semaphore, mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel}, oneshot}, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace, warn};
use url::Host;

use crate::{client_state_manager::UiStateEvent, context::AppContext, download::{Download, DownloadId, DownloadStatus, DownloadUpdate, ManagerCommand}, download_task::DownloadSupervisor};

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
    fn new(permit: OwnedSemaphorePermit, host_sender: UnboundedSender<HostMessage>, authority: Weak<()>, _guard: PermitGuard) -> Self {
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
    CancelDownload(DownloadId),
    PauseDownload(DownloadId),
    ResumeDownload(Download),
    DownloadFinished(DownloadId),
    PermitReleased(OwnedSemaphorePermit),
    RequestPermits(DownloadId),
    RateLimited(Option<u64>),
}

pub struct HostManager {
    host: Host,
    sender: UnboundedSender<HostMessage>,
    receiver: UnboundedReceiver<HostMessage>,
    active_downloads: HashMap<DownloadId, DownloadSupervisor>, // Maybe change this to a BTreeMap for order
    connections_budget: Arc<Semaphore>,
    permit_queue: VecDeque<DownloadId>,
    app_context: AppContext,
    download_supervisors_debt: Arc<AtomicUsize>, // How many permits the supervisors still have to return
    authority: Arc<()>,
    rate_limited: bool,
}

impl HostManager {
    pub fn new(app_context: AppContext, host: Host, sender: UnboundedSender<HostMessage>, receiver: UnboundedReceiver<HostMessage>) -> Self {
        Self {
            host,
            sender,
            receiver,
            active_downloads: HashMap::new(),
            connections_budget: Arc::new(Semaphore::const_new(16)),
            permit_queue: VecDeque::new(),
            app_context,
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
                    debug!("Rate limit lifted, resuming downloads.");
                    self.rate_limited = false;
                    self.distribute_permits().await;
                }
                Some(message) = self.receiver.recv() => {
                    match message {
                        HostMessage::ProcessDownload(url, download_id) => {
                            self.process_download(url, download_id);
                        },
                        HostMessage::QueueDownload(download) => {
                            trace!("queueing download: {}", download.name());

                            self.permit_queue.push_back(download.id());
                            self.active_downloads.insert(download.id(), DownloadSupervisor::new(self.app_context.clone(), download.clone(), self.sender.clone()));
                            let _ = self.app_context.ui_sender.send(UiStateEvent::AddDownload(download));

                            self.distribute_permits().await;
                        },
                        HostMessage::PermitReleased(owned_semaphore_permit) => {
                            // Check if we owe debt because of the global limit having been reduced
                            let current_debt = self.download_supervisors_debt.load(Ordering::Acquire);
    
                            if current_debt > 0 {
                                // We need to shrink the global pool.
                                // Instead of dropping it (which returns it to the pool), we forget it (permanently decreases max permits).
                                owned_semaphore_permit.forget();
                                
                                // We pay 1 unit of debt
                                self.download_supervisors_debt.fetch_sub(1, Ordering::SeqCst);
                                trace!("Permit forgotten to pay debt. Remaining debt: {}", current_debt - 1);
                            } else {
                                // If we have no debt, we drop normally
                                drop(owned_semaphore_permit);
                            }

                            // Now we distribute whatever is left in the pool based on demand
                            self.distribute_permits().await
                        },
                        HostMessage::DownloadFinished(download_id) => {
                            let _ = self.app_context.ui_sender.send(UiStateEvent::AddUpdate(crate::download::DownloadUpdate::StatusChanged { id: download_id, status: DownloadStatus::Completed }));
                            self.active_downloads.remove(&download_id);
        
                            if let Some(pos) = self.permit_queue.iter().position(|x| *x == download_id) {
                                self.permit_queue.remove(pos);
                            }

                            self.distribute_permits().await;
                        },
                        HostMessage::RequestPermits(download_id) => {
                            trace!("{} is requesting permits", *download_id);
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
                        HostMessage::CancelDownload(download_id) => {
                            // Remove it from the permit queue so manager doesn't give permits to a canceled download
                            if let Some(pos) = self.permit_queue.iter().position(|x| *x == download_id) {
                                self.permit_queue.remove(pos);
                            }

                            if let Some(mut supervisor) = self.active_downloads.remove(&download_id) {
                                let download_manager = self.app_context.download_manager.clone();
                                tokio::spawn(async move {
                                    if let Some(handle) = supervisor.handle_mut() { 
                                        handle.abort();
                                        let _ = handle.await; 
                                        
                                        drop(supervisor);
                                    }

                                    let _ = download_manager.send(ManagerCommand::CleanUpDownload(download_id));
                                });
                            } else {
                                let _ = self.app_context.download_manager.send(ManagerCommand::CleanUpDownload(download_id));
                            }
                        },
                        HostMessage::PauseDownload(download_id) => {
                            // Remove it from the permit queue so manager doesn't give permits to a paused download
                            if let Some(pos) = self.permit_queue.iter().position(|x| *x == download_id) {
                                self.permit_queue.remove(pos);
                            }

                            if let Some(supervisor) = self.active_downloads.remove(&download_id) {
                                supervisor.pause();
                                
                                drop(supervisor);
                            }
                        },
                        HostMessage::ResumeDownload(mut download) => {
                            if download.status() == DownloadStatus::Completed {
                                debug!("Download {} is already completed! Ignoring resume command.", download.name());
                                return;
                            }

                            let id = download.id();
                            
                            if self.active_downloads.contains_key(&id) || self.permit_queue.contains(&id) {
                                warn!("Download {} is already active! Ignoring resume command.", id);
                                return;
                            }

                            trace!("Resuming download: {}", download.name());

                            download.set_status(DownloadStatus::InProgress);

                            self.permit_queue.push_back(id);
                            self.active_downloads.insert(id, DownloadSupervisor::new(self.app_context.clone(), download, self.sender.clone()));
                            
                            let _ = self.app_context.ui_sender.send(UiStateEvent::AddUpdate(
                                DownloadUpdate::StatusChanged { 
                                    id, 
                                    status: DownloadStatus::InProgress 
                                }
                            ));

                            self.distribute_permits().await;
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
            let supervisor = match self.active_downloads.get_mut(download_id) {
                Some(supervisor) => supervisor,
                None => break,
            };

            while self.connections_budget.available_permits() > 0 
            && supervisor.demand().load(Ordering::Acquire) > 0 
            {
                
                let permit = match self.connections_budget.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => return,
                };

                info!("Giving permit to {}", supervisor.download_id());

                supervisor.demand().fetch_sub(1, Ordering::SeqCst);

                let guard = PermitGuard::new(supervisor.permit_count());

                supervisor.give_permit(ActiveDownloadPermit::new(
                    permit, 
                    self.sender.clone(), 
                    Arc::downgrade(&self.authority), 
                    guard
                ));
            }
        }
    }

    fn process_download(&self, url: String, id: DownloadId) -> JoinHandle<()> {
        let self_sender = self.sender.clone();
        let plugin_registry = self.app_context.plugin_registry.clone();

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
                    warn!("No plugin found for url: {}", url);
                }
            };
        })
    }
}

pub struct HostHandle {
    sender: UnboundedSender<HostMessage>,
}

impl HostHandle {
    pub fn spawn(app_context: AppContext, host: Host) -> (Self, JoinHandle<()>) {
        let (host_sender, host_receiver) = unbounded_channel();

        let host_manager = HostManager::new(app_context, host, host_sender.clone(), host_receiver);

        let handle = tokio::spawn(async move {
            host_manager.run().await;
        });

        let host_handle = Self { 
            sender: host_sender
        };

        (host_handle, handle)
    }

    pub fn process_download(&self, url: String, download_id: DownloadId) {
        trace!("sending through handle");
        let _ = self.sender.send(HostMessage::ProcessDownload(url, download_id));
    }

    pub fn cancel_download(&self, download_id: DownloadId) {
        let _ = self.sender.send(HostMessage::CancelDownload(download_id));
    }

    pub fn pause_download(&self, download_id: DownloadId) {
        let _ = self.sender.send(HostMessage::PauseDownload(download_id));
    }

    pub fn queue_download(&self, download: Download) {
        let _ = self.sender.send(HostMessage::ResumeDownload(download));
    }
}