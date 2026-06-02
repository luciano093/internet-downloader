use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use std::{usize, vec};

use tokio::fs::create_dir_all;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn};
use url::Host;

use crate::app::limiters::DownloadLimiterGroup;
use crate::app::context::AppContext;
use crate::app::manager::AppManagerEvent;
use crate::client_state_manager::FileUpdate;
use crate::download::error::FileFailureReason;
use crate::download::hosts::manager::{DownloadResult, HttpFailureKind, MetadataJob, MetadataResult, PermanentFailureKind, RangeJob, StreamJob};
use crate::download::items::{ActiveOperation, Download, DownloadId, DownloadItem, FileDownload, FileId, FileSize};
use crate::download::status::FileStatus;
use crate::download::verifier::VerifierHandle;
use crate::download::writer::DownloadWriterManager;
use crate::utils::file_utils::force_delete_file;
use crate::utils::network_utils::BandwidthLimiter;
use crate::utils::shared_file_map::SharedFileMap;

pub const BLOCK_SIZE: usize = 16384; // 16 KB
pub const HASH_CHUNK_SIZE: usize = 1048576; // 1 MB 
pub const BLOCKS_PER_HASH: usize = HASH_CHUNK_SIZE / BLOCK_SIZE; // (1 MB / 16 KB) or 64 blocks
const TARGET_RANGE_SIZE: usize = 5242880 / BLOCK_SIZE; // 320 ranges of chunks

enum DownloadCommand {
    Cancel { from_disk: bool },
    Pause,
    Resume,
    Finish,
}

struct MetadataInfo {
    file_id: FileId,
    url: Arc<String>,
    host: Arc<Host>,
}

impl MetadataInfo {
    fn new(file_id: FileId, url: Arc<String>, host: Arc<Host>) -> Self {
        Self { 
            file_id,
            url,
            host 
        }
    }
}

pub struct RangeInfo { 
    range: Range<usize>, 
    expected_bytes: u64, 
    previously_downloaded: u64,
}

impl RangeInfo {
    fn new(range: Range<usize>, expected_bytes: u64, previously_downloaded: u64) -> Self {
        Self { range, expected_bytes, previously_downloaded }
    }
}

#[derive(Clone)]
pub struct ChunkedFile {
    pub file_map: Arc<SharedFileMap>,
    pub progress: Arc<AtomicU64>,
}

pub struct DownloadSupervisor {
    download: Download,
    limiters: Arc<DownloadLimiterGroup>,
    app_context: AppContext,
    verifier: VerifierHandle,
    app_manager_event_sender: mpsc::UnboundedSender<AppManagerEvent>,
    receiver: mpsc::Receiver<DownloadCommand>,
    sender: mpsc::Sender<DownloadCommand>,
    cancel_token: CancellationToken,
}

impl DownloadSupervisor {
    fn spawn(
        download: Download, 
        limiters: Arc<DownloadLimiterGroup>, 
        app_context: AppContext,
        verifier: VerifierHandle,
        app_manager_event_sender: mpsc::UnboundedSender<AppManagerEvent>,
        receiver: mpsc::Receiver<DownloadCommand>,
        sender: mpsc::Sender<DownloadCommand>,
    ) -> Self {
        Self {
            download,
            limiters,
            app_context,
            verifier,
            app_manager_event_sender,
            receiver,
            sender,
            cancel_token: CancellationToken::new(),
        }
    }

    pub async fn run(mut self) {
        let changed_items = self.download.set_active_operation(Some(ActiveOperation::Verifying));
        self.app_context.ui_handle.update_operations(self.download.id(), changed_items);

        self.app_context.db_manager.write_download(&self.download).await.unwrap();

        let mut verification_receiver = {
            let (verification_sender, verification_receiver) = oneshot::channel();
            let _ = self.verifier.verify_download(self.download.clone(), verification_sender).await;
            Some(verification_receiver)
        };

        let mut save_interval = tokio::time::interval(Duration::from_millis(200));
            
        loop {
            tokio::select! {
                _ = save_interval.tick() => {
                    self.app_context.db_manager.write_download(&self.download).await.unwrap();
                }
                Some(command) = self.receiver.recv() => {
                    match command {
                        DownloadCommand::Cancel { from_disk } => {
                            let (reply, listener) = oneshot::channel();
                            let _ = self.verifier.cancel_verification(self.download.id(), reply).await;
                            let _ = listener.await;
                            
                            if from_disk {
                                for file in self.download.files().values() {
                                    let path = file.relative_path();
                                    if path.exists() {
                                        force_delete_file(&path);
                                    }
                                }

                                for folder in self.download.folders().values() {
                                    let path = folder.relative_path();
                                    
                                    if path.exists() && path.is_dir() {
                                        let _ = tokio::fs::remove_dir(&path).await;
                                    }
                                }
                            }
                            
                            self.app_context.db_manager.delete_download(self.download.id()).await.unwrap();
                            self.app_context.ui_handle.remove_download(self.download.id());
                            let _ = self.app_manager_event_sender.send(AppManagerEvent::RemoveDownload(self.download.id()));
                            return;
                        },
                        DownloadCommand::Pause => {
                            let (reply, listener) = oneshot::channel();
                            let _ = self.verifier.cancel_verification(self.download.id(), reply).await;
                            let _ = listener.await;
                            
                            verification_receiver = None;
                            self.app_context.db_manager.write_download(&self.download).await.unwrap();
                        },
                        DownloadCommand::Resume => {
                            let (new_verification_sender, new_verification_receiver) = oneshot::channel();
                            let _ = self.verifier.verify_download(self.download.clone(), new_verification_sender).await;
                            verification_receiver = Some(new_verification_receiver);
                        },
                        DownloadCommand::Finish => {
                            warn!("Finish received during verification for download {}", self.download.id());
                            self.app_context.db_manager.write_download(&self.download).await.unwrap();

                            let changed_items = self.download.set_active_operation(None);
                            self.app_context.ui_handle.update_operations(self.download.id(), changed_items);
                            
                            return;
                        },
                    }
                }
                reply = async {
                    match &mut verification_receiver {
                        Some(verification_receiver) => verification_receiver.await.map_err(|error| error.to_string()),
                        // This allows this branch to not resolve to Ok() or Err() while our verification_receiver is None
                        // As soon as the verification_receiver becomes None, this branch awaits that instead
                        None => std::future::pending::<Result<Download, String>>().await,
                    }
                } => {
                    match reply {
                        Ok(download) => {
                            debug!("Download supervisor for {} received a verification reply", self.download.id());
                            self.download = download;
                            break;
                        },
                        Err(error) => {
                            error!("Verifier dropped unexpectedly for download {}. Error: {}", self.download.id(), error);
                            let changed_items = self.download.set_all_files_failed(FileFailureReason::ClientError);
                            self.app_context.ui_handle.update_statuses(self.download.id(), changed_items);
                            self.app_context.db_manager.write_download(&self.download).await.unwrap();
                            return;
                        },
                    }
                }
            }
        }
    
        // get all hosts from network manager
        let urls: Vec<Arc<String>> = self.download.files()
            .values()
            .map(|file| file.url().clone())
            .collect();

        let (oneshot_sender, oneshot_receiver) = oneshot::channel();
        
        self.app_context.network_handle
            .get_host_handles(urls, oneshot_sender)
            .await;

        let host_handles = oneshot_receiver.await.unwrap();

        // get all metadata jobs for this download
        let metadata_info = self.get_metadata_jobs();

        // create event channels
        let (event_sender, mut event_receiver) = mpsc::channel(1000);
        let (active_operations_sender, mut active_operations_receiver) = mpsc::unbounded_channel::<(FileId, ActiveOperation)>();

        // send all metadata jobs to respective host managers
        for info in metadata_info {
            let handle = host_handles.get(&info.host).unwrap();

            let job = MetadataJob { 
                download_id: self.download.id(),
                file_id: info.file_id, 
                url: info.url, 
                result: event_sender.clone(),
                cancel_token: self.cancel_token.clone(),
                active_operations_sender: active_operations_sender.clone(),
                retries: 0,
            };
            
            handle.queue_metadata(job).await;
        }

        let mut chunked_files = HashMap::new();
        let download_id = self.download.id();

        let changed_items = self.download.set_active_operation(Some(ActiveOperation::Queued));
        self.app_context.ui_handle.update_operations(self.download.id(), changed_items);

        for (_file_id, file) in self.download.files() {
            if let Some(FileSize::Known(file_size)) = file.size() {
                // Already fully downloaded
                if file.status() == FileStatus::Completed || file.blocks().all() {
                    continue;
                }
                
                let jobs = Self::get_range_jobs(
                    download_id, 
                    file, 
                    file_size,
                    &mut chunked_files,
                    self.limiters.clone(),
                    self.cancel_token.clone(),
                    event_sender.clone(), 
                    active_operations_sender.clone()
                ).await;
                
                let handle = host_handles.get(file.host_ref()).unwrap();
                let _ = handle.queue_ranges(jobs).await;
            } 
            // We proably have a stream if the size is unknown
            else if let Some(FileSize::Unknown) = file.size() {
                let job = Self::get_stream_job(
                    download_id, 
                    file, 
                    self.limiters.clone(),
                    self.cancel_token.clone(),
                    event_sender.clone(), 
                    active_operations_sender.clone()
                );

                let handle = host_handles.get(file.host_ref()).unwrap();
                let _ = handle.queue_stream(job).await;
            }
        }

        let mut save_interval = tokio::time::interval(Duration::from_millis(100));
        
        loop {
            tokio::select! {
                _ = save_interval.tick() => {
                    self.app_context.db_manager.write_download(&self.download).await.unwrap();
                }
                Some(command) = self.receiver.recv() => {
                    match command {
                        DownloadCommand::Cancel { from_disk } => {
                            // Cancel all active tasks
                            self.cancel_token.cancel();

                            if from_disk {
                                for file in self.download.files().values() {
                                    let path = file.relative_path();
                                    if path.exists() {
                                        force_delete_file(&path);
                                    }
                                }

                                for folder in self.download.folders().values() {
                                    let path = folder.relative_path();
                                    
                                    if path.exists() && path.is_dir() {
                                        let _ = tokio::fs::remove_dir(&path).await;
                                    }
                                }
                            }

                            // Delete from DB
                            self.app_context.db_manager.delete_download(self.download.id()).await.unwrap();
                            
                            // Tell the frontend
                            self.app_context.ui_handle.remove_download(self.download.id());
                            
                            // Tell AppManager to remove from registry
                            let _ = self.app_manager_event_sender.send(AppManagerEvent::RemoveDownload(self.download.id()));
                            
                            break;
                        },
                        DownloadCommand::Pause => {
                            // Kill all active work
                            self.cancel_token.cancel();
                            self.cancel_token = CancellationToken::new();

                            // Drain the event channel
                            while let Ok(_) = event_receiver.try_recv() {}
                            while let Ok(_) = active_operations_receiver.try_recv() {}

                            let changed_items = self.download.set_active_operation(Some(ActiveOperation::Paused));
                            self.app_context.ui_handle.update_operations(self.download.id(), changed_items);

                            // Save to DB
                            self.app_context.db_manager.write_download(&self.download).await.unwrap();
                        },
                        DownloadCommand::Resume => {
                            let changed_items = self.download.set_active_operation(Some(ActiveOperation::Queued));
                            self.app_context.ui_handle.update_operations(self.download.id(), changed_items);

                            for (_file_id, file) in self.download.files() {
                                if let Some(FileSize::Known(file_size)) = file.size() {
                                    // Already fully downloaded
                                    if file.status() == FileStatus::Completed || file.blocks().all() {
                                        continue;
                                    }
                                    
                                    let jobs = Self::get_range_jobs(
                                        download_id, 
                                        file, 
                                        file_size,
                                        &mut chunked_files,
                                        self.limiters.clone(),
                                        self.cancel_token.clone(),
                                        event_sender.clone(), 
                                        active_operations_sender.clone()
                                    ).await;
                                    
                                    let handle = host_handles.get(file.host_ref()).unwrap();
                                    let _ = handle.queue_ranges(jobs).await;
                                } 
                                // We proably have a stream if the size is unknown
                                else if let Some(FileSize::Unknown) = file.size() {
                                    let job = Self::get_stream_job(
                                        download_id, 
                                        file, 
                                        self.limiters.clone(),
                                        self.cancel_token.clone(),
                                        event_sender.clone(), 
                                        active_operations_sender.clone()
                                    );
                    
                                    let handle = host_handles.get(file.host_ref()).unwrap();
                                    let _ = handle.queue_stream(job).await;
                                }
                            }
                        },
                        DownloadCommand::Finish => {
                            info!("Download finished: {} ({}) with final status: {:?}", self.download.id(), self.download.name(), self.download.status());
                            self.app_context.db_manager.write_download(&self.download).await.unwrap();
                            let _ = self.app_manager_event_sender.send(AppManagerEvent::FinishDownload(self.download.id()));
                            break;
                        },
                    }
                }
                Some(event) = event_receiver.recv() => {
                    match event {
                        DownloadResult::Metadata { file_id, metadata } => {
                            debug!("Received metadata for file {}", file_id);
                            let download_id = self.download.id();
                            
                            let file = match self.download.get_file_mut(&file_id) {
                                Some(file) => file,
                                None => {
                                    warn!("Got metadata for file id {}, but this file does not exist in download {}", file_id, self.download.id());
                                    continue;
                                }
                            };

                            // if it doesn't exist for some reason, ask the network manager again
                            // and store it back to host handles
                            let handle = host_handles.get(file.host_ref()).unwrap();

                            // Once we have metdata, we can get start creating download tasks for a host
                            match metadata {
                                MetadataResult::Stream { file_size, file_name } => {
                                    file.set_size(file_size);
                                    file.set_file_name(file_name);

                                    // Set blocks size
                                    if let FileSize::Known(file_size) = file_size {    
                                        self.app_context.ui_handle.update_file(download_id, FileUpdate::FileSize { id: file_id, len: file_size });
                                        
                                        let block_count = file_size.div_ceil(BLOCK_SIZE as u64) as usize;
                                        file.blocks_mut().resize(block_count, false);
    
                                        let hash_chunk_count = file_size.div_ceil(HASH_CHUNK_SIZE as u64) as usize;
                                        file.chunk_hashes_mut().resize(hash_chunk_count, None);
                                    }

                                    let job = Self::get_stream_job(
                                        download_id, 
                                        file, 
                                        self.limiters.clone(),
                                        self.cancel_token.clone(),
                                        event_sender.clone(), 
                                        active_operations_sender.clone()
                                    );
                                    
                                    let _ = handle.queue_stream(job).await;
                                },
                                MetadataResult::Chunked { file_size, file_name } => {
                                    debug!("Received metadata for file {} from download {} with file size {} and name {}", file.id(), download_id, file_size, file_name);
                                    file.set_size(FileSize::Known(file_size));
                                    file.set_file_name(file_name);

                                    self.app_context.ui_handle.update_file(download_id, FileUpdate::FileSize { id: file_id, len: file_size });

                                    // Set blocks size
                                    let block_count = file_size.div_ceil(BLOCK_SIZE as u64) as usize;
                                    file.blocks_mut().resize(block_count, false);

                                    let hash_chunk_count = file_size.div_ceil(HASH_CHUNK_SIZE as u64) as usize;
                                    file.chunk_hashes_mut().resize(hash_chunk_count, None);
                                    
                                    let jobs = Self::get_range_jobs(
                                        download_id, 
                                        file, 
                                        file_size,
                                        &mut chunked_files,
                                        self.limiters.clone(),
                                        self.cancel_token.clone(),
                                        event_sender.clone(), 
                                        active_operations_sender.clone()
                                    ).await;

                                    if let Some(job) = jobs.get(0) {
                                        debug!("Download supervisor is queuing {} range jobs for file {} from download {}", jobs.len(), job.file_id, job.download_id);
                                    } else {
                                        debug!("Download supervisor is sending request to queue {} range jobs", jobs.len());
                                    }
                                    
                                    let _ = handle.queue_ranges(jobs).await;
                                },
                            }
                        },
                        DownloadResult::Stream { file_id, bytes_downloaded } => {
                            let file = match self.download.get_file_mut(&file_id) {
                                Some(file) => file,
                                None => {
                                    warn!("Downloaded stream for file id {}, but this file does not exist in download {}", file_id, self.download.id());
                                    continue;
                                }
                            };
                            
                            // Store new progress and resize chunks
                            file.set_size(FileSize::Known(bytes_downloaded));

                            let chunk_count = bytes_downloaded.div_ceil(BLOCK_SIZE as u64) as usize;
                            file.blocks_mut().resize(chunk_count, true);

                            trace!("Got {} chunks for file {} in download {}", file.blocks().len(), file_id, self.download.id());

                            self.app_context.ui_handle.update_file(self.download.id(), FileUpdate::BytesDownloaded { id: file_id, len: bytes_downloaded });
                            
                            // Update new statuses
                            if let Some(changed_items) = self.download.set_file_status(file_id, FileStatus::Completed) {
                                self.app_context.ui_handle.update_statuses(self.download.id(), changed_items);
                            }

                            if self.download.is_completed() {
                                let _ =self.sender.send(DownloadCommand::Finish).await;
                            }
                        },
                        DownloadResult::Range { file_id, range, hashes } => {
                            // TODO: not update status every time, look for better way to switch to partial
                            if let Some(changed_items) = self.download.set_file_status(file_id, FileStatus::Partial) {
                                self.app_context.ui_handle.update_statuses(self.download.id(), changed_items);
                            }
                            
                            let file = match self.download.get_file_mut(&file_id) {
                                Some(file) => file,
                                None => {
                                    warn!("Downloaded range of blocks {}..{} for file id {}, but this file does not exist in download {}", range.start, range.end, file_id, self.download.id());
                                    continue;
                                }
                            };

                            file.blocks_mut()[range.start..range.end].fill(true);

                            let hash_start_index = range.start.div_ceil(BLOCKS_PER_HASH);
                            for (i, hash) in hashes.into_iter().enumerate() {
                                file.chunk_hashes_mut()[hash_start_index + i] = Some(hash);
                            }

                            // file has finished downloading
                            if file.blocks().all() {
                                let bytes_downloaded = chunked_files
                                    .get(&file_id)
                                    .map(|chunked_map| {
                                        chunked_map.progress.load(Ordering::Relaxed)
                                    })
                                    .unwrap_or(0);

                                trace!("file {} ({}) finished! got {} bytes", file.id(), file.name(), bytes_downloaded);

                                let changed_items = self.download.set_file_active_operation(file_id, None);
                                self.app_context.ui_handle.update_operations(self.download.id(), changed_items);
                                
                                if let Some(changed_items) = self.download.set_file_status(file_id, FileStatus::Completed) {
                                    self.app_context.ui_handle.update_statuses(self.download.id(), changed_items);
                                }
                                
                                chunked_files.remove(&file_id);

                                if self.download.is_completed() {
                                    let _ =self.sender.send(DownloadCommand::Finish).await;
                                }
                            }

                        },
                        DownloadResult::Failed(permanent_failure) => {
                            let file_id = permanent_failure.operation.file_id();

                            match permanent_failure.kind {
                                PermanentFailureKind::Disk(_error_kind) => {
                                    // Disk errors are fatal for the entire download
                                    error!("Disk error for download {}: {}", self.download.id(), permanent_failure);
                                    let changed_items = self.download.set_all_files_failed(FileFailureReason::DiskError);
                                    self.app_context.ui_handle.update_statuses(self.download.id(), changed_items);
                                    self.app_context.db_manager.write_download(&self.download).await.unwrap();
                                    break;
                                },
                                PermanentFailureKind::Http(HttpFailureKind::FileNotFound) => {
                                    // File is 404, we can't retry this file
                                    warn!("File {} not found (404) for download {}", file_id, self.download.id());
                                    
                                    if let Some(changed_items) = self.download.set_file_status(file_id, FileStatus::Failed(FileFailureReason::ServerError)) {
                                        self.app_context.ui_handle.update_statuses(self.download.id(), changed_items);
                                    }
                                    self.app_context.db_manager.write_download(&self.download).await.unwrap();
                        
                                    if self.download.is_completed() {
                                        let _ =self.sender.send(DownloadCommand::Finish).await;
                                    }
                                },
                                PermanentFailureKind::Http(http_failure_kind) => {
                                    warn!("File {} had HTTP error {} for download {}", file_id, http_failure_kind, self.download.id());
                                    
                                    if let Some(changed_items) = self.download.set_file_status(file_id, FileStatus::Failed(FileFailureReason::ServerError)) {
                                        self.app_context.ui_handle.update_statuses(self.download.id(), changed_items);
                                    }
                                    self.app_context.db_manager.write_download(&self.download).await.unwrap();
                        
                                    if self.download.is_completed() {
                                        let _ =self.sender.send(DownloadCommand::Finish).await;
                                    }
                                },
                                PermanentFailureKind::TooManyRetries { attempts } => {
                                    error!("Too many failed retries ({}) for file {} in download {}: {}", attempts, file_id, self.download.id(), permanent_failure);
                                    if let Some(changed_items) = self.download.set_file_status(
                                        file_id, 
                                        FileStatus::Failed(FileFailureReason::ClientError)
                                    ) {
                                        self.app_context.ui_handle.update_statuses(self.download.id(), changed_items);
                                    }
                                    self.app_context.db_manager.write_download(&self.download).await.unwrap();
                        
                                    if self.download.is_completed() {
                                        let _ = self.sender.send(DownloadCommand::Finish).await;
                                    }
                                },
                            }
                        },
                    }
                }
                Some((file_id, active_operation)) = active_operations_receiver.recv() => {
                    let changed_items = self.download.set_file_active_operation(file_id, Some(active_operation));
                    self.app_context.ui_handle.update_operations(self.download.id(), changed_items);
                }
            }
        }
    }

    /// Gets a metadata job and automatically updates its cursor as needed
    fn get_metadata_jobs(&self) -> Vec<MetadataInfo> {
        let mut jobs = vec![];
        
        for (&file_id, file) in self.download.files() {
            // No file size means we still haven't gotten the metadata of this file
            if file.size().is_none() {
                jobs.push(MetadataInfo::new(file_id, file.url(), file.host()))
            }
        }

        jobs
    }

    fn get_stream_job(
        download_id: DownloadId, 
        file: &FileDownload, 
        limiters: Arc<DownloadLimiterGroup>,
        cancel_token: CancellationToken,
        event_sender: mpsc::Sender<DownloadResult>, 
        active_operations_sender: mpsc::UnboundedSender<(FileId, ActiveOperation)>
    ) -> StreamJob {
        let download_limiter = limiters.download_limiter();
        let file_limiter = limiters
            .file_limiters()
            .get(&file.id())
            .map(|limiter| limiter.clone())
            .get_or_insert_with(|| {
            let file_limiter = Arc::new(BandwidthLimiter::new(0));
            file_limiter.set_unlimited(true);
            file_limiter
        }).clone();

        StreamJob {
            download_id,
            file_id: file.id(), 
            url: file.url(), 
            path: file.relative_path().to_owned(), 
            download_limiter, 
            file_limiter, 
            result: event_sender,
            cancel_token: cancel_token,
            active_operations_sender: active_operations_sender,
            retries: 0,
        }
    }

    async fn get_range_jobs(
        download_id: DownloadId, 
        file: &FileDownload, 
        file_size: u64,
        chunked_files: &mut HashMap<FileId, ChunkedFile>,
        limiters: Arc<DownloadLimiterGroup>,
        cancel_token: CancellationToken,
        event_sender: mpsc::Sender<DownloadResult>, 
        active_operations_sender: mpsc::UnboundedSender<(FileId, ActiveOperation)>
    ) -> Vec<RangeJob> {   
        let chunked_file = match chunked_files.get(&file.id()) {
            Some(chunked_file) => chunked_file.clone(),
            None => {
                let path = file.relative_path();
                if let Some(parent_path) = path.parent() {
                    create_dir_all(parent_path).await.unwrap();
                }
        
                let file_map = DownloadWriterManager::create_file(path.clone(), file_size).await.unwrap();
                let initial_bytes = file.calculate_initial_bytes(BLOCK_SIZE as u64);
                
                let chunked_file = ChunkedFile { 
                    file_map: Arc::new(file_map), 
                    progress: Arc::new(AtomicU64::new(initial_bytes)),
                };
                
                chunked_files.insert(file.id(), chunked_file.clone());

                chunked_file
            }
        };
        
        let range_info = Self::get_ranges(file, file_size).await;

        let download_limiter = limiters.download_limiter();
        let file_limiter = limiters
            .file_limiters()
            .get(&file.id())
            .map(|limiter| limiter.clone())
            .get_or_insert_with(|| {
            let file_limiter = Arc::new(BandwidthLimiter::new(0));
            file_limiter.set_unlimited(true);
            file_limiter
        }).clone();

        let mut jobs = Vec::with_capacity(range_info.len());
        let is_converting_to_stream = Arc::new(AtomicBool::new(false));

        for info in range_info {
            let job = RangeJob {
                download_id,
                file_id: file.id(), 
                url: file.url(),
                range: info.range,
                expected_bytes: info.expected_bytes,
                previously_downloaded: info.previously_downloaded, 
                file_map: chunked_file.file_map.clone(), 
                progress: chunked_file.progress.clone(), 
                download_limiter: download_limiter.clone(), 
                file_limiter: file_limiter.clone(), 
                result: event_sender.clone(),
                cancel_token: cancel_token.clone(),
                active_operations_sender: active_operations_sender.clone(),
                is_converting_to_stream: is_converting_to_stream.clone(),
                retries: 0,
            };

            jobs.push(job);
        }

        jobs
    }

    async fn get_ranges(file: &FileDownload, file_size: u64) -> Vec<RangeInfo> {
        let blocks = file.blocks();
        let mut ranges = Vec::new();

        let mut cursor = 0;

        // We first find the first undownloaded block and start gathering undownloaded blocks
        // for our range from there
        while let Some(relative_start) = blocks[cursor..].first_zero() {

            // In the case that the start index isn't aligned with the boundary we need for ranges
            // we snap back to force align it
            // For example: if we want to align to a 1MB range size, but the zero_index is 
            // 3.5 MB, this converts it to 3MB
            let absolute_start = cursor + relative_start;

            let start_index = (absolute_start / BLOCKS_PER_HASH).saturating_mul(BLOCKS_PER_HASH);
            
            // We aim for the target range size, this is the ideal end for this range
            let max_end = (start_index as u64 + TARGET_RANGE_SIZE as u64).min(blocks.len() as u64) as usize;

            // We look for the first block that is downloaded in the range from the start index to our max index
            let first_one_index = blocks[start_index..max_end]
                .first_one()
                .map(|index| index + start_index);

            // This would be our end index if we didn't have to align to BLOCKS_PER_HASH
            let unaligned_end_index = first_one_index.unwrap_or(max_end);

            let mut end_index = {
                // We align to our range size   
                let aligned_end = (unaligned_end_index / BLOCKS_PER_HASH).saturating_mul(BLOCKS_PER_HASH);

                // If the alignment was less than our start index, we have a problem and should align forward
                if aligned_end > start_index {
                    aligned_end
                } else {
                    start_index.saturating_add(BLOCKS_PER_HASH)
                }
            };

            // We do want to limit the index to the maximum chunk even if it ends up unaligned
            end_index = end_index.min(blocks.len());

            let range = Range { start: start_index, end: end_index };

            // We calculate how many bytes we had previously downloaded for this range if any.
            // this helps in error handling if a range fails before it can be written to disk and we have
            // to report the bytes retracted.
            let mut previously_downloaded = 0;

            for block_index in range.start..range.end {
                if blocks.get(block_index).unwrap() == true {
                    // If we are on the last block, it is very possible that the size of the block
                    // in bytes is less than the normal CHUNK_SIZE
                    if block_index == (blocks.len() - 1) {
                        let block_start_byte = block_index as u64 * BLOCK_SIZE as u64;
                        previously_downloaded += file_size - block_start_byte;
                    } else {
                        previously_downloaded += BLOCK_SIZE as u64; 
                    }
                }
            }

            let expected_bytes = Self::calculate_range_expected_len(BLOCK_SIZE, &range, file_size);

            cursor = range.end;
            debug!("cursor: {}", cursor);
            ranges.push(RangeInfo::new(range, expected_bytes, previously_downloaded));
        }

        ranges
    }

    fn calculate_range_expected_len(chunk_size: usize, range: &Range<usize>, file_size: u64) -> u64 {
        let start_byte = range.start as u64 * chunk_size as u64;
        let theoretical_end = range.end as u64 * chunk_size as u64;

        let actual_end = std::cmp::min(theoretical_end, file_size);
        let expected_len = actual_end.saturating_sub(start_byte);
        
        expected_len.min(file_size)
    }
}

pub struct DownloadHandle {
    sender: mpsc::Sender<DownloadCommand>
}

impl DownloadHandle {
    pub fn spawn(download: Download, limiters: Arc<DownloadLimiterGroup>, app_context: AppContext, verifier: VerifierHandle, app_manager_event_sender: mpsc::UnboundedSender<AppManagerEvent>) -> Self {
        let (sender, receiver) = mpsc::channel(1000);
        
        let download_supervisor = DownloadSupervisor::spawn(download, limiters, app_context, verifier, app_manager_event_sender, receiver, sender.clone());

        tokio::spawn(async move {
           download_supervisor.run().await; 
        });
        
        Self { 
            sender,
        }
    }

    pub async fn cancel(&self, from_disk: bool) {
        let _ = self.sender.send(DownloadCommand::Cancel { from_disk }).await;
    }
    
    pub async fn pause(&self) {
        let _ = self.sender.send(DownloadCommand::Pause).await;
    }
    
    pub async fn resume(&self) {
        let _ = self.sender.send(DownloadCommand::Resume).await;
    }
}
