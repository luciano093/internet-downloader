use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use indexmap::IndexMap;
use memmap2::MmapOptions;
use tokio::sync::Semaphore;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::client_state_manager::UiStateEvent;
use crate::db::state_manager::StateManager;
use crate::download::{DownloadId, DownloadUpdate, FileFailureReason, FileSize, FileUpdate, FolderUpdate, ItemUpdate, ManagerCommand};
use crate::download::items::{ActiveOperation, ChangedItemOperation, ChangedItemStatus, Download, DownloadItem, DownloadType};
use crate::download::status::FileStatus;
use crate::download_task::{BLOCKS_PER_HASH, HASH_CHUNK_SIZE};
use crate::utils::file_utils::hash_file;

struct FileVerificationDiff {
    pub file_id: usize,
    pub new_status: Option<FileStatus>,
    pub failed_chunks: Option<Vec<usize>>,
}

pub enum VerifierMessage {
    VerifyDownload(Download),
    VerifyDownloads(IndexMap<DownloadId, Download>),
    VerificationFinished(DownloadId),
    CancelVerification(DownloadId),
    PauseVerification(DownloadId),
}

struct DownloadGuard {
    download_id: DownloadId,
    download: Option<Download>,
    ui_sender: UnboundedSender<UiStateEvent>,
    verifier_sender: mpsc::Sender<VerifierMessage>,
}

impl Drop for DownloadGuard {
    fn drop(&mut self) {
        let download = self.download.take();
        let ui_sender = self.ui_sender.clone();
        let verifier_sender = self.verifier_sender.clone();
        let download_id = self.download_id;

        tokio::spawn(async move {
            if let Some(mut download) = download {
                let changed_items = download.set_active_operation(None);
                
                // Send every change to ui
                for item in changed_items {
                    match item {
                        ChangedItemOperation::File { id, operation } => {
                            let _ = ui_sender.send(UiStateEvent::AddUpdate(
                                DownloadUpdate::ItemUpdated { 
                                    id: download_id, 
                                    item_update: ItemUpdate::File(FileUpdate::Operation { id, operation }) 
                                }
                            ));
                        },
                        ChangedItemOperation::Folder { id, operation } => {
                            let _ = ui_sender.send(UiStateEvent::AddUpdate(
                                DownloadUpdate::ItemUpdated { 
                                    id: download_id,
                                    item_update: ItemUpdate::Folder(FolderUpdate::Operation { id, operation }) 
                                }
                            ));
                        }
                        ChangedItemOperation::Download(operation) => {
                            let _ = ui_sender.send(UiStateEvent::AddUpdate(
                                DownloadUpdate::OperationChanged { 
                                    id: download_id,
                                    operation,
                                }
                            ));
                        },
                    }
                }
            }

            let _ = verifier_sender.send(VerifierMessage::VerificationFinished(download_id)).await;
        });
    }
}

struct Verifier {
    receiver: mpsc::Receiver<VerifierMessage>,
    sender: mpsc::Sender<VerifierMessage>,
    download_manager: UnboundedSender<ManagerCommand>,
    ui_sender: UnboundedSender<UiStateEvent>,
    db_manager: StateManager,
    handles: HashMap<DownloadId, (JoinHandle<()>, Arc<AtomicBool>)>, // We save an atomic bool in case we need to kill nested handles
    semaphore: Arc<Semaphore>,
}

impl Verifier {
    fn new(
        receiver: mpsc::Receiver<VerifierMessage>,
        sender: mpsc::Sender<VerifierMessage>,
        download_manager: UnboundedSender<ManagerCommand>,
        ui_sender: UnboundedSender<UiStateEvent>,
        db_manager: StateManager,
    ) -> Self
    {

        Self {
            receiver,
            sender,
            download_manager,
            ui_sender,
            db_manager,
            handles: HashMap::new(),
            semaphore: Arc::new(Semaphore::new(1)), 
        }
    }

    async fn run(mut self) {
        while let Some(message) = self.receiver.recv().await {
            match message {
                VerifierMessage::CancelVerification(download_id) => {
                    if let Some((handle, cancel_flag)) = self.handles.remove(&download_id) {
                        cancel_flag.store(true, Ordering::Relaxed);
                        handle.abort();

                        // We want to wait until the handle ends before sending the clean up message
                        let _ = handle.await; 
                    }

                    let _ = self.download_manager.send(ManagerCommand::CleanUpDownload(download_id));
                },
                VerifierMessage::PauseVerification(download_id) => {
                    if let Some((handle, cancel_flag)) = self.handles.remove(&download_id) {
                        cancel_flag.store(true, Ordering::Relaxed);
                        handle.abort();
                    }
                },
                VerifierMessage::VerifyDownload(download) => {
                    let download_name = download.name().clone();

                    if self.handles.contains_key(&download.id()) {
                        warn!("Download {} is already verifying. Ignoring duplicate request.", download.id());
                        continue;
                    }

                    info!("Queued {} for verification", download_name); 

                    let _ = self.ui_sender.send(UiStateEvent::AddDownload(download.clone()));
                    self.handle_download(download).await;
                },
                VerifierMessage::VerifyDownloads(download_map) => {
                    for (_, download) in download_map {
                        let download_name = download.name().clone();

                        if self.handles.contains_key(&download.id()) {
                            warn!("Download {} is already verifying. Ignoring duplicate request.", download.id());
                            continue;
                        }

                        info!("Queued {} for verification", download_name); 

                        let _ = self.ui_sender.send(UiStateEvent::AddDownload(download.clone()));
                        self.handle_download(download).await;
                    }
                },
                VerifierMessage::VerificationFinished(download_id) => {
                    self.handles.remove(&download_id); 
                },
            }
        }
    }

    async fn handle_download(&mut self, mut download: Download) {
        let download_id = download.id();
        let changed_items = download.set_active_operation(Some(ActiveOperation::Verifying));
        
        // Send every change to ui
        for item in changed_items {
            match item {
                ChangedItemOperation::File { id, operation } => {
                    let _ = self.ui_sender.send(UiStateEvent::AddUpdate(
                        DownloadUpdate::ItemUpdated { 
                            id: download_id, 
                            item_update: ItemUpdate::File(FileUpdate::Operation { id, operation }) 
                        }
                    ));
                },
                ChangedItemOperation::Folder { id, operation } => {
                    let _ = self.ui_sender.send(UiStateEvent::AddUpdate(
                        DownloadUpdate::ItemUpdated { 
                            id: download_id,
                            item_update: ItemUpdate::Folder(FolderUpdate::Operation { id, operation }) 
                        }
                    ));
                }
                ChangedItemOperation::Download(operation) => {
                    let _ = self.ui_sender.send(UiStateEvent::AddUpdate(
                        DownloadUpdate::OperationChanged { 
                            id: download_id,
                            operation,
                        }
                    ));
                },
            }
        }

        self.db_manager.write_download(&download).await;

        let ui_sender = self.ui_sender.clone();
        let db_manager = self.db_manager.clone();
        let download_manager = self.download_manager.clone();

        let semaphore = self.semaphore.clone();
        let cancel_flag = Arc::new(AtomicBool::new(false));

        let task_cancel_flag = cancel_flag.clone();

        let mut download_guard = DownloadGuard { 
            download_id: download_id,
            download: Some(download),
            ui_sender: self.ui_sender.clone(),
            verifier_sender: self.sender.clone(),
        };

        let handle = tokio::spawn(async move {
            let start_time = std::time::Instant::now();
            
            info!("Started verifying {}.", download_guard.download.as_ref().unwrap().name());

            let _permit = semaphore.acquire_owned().await.unwrap();

            let diffs = Self::verify_download(download_guard.download.as_ref().unwrap(), task_cancel_flag).await;

            for diff in diffs {
                let file_id = diff.file_id;

                if let Some(new_status) = diff.new_status {
                    if let Some(changed_items) = download_guard.download.as_mut().unwrap().set_file_status(file_id, new_status) {
                        for item in changed_items {
                            match item {
                                ChangedItemStatus::File { id, status } => {
                                    let _ = ui_sender.send(UiStateEvent::AddUpdate(
                                        DownloadUpdate::ItemUpdated { 
                                            id: download_id, 
                                            item_update: ItemUpdate::File(FileUpdate::Status { id, status }) 
                                        }
                                    ));
                                },
                                ChangedItemStatus::Folder { id, status } => {
                                    let _ = ui_sender.send(UiStateEvent::AddUpdate(
                                        DownloadUpdate::ItemUpdated { 
                                            id: download_id,
                                            item_update: ItemUpdate::Folder(FolderUpdate::Status { id, status, }) 
                                        }
                                    ));
                                }
                                ChangedItemStatus::Download(status) => {
                                    let _ = ui_sender.send(UiStateEvent::AddUpdate(
                                        DownloadUpdate::StatusChanged { 
                                            id: download_id,
                                            status,
                                        }
                                    ));
                                },
                            }
                        }
                    }                      
                }

                if let Some(failed_chunks) = diff.failed_chunks {
                    if let Some(file) = download_guard.download.as_mut().unwrap().get_file_mut(&file_id) {
                        // set failed blocks back to false
                        let chunks = file.blocks_mut();

                        for failed_chunk in failed_chunks {
                            let start_block = failed_chunk * BLOCKS_PER_HASH;
                            let end_block = std::cmp::min(start_block + BLOCKS_PER_HASH, chunks.len());

                            for block_index in start_block..end_block {
                                chunks.get_mut(block_index).unwrap().set(false);
                            }
                        }
                    }
                }
            }

            let mut download = download_guard.download.take().unwrap();

            let changed_items = download.set_active_operation(None);
            
            // Send every change to ui
            for item in changed_items {
                match item {
                    ChangedItemOperation::File { id, operation } => {
                        let _ = ui_sender.send(UiStateEvent::AddUpdate(
                            DownloadUpdate::ItemUpdated { 
                                id: download_id, 
                                item_update: ItemUpdate::File(FileUpdate::Operation { id, operation }) 
                            }
                        ));
                    },
                    ChangedItemOperation::Folder { id, operation } => {
                        let _ = ui_sender.send(UiStateEvent::AddUpdate(
                            DownloadUpdate::ItemUpdated { 
                                id: download_id,
                                item_update: ItemUpdate::Folder(FolderUpdate::Operation { id, operation }) 
                            }
                        ));
                    }
                    ChangedItemOperation::Download(operation) => {
                        let _ = ui_sender.send(UiStateEvent::AddUpdate(
                            DownloadUpdate::OperationChanged { 
                                id: download_id,
                                operation,
                            }
                        ));
                    },
                }
            }

            info!("Finished verification {} in {:?}", download.name(), start_time.elapsed());

            db_manager.write_download(&download).await;
            let _ = download_manager.send(ManagerCommand::DownloadVerified(download));
        });

        self.handles.insert(download_id, (handle, cancel_flag));
    }

    async fn verify_download(download: &Download, task_cancel_flag: Arc<AtomicBool>) -> Vec<FileVerificationDiff> {
        let mut pending_changes = Vec::new();

        for (&id, download_item) in download.files() {
            if let DownloadType::File(file) = download_item {
                let mut new_status = None;
                let mut failed_chunks = None;

                // We first check if the file exists in disk
                let exists_physically = tokio::fs::try_exists(file.relative_path()).await.unwrap_or(false);

                if file.must_exist_in_disk() && !exists_physically {
                    new_status = Some(FileStatus::NotFound);
                }

                // Then we check if the download is completed, and if so, we check the stored hash
                // against the hash of the file in disk
                else if file.status() == FileStatus::Completed  {
                    let path = file.relative_path().to_path_buf();
                    let task_cancel_flag = task_cancel_flag.clone();

                    let hash = tokio::task::spawn_blocking(move || {
                        hash_file(&path, Some(task_cancel_flag))
                    }).await;

                    match hash {
                        Ok(Ok(hash)) => {
                            if Some(hash) != file.hash() {
                                new_status = Some(FileStatus::Failed(FileFailureReason::HashMismatch));
                            }
                        }   
                        // Hashing error
                        Ok(Err(error)) => {
                            warn!("Failed to read completed file during verification: {}", error);
                            new_status = Some(FileStatus::Failed(FileFailureReason::DiskError)); 
                        }
                        // Task error
                        Err(error) => {
                            if error.is_cancelled() {
                                warn!("Hashing task was cancelled.");
                                new_status = Some(FileStatus::Failed(FileFailureReason::ClientError));
                            } else {
                                warn!("Hashing task panicked: {}", error);
                                new_status = Some(FileStatus::Failed(FileFailureReason::ClientError)); 
                            }
                        }
                    }
                }
                // Otherwise we check individual chunk hashes to see how much of the file we truly have 
                else {
                    if let Some(FileSize::Known(size)) = file.size() {
                        // either we should be able to pass a vector of ranges
                        // or we should have a function where we can pass a file size, chunk size,
                        // and returns a vector of hashes
                        let path = file.relative_path().to_path_buf();
                        let chunks_to_check = file.chunk_hashes().to_owned();

                        if !chunks_to_check.is_empty() {
                            let task_cancel_flag = task_cancel_flag.clone();

                            let failed_indices = tokio::task::spawn_blocking(move || {
                                Self::verify_file_chunks(&path, HASH_CHUNK_SIZE, size as usize, chunks_to_check, task_cancel_flag)
                            }).await;

                            match failed_indices {
                                Ok(Ok(failed_indices)) => {
                                    if !failed_indices.is_empty() {
                                        failed_chunks = Some(failed_indices);
                                    }   
                                },
                                // File chunk hashing error
                                Ok(Err(error)) => {
                                    if error.kind() == std::io::ErrorKind::NotFound {
                                        warn!("Failed to find file {} during verification: {}", file.name(), error);
                                        new_status = Some(FileStatus::NotFound); 
                                    } else {
                                        warn!("Failed to read file {} during verification: {}", file.name(), error);
                                        new_status = Some(FileStatus::Failed(FileFailureReason::DiskError)); 
                                    }
                                },
                                // Task error
                                Err(error) => {
                                    if error.is_cancelled() {
                                        warn!("Hashing task was cancelled.");
                                        new_status = Some(FileStatus::Failed(FileFailureReason::ClientError));
                                    } else {
                                        warn!("Hashing task panicked: {}", error);
                                        new_status = Some(FileStatus::Failed(FileFailureReason::ClientError)); 
                                    }
                                }
                            }
                        }
                    }
                }

                if new_status.is_some() || failed_chunks.is_some() {
                    pending_changes.push(FileVerificationDiff {
                        file_id: id,
                        new_status,
                        failed_chunks,
                    });
                }
            }
        }

        pending_changes
    }

    fn verify_file_chunks(path: &Path, chunk_size: usize, file_len: usize, chunks_to_check: Vec<Option<[u8; 16]>>, task_cancel_flag: Arc<AtomicBool>) -> Result<Vec<usize>, std::io::Error> {
        let mut failed_chunks = Vec::new();

        let mut file = File::open(&path)?;
        let mut hasher = blake3::Hasher::new();

        // If the file is tiny, skip the mmap overhead and just read it.
        if file_len < 16 * 1024 {
            let mut buffer = vec![0u8; file_len];

            file.seek(SeekFrom::Start(0))?;
            file.read_exact(&mut buffer)?;

            for (index, expected) in chunks_to_check.iter().enumerate() {
                if task_cancel_flag.load(Ordering::Relaxed) {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Interrupted, 
                        "Verification cancelled by user"
                    ));
                }

                // We only want to check, if we know we have another hash to check against
                // first, otherwise we skip it
                if let Some(expected_hash) = expected {
                    let start = index * chunk_size;
                    let end = std::cmp::min(start + chunk_size, file_len);

                    if let Some(bytes) = buffer.get(start..end) {
                        // chunks are too small to use rayon here
                        hasher.update(&bytes);

                        let mut hash = [0u8; 16];
                        hasher.finalize_xof().fill(&mut hash);
                        hasher.reset();

                        if hash != *expected_hash {
                            failed_chunks.push(index);
                        }
                    } else {
                        failed_chunks.push(index);
                    }
                }
            }
        } else {
            let mmap = unsafe { 
                MmapOptions::new()
                    .map(&file)?
            };

            for (index, expected) in chunks_to_check.iter().enumerate() {
                if task_cancel_flag.load(Ordering::Relaxed) {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Interrupted, 
                        "Verification cancelled by user"
                    ));
                }
                
                // We only want to check, if we know we have another hash to check against
                // first, otherwise we skip it
                if let Some(expected_hash) = expected {
                    let start = index * chunk_size;
                    let end = std::cmp::min(start + chunk_size, file_len);

                    if let Some(bytes) = mmap.get(start..end) {
                        hasher.update_rayon(&bytes);

                        let mut hash = [0u8; 16];
                        hasher.finalize_xof().fill(&mut hash);
                        hasher.reset();

                        if hash != *expected_hash {
                            failed_chunks.push(index);
                        }
                    } else {
                        failed_chunks.push(index);
                    }
                }
            }
        }

        Ok(failed_chunks)
    }
}

pub struct VerifierHandle {
    sender: mpsc::Sender<VerifierMessage>
}

impl VerifierHandle {
    pub fn spawn(
        download_manager: UnboundedSender<ManagerCommand>,
        ui_sender: UnboundedSender<UiStateEvent>,
        db_manager: StateManager,
    )-> Self
    {
        let (sender, receiver) = mpsc::channel(1000);

        let verifier = Verifier::new(receiver, sender.clone(), download_manager, ui_sender, db_manager);

        tokio::spawn(async move {
            verifier.run().await;
        });

        Self {
            sender
        }
    }

    pub async fn verify_download(&self, download: Download) -> Result<(), SendError<VerifierMessage>> {
        self.sender.send(VerifierMessage::VerifyDownload(download)).await
    }

    pub async fn verify_downloads(&self, downloads: IndexMap<DownloadId, Download>) -> Result<(), SendError<VerifierMessage>> {
        self.sender.send(VerifierMessage::VerifyDownloads(downloads)).await
    }

    pub async fn cancel_verification(&self, download_id: DownloadId) -> Result<(), SendError<VerifierMessage>> {
        self.sender.send(VerifierMessage::CancelVerification(download_id)).await
    }

    pub async fn pause_verification(&self, download_id: DownloadId) -> Result<(), SendError<VerifierMessage>> {
        self.sender.send(VerifierMessage::PauseVerification(download_id)).await
    }
}
