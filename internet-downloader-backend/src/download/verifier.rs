use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

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
    CancelVerification(DownloadId),
    PauseVerification(DownloadId),
}

struct Verifier {
    receiver: mpsc::Receiver<VerifierMessage>,
    download_manager: UnboundedSender<ManagerCommand>,
    ui_sender: UnboundedSender<UiStateEvent>,
    db_manager: StateManager,
    handles: HashMap<DownloadId, JoinHandle<()>>, // We save the original file statuses in case we need them
    semaphore: Arc<Semaphore>,
}

impl Verifier {
    fn new(
        receiver: mpsc::Receiver<VerifierMessage>,
        download_manager: UnboundedSender<ManagerCommand>,
        ui_sender: UnboundedSender<UiStateEvent>,
        db_manager: StateManager,
    ) -> Self
    {
        Self {
            receiver,
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
                    if let Some(handle) = self.handles.remove(&download_id) {
                        handle.abort();

                        // We want to wait until the handle ends before sending the clean up message
                        let _ = handle.await; 
                    }

                    let _ = self.download_manager.send(ManagerCommand::CleanUpDownload(download_id));
                },
                VerifierMessage::PauseVerification(download_id) => {
                    if let Some(handle) = self.handles.remove(&download_id) {
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

        let handle = tokio::spawn(async move {
            let start_time = std::time::Instant::now();
            info!("Started verifying {}.", download.name());

            let _permit = semaphore.acquire_owned().await.unwrap();

            let diffs = Self::verify_download(&download).await;

            for diff in diffs {
                let file_id = diff.file_id;

                if let Some(new_status) = diff.new_status {
                    if let Some(changed_items) = download.set_file_status(file_id, new_status) {
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
                    if let Some(file) = download.get_file_mut(&file_id) {
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

        self.handles.insert(download_id, handle);
    }

    async fn verify_download(download: &Download) -> Vec<FileVerificationDiff> {
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

                    let hash = tokio::task::spawn_blocking(move || {
                        hash_file(&path)
                    }).await.unwrap();

                    if Some(hash) != file.hash() {
                        new_status = Some(FileStatus::Failed(FileFailureReason::HashMismatch));
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
                            let failed_indices = tokio::task::spawn_blocking(move || {
                                Self::verify_file_chunks(&path, HASH_CHUNK_SIZE, size as usize, chunks_to_check)
                            }).await.unwrap();

                            if !failed_indices.is_empty() {
                                failed_chunks = Some(failed_indices);
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

    fn verify_file_chunks(path: &Path, chunk_size: usize, file_len: usize, chunks_to_check: Vec<Option<[u8; 16]>>) -> Vec<usize> {
        let mut failed_chunks = Vec::new();

        let mut file = File::open(&path).expect("Failed to open file");
        let mut hasher = blake3::Hasher::new();

        // If the file is tiny, skip the mmap overhead and just read it.
        if file_len < 16 * 1024 {
            let mut buffer = vec![0u8; file_len];

            file.seek(SeekFrom::Start(0)).expect("Failed to seek");
            file.read_exact(&mut buffer).expect("Failed to read");

            for (index, expected) in chunks_to_check.iter().enumerate() {
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
                    .map(&file)
                    .expect("Failed to map file chunk") 
            };

            for (index, expected) in chunks_to_check.iter().enumerate() {
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

        failed_chunks
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

        let verifier = Verifier::new(receiver, download_manager, ui_sender, db_manager);

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