use std::collections::{HashMap, VecDeque};
use std::fmt::{Debug, Display};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::http::HeaderValue;
use bincode::{Decode, Encode};
use bitvec::vec::BitVec;
use futures_util::{StreamExt, stream};
use indexmap::IndexMap;
use memmap2::MmapOptions;
use reqwest::header;
use serde::{Deserialize, Serialize, Serializer};
use thiserror::Error;
use tokio::fs::{create_dir_all, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{Mutex, Semaphore, broadcast, mpsc};
use tokio::task::JoinSet;
use xxhash_rust::xxh3::{Xxh3, xxh3_128_with_seed};

use crate::client_state_manager::{DownloadSnapshot, FrontendMessage, UiStateEvent, UiStateHandle, UiStateManager, get_snapshot};
use crate::download::hosts::{DownloadTask, FileTask, FolderTask, Host, HostParseError, TaskType};
use crate::network_manager::NetworkHandle;
use crate::state_manager::StateManager;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum DownloadUpdate {
    StatusChanged { id: DownloadId, status: DownloadStatus },
    FileUpdated { id: DownloadId, file_update: FileUpdate },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum FileUpdate {
    Status { id: usize, status: DownloadStatus },
    Hash { id: usize, hash: u128 },
    FileSize { id: usize, len: u64 },
    BytesDownloaded { id: usize, len: u64 },
}

impl FileUpdate {
    pub fn id(&self) -> usize {
        match self {
            FileUpdate::Status { id, .. } => *id,
            FileUpdate::Hash { id, .. } => *id,
            FileUpdate::FileSize { id, .. } => *id,
            FileUpdate::BytesDownloaded { id, .. } => *id,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum InternalFileUpdate {
    Status { id: usize, status: DownloadStatus },
    Hash { id: usize, hash: u128 },
    ChunkCompleted { id: usize, chunk_index: usize, len: u64 },
    FileSize { id: usize, len: u64 },
}

impl InternalFileUpdate {
        fn to_external(&self, download: &Download) -> Option<FileUpdate> {
        match self {
            InternalFileUpdate::ChunkCompleted { id, len: new_chunk_len, .. } => {
                let current_total  = match download.files().get(&id).unwrap() {
                    DownloadType::File(file_download) => file_download.bytes_downloaded(),
                    DownloadType::Folder(_folder_download) => todo!(),
                };

                // if should_send_progress_update(download, *id, progress) {
                //     Some(FileUpdate::Progress { id: *id, progress })
                // } else {
                //     None
                // }

                let new_total = current_total + new_chunk_len;

                Some(FileUpdate::BytesDownloaded { id: *id, len: new_total })
            },
            InternalFileUpdate::Status { id, status } => {
                Some(FileUpdate::Status { id: *id, status: *status })
            },
            InternalFileUpdate::Hash { id, hash } => {
                Some(FileUpdate::Hash { id: *id, hash: *hash })
            },
            InternalFileUpdate::FileSize { id, len } => {
                Some(FileUpdate::FileSize { id: *id, len: *len })
            },
        }
    }
}

// fn should_send_progress_update(download: &Download, id: usize, new_progress: f64) -> bool {
//         let last_progress = match download.files().get(&id).unwrap() {
//             DownloadType::File(file_download) => {
//                 file_download.get_progress_percent()
//             },
//             DownloadType::Folder(folder_download) => todo!(),
//         }
        
//         // get the last time we sent an update

        
//         // send update if:
//         // progress changed by more than 1%
//         let progress_changed = (new_progress - last_progress).abs() > 0.01;
        
//         // it's been more than 100ms since last update
//         // let time_elapsed = last_time
//         //     .map_or(true, |t| now.duration_since(*t).as_millis() > 100);
        
//         // always send new progress when completed
//         let completed = new_progress >= 100.0;
        
//         progress_changed || time_elapsed || completed
// }

#[derive(Debug, Clone, Copy, Hash, PartialEq, PartialOrd, Eq, Serialize, Deserialize, Encode, Decode, Ord)]
#[serde(transparent)]
pub struct DownloadId(pub usize);

impl Deref for DownloadId {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Display for DownloadId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, PartialOrd, Eq)]
pub enum DownloadReturnStatus {
    Completed,
    Canceled,
    Paused,
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, PartialOrd, Eq)]
pub enum DownloadCommand {
    Pause,
    Resume,
    Cancel,
}

enum ManagerCommand {
    QueueDownload(String),
    RemoveDownload(DownloadId),
    Shutdown,
}

#[derive(Debug, Error)]
pub enum DownloadManagerError {
    #[error(transparent)]
    Parse(#[from] HostParseError)
}

#[derive(Debug)]
pub struct DownloadManager {
    next_id: Option<AtomicUsize>,
    db_state_manager: StateManager,
    unprocessed_downloads: IndexMap<DownloadId, Download>,
    download_command_sender: Arc<Mutex<HashMap<DownloadId, tokio::sync::broadcast::Sender<DownloadCommand>>>>, 
    client: reqwest::Client,
    ui_state_handle: Option<UiStateHandle>,
    command_sender: Option<UnboundedSender<ManagerCommand>>,
    concurrency_limit: Arc<Semaphore>,
}

impl DownloadManager {
    pub fn new(db_state_manager: StateManager) -> Self {
        let client = reqwest::Client::builder()
            .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:141.0) Gecko/20100101 Firefox/141.0")
            .build().unwrap();

        DownloadManager {
            next_id: Some(AtomicUsize::new(0)), 
            db_state_manager,
            unprocessed_downloads: IndexMap::new(),
            download_command_sender: Arc::new(Mutex::new(HashMap::new())),
            client,
            ui_state_handle: None,
            command_sender: None,
            concurrency_limit: Arc::new(Semaphore::const_new(10))
        }
    }

    pub async fn load_state(&mut self) {
        let restored_downloads = self.db_state_manager.load_downloads().await.unwrap();

        let max_id = restored_downloads.keys().max().copied().unwrap_or(DownloadId(0));

        self.next_id.as_mut().unwrap().store(*max_id + 1, Ordering::Relaxed);

        println!("restored: {:#?}", restored_downloads);

        for (id, download) in restored_downloads {
            self.unprocessed_downloads.insert(id, download.clone());
        }
    }

    pub async fn verify_downloads(&mut self) {
        for (_, download) in &mut self.unprocessed_downloads {
            let mut fail = false;

            for (_, download_type) in &mut download.files {
                if (download_type.status() != DownloadStatus::Queued) && !download_type.relative_path().exists() {
                    download_type.set_status(DownloadStatus::NotFound);
                    fail = true;
                }

                if let DownloadType::File(file_download) = download_type {
                    if file_download.status() == DownloadStatus::Completed {
                        let hash = hash_file(file_download.relative_path().to_path_buf()).await;

                        if Some(hash) != file_download.hash {
                            file_download.status = DownloadStatus::Failed(DownloadFailureReason::HashMismatch);
                        }
                    }
                }
            }

            if fail {
                download.status = DownloadStatus::Failed(DownloadFailureReason::HashMismatch);
            }
        }

        println!("restored: {:#?}", self.unprocessed_downloads);
    }

    pub async fn queue_download(&mut self, url: String) -> Result<(), DownloadManagerError> {
        if let Some(sender) = &self.command_sender {
            sender.send(ManagerCommand::QueueDownload(url)).unwrap();
        }

        Ok(())
    }

    pub async fn remove_download(&mut self, id: DownloadId) {
        if let Some(sender) = &self.command_sender {
            let _ = sender.send(ManagerCommand::RemoveDownload(id));
        }
    }

    pub async fn start_processing(&mut self) {
        let (command_sender, mut command_receiver) = mpsc::unbounded_channel::<ManagerCommand>();

        self.command_sender = Some(command_sender.clone()); 

        let ui_state_manager = UiStateManager::new();
        let ui_state_handle = ui_state_manager.start();
        self.ui_state_handle = Some(ui_state_handle);

        
        // Clone shared resources
        let ui_event_sender = self.ui_state_handle.as_ref().unwrap().get_event_sender();
        let concurrency_limit = self.concurrency_limit.clone();
        let db_manager = self.db_state_manager.clone();
        let command_broadcast_map = self.download_command_sender.clone();

        let mut queue: IndexMap<DownloadId, Download> = self.unprocessed_downloads.drain(..).collect();

        let (network_manager, _) = NetworkHandle::spawn(ui_event_sender.clone(), db_manager.clone()).await;

        // Download registry for deduplication purposes
        let mut url_registry: HashMap<String, DownloadId> = HashMap::new();
        let mut id_registry: HashMap<DownloadId, String> = HashMap::new();

        let existing_downloads = db_manager.get_all_download_urls().await; 

        // Add existing downloads to registry
        for (id, url) in existing_downloads {
            url_registry.insert(url.clone(), DownloadId(id));
            id_registry.insert(DownloadId(id), url);
        }

        // Add queued downloads to registry
        for (id, download) in &queue {
            url_registry.insert(download.url().to_string(), *id);
            id_registry.insert(*id, download.url().to_string());
        }

        let next_id = self.next_id.take().unwrap();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(command) = command_receiver.recv() => {
                        match command {
                            ManagerCommand::QueueDownload(url) => {
                                println!("registry: {:#?}", url_registry);
                                println!("url: {}", url);
                                if url_registry.contains_key(&url) {
                                    println!("Download already exists: {}", url);
                                    continue; 
                                }

                                let id = DownloadId(next_id.fetch_add(1, Ordering::Relaxed));
                                url_registry.insert(url.clone(), id);
                                id_registry.insert(id, url.clone());

                                network_manager.queue_download(url, id);
                            },
                            ManagerCommand::RemoveDownload(id) => {
                                // First, remove from registry
                                if let Some(url) = id_registry.remove(&id) {
                                    url_registry.remove(&url);
                                }

                                 // Try to remove from Pending Queue
                                if queue.shift_remove(&id).is_some() {
                                    println!("Removed pending download {}", id);
                                    db_manager.delete_download(id).await;
                                } 
                                // If not in queue, it might be running. Send Cancel signal.
                                else if let Some(sender) = command_broadcast_map.lock().await.get(&id) {
                                    let _ = sender.send(DownloadCommand::Cancel);
                                }
                                // Else if it's already done or doesn't exist; just DB delete
                                else {
                                    println!("Removed completed download {}", id);
                                    db_manager.delete_download(id).await;
                                }
                                let _ = ui_event_sender.send(UiStateEvent::RemoveDownload(*id));
                            },
                            ManagerCommand::Shutdown => {
                                break;
                            },
                        }
                    }

                    Ok(permit) = concurrency_limit.clone().acquire_owned(), if !queue.is_empty() => {
                        if let Some((_, download)) = queue.shift_remove_index(0) {
                            let _ = command_sender.send(ManagerCommand::QueueDownload(download.url().to_string()));
                        }
                        // if let Some((_, download)) = queue.shift_remove_index(0) {
                        //     if download.is_completed() {
                        //         continue;
                        //     }

                        //     let client = client.clone();
                        //     let db = db_manager.clone();
                        //     let command_map = command_broadcast_map.clone();
                        //     let ui_event_sender = ui_event_sender.clone();

                        //     tokio::spawn(async move {
                        //         let _permit = permit; 
                        //         let download_id = DownloadId(download.id());

                        //         let (commands_sender, commands_receiver) = tokio::sync::broadcast::channel(20);
                        //         command_map.lock().await.insert(download_id, commands_sender);

                        //         let return_status = process_download(
                        //             db.clone(),
                        //             ui_event_sender,
                        //             commands_receiver,
                        //             client,
                        //             download
                        //         ).await;

                        //         if return_status == DownloadReturnStatus::Canceled {
                        //             db.delete_download(*download_id).await;
                        //         }

                        //         println!("download finished");

                        //         command_map.lock().await.remove(&download_id);
                        //     });
                        // }
                    }
                }
            }
        });
    }

    pub fn download_subscribe(&self) -> broadcast::Receiver<FrontendMessage> {
        self.ui_state_handle.as_ref().unwrap().subscribe()
    }

    pub async fn get_snapshot(&self) -> DownloadSnapshot {
        get_snapshot(&self.db_state_manager).await
    }
}

async fn process_download(state_manager: StateManager, ui_event_sender: mpsc::UnboundedSender<UiStateEvent>, commands_receiver: tokio::sync::broadcast::Receiver<DownloadCommand>, client: reqwest::Client, mut download: Download) -> DownloadReturnStatus {
    download.status = DownloadStatus::InProgress;
    let _ = ui_event_sender.send(UiStateEvent::AddUpdate(DownloadUpdate::StatusChanged { id: download.id(), status: DownloadStatus::InProgress }));

    let mut unprocessed_downloads = VecDeque::new();

    for (&id, download_type) in &download.files {
        if let DownloadType::File(file_download) = download_type {
            unprocessed_downloads.push_back((id, file_download.url.to_owned(), file_download.relative_path().to_owned()));
        }
    }

    let host = download.host;

    let (sender, mut receiver) = mpsc::unbounded_channel::<InternalFileUpdate>();

    let ui_event_sender_clone = ui_event_sender.clone();

    let handle = tokio::spawn(async move {
        let state_manager = state_manager;

        let mut download = download;
        let mut save_interval = tokio::time::interval(Duration::from_millis(100));

        loop {
            tokio::select! {
                update = receiver.recv() => {
                    match update {
                        Some(update) => {
                            if let Some(file_update) = update.to_external(&download) {
                                _ = ui_event_sender_clone.send(UiStateEvent::AddUpdate(DownloadUpdate::FileUpdated { id: download.id(), file_update }));
                            }

                            handle_download_update(&mut download, update).await;
                        }
                        None => break,
                    }
                }
                _ = save_interval.tick() => {
                    state_manager.write_download(&download).await;
                }
            }
        }

        state_manager.write_download(&download).await;
        (download, state_manager)
    });

    let sizes = fetch_download_size(unprocessed_downloads.clone(), client.clone(), host).await;

    println!("found size of download: {}", sizes.clone().into_iter().map(|result| result.unwrap().1).fold(0, |a, b| a + b));

    for result in sizes {
        if let Some((id, len)) = result {
            sender.send(InternalFileUpdate::FileSize { id, len }).unwrap();
        }
    }

    let mut file_download_handles = JoinSet::new();
    let file_concurrency_limit = Arc::new(Semaphore::new(5)); 


    for (id, url, path) in unprocessed_downloads {
        let client = client.clone();
        let commands_receiver = commands_receiver.resubscribe();
        let sender = sender.clone(); 
        let permit = file_concurrency_limit.clone().acquire_owned().await.unwrap();

        file_download_handles.spawn(async move {
            let _permit = permit; 

            sender.send(InternalFileUpdate::Status { id, status: DownloadStatus::InProgress }).unwrap();

            let return_status = download_file(id, &url, &path, host, &client, &sender, commands_receiver).await;

            if return_status == DownloadReturnStatus::Canceled {
                return DownloadReturnStatus::Canceled;
            }

            sender.send(InternalFileUpdate::Status { id, status: DownloadStatus::Completed }).unwrap();

            return_status
        });
    }

    drop(sender);

    let (mut download, state_manager) = handle.await.unwrap();

    while let Some(result) = file_download_handles.join_next().await {
        match result {
            Ok(status) => {
                if status == DownloadReturnStatus::Canceled {
                    file_download_handles.shutdown().await; 
                    state_manager.delete_download(download.id).await;

                    return DownloadReturnStatus::Canceled;
                }
            }
            Err(e) => {
                println!("Download task failed: {:?}", e);
            }
        }
    }
    
    download.status = DownloadStatus::Completed;
    let _ = ui_event_sender.send(UiStateEvent::AddUpdate(DownloadUpdate::StatusChanged { id: download.id(), status: DownloadStatus::Completed }));

    println!("{}", download.url());
    state_manager.write_download(&download).await;

    DownloadReturnStatus::Completed
}

async fn handle_download_update(download: &mut Download, update: InternalFileUpdate) {
    match update {
        InternalFileUpdate::Status { id, status } => {
            let file = download.get_file_mut(&id).unwrap();
            file.set_status(status);

            if file.status() == DownloadStatus::Completed {
                let hash = hash_file(file.relative_path().to_path_buf()).await;
                file.hash = Some(hash);

                if let Some(parent_id) = file.parent_id() {
                    download.try_complete_folder(parent_id);
                }
            }
        },
        InternalFileUpdate::Hash { id, hash } => {
            let file = download.get_file_mut(&id).unwrap();
            file.hash = Some(hash);
        },
        InternalFileUpdate::ChunkCompleted { id, chunk_index, len } => {
            let file = download.get_file_mut(&id).unwrap();
            file.bytes_downloaded += len;

            if chunk_index >= file.chunks.len() {
                file.chunks.resize(chunk_index + 1, false);
                eprintln!("TODO: file size should probably be recalculated here too");
            }

            file.chunks.set(chunk_index, true);
        }
        InternalFileUpdate::FileSize { id, len } => {
            let file = download.get_file_mut(&id).unwrap();
            let chunk_size = len.div_ceil(16384) as usize;

            if file.chunks.len() != chunk_size {
                file.chunks.resize(chunk_size, false);
            }

            // if let FileSize::Known(size) = &mut file.size && *size != len as u64 {
            //     *size = len as u64;
            // } else {
            //     file.size = FileSize::Known(len as u64);
            // }
        }
    };
}

async fn fetch_download_size(unprocessed_downloads: VecDeque<(usize, String, PathBuf)>, client: reqwest::Client, host: Host) -> Vec<Option<(usize, u64)>>{
    let stream = stream::iter(unprocessed_downloads);

    let results = stream.map(|(id, url, _)| {
        let client = client.clone();

        async move {
            fetch_file_size(host, &client, &url).await.map(|len| (id, len))
        }
    }).buffer_unordered(4);

    let sizes = results.collect().await;

    sizes
}

async fn fetch_file_size(host: Host, client: &reqwest::Client, url: &str) -> Option<u64> {
    // Try a HEAD request first
    let head_result = client.head(url)
        .headers(host.headers())
        .header("Accept-Encoding", "identity")
        .send()
        .await;

    if let Ok(response) = head_result {
        if let Some(len) = response.content_length() && response.status().is_success() {
            if len > 0 {
                return Some(len);
            }
        }
    }

    // If HEAD fails or returns no length, do a GET request and abort immediately to avoid downloading body
    let get_result = client.get(url)
        .headers(host.headers())
        .header("Accept-Encoding", "identity")
        .header("Range", "bytes=0-0")
        .send()
        .await;

        if let Ok(resp) = get_result {
            if let Some(range_header) = resp.headers().get(header::CONTENT_RANGE) {
                return Some(parse_content_range_size(range_header)); 
            }
            if let Some(len) = resp.content_length() {
                return Some(len);
            }
        }

    None
}

fn parse_content_range_size(range_header: &HeaderValue) -> u64 {
     // e.g. "bytes 0-0/1048576"
    u64::from_str_radix(range_header.to_str().unwrap().rsplit_once("/").unwrap().1, 10).unwrap()
}

async fn download_file(id: usize, url: &str, path: &Path, host: Host, client: &reqwest::Client, sender: &UnboundedSender<InternalFileUpdate>, mut commands_receiver: tokio::sync::broadcast::Receiver<DownloadCommand>) -> DownloadReturnStatus {
    let mut response = client.get(url)
        .headers(host.headers())
        .send()
        .await.unwrap();

    if let Some(file_size) = response.content_length() {
        sender.send(InternalFileUpdate::FileSize { id, len: file_size }).unwrap();
    }

    println!("{}", path.to_str().unwrap());

    if let Some(parent_path) = path.parent() {
        create_dir_all(parent_path).await.unwrap();
    }

    let mut file = tokio::fs::File::create(&path).await.unwrap();
    
    println!("url: {}", url);

    let chunk_size = 16384; // 16 KB
    let mut buffer = Vec::with_capacity(chunk_size * 2); // * 2 to prevent too many reallocations
    let mut current_chunk = 0;
    
    loop {
        tokio::select! {
            chunk_result = response.chunk() => {
                if let Ok(Some(chunk)) = chunk_result {
                    buffer.extend_from_slice(&chunk);

                    while buffer.len() >= chunk_size {
                        file.write_all(&buffer[..chunk_size]).await.unwrap();

                        sender.send(InternalFileUpdate::ChunkCompleted { id, chunk_index: current_chunk, len: chunk_size as u64 }).unwrap();
                        current_chunk += 1;

                        buffer.copy_within(chunk_size.., 0);
                        buffer.truncate(buffer.len() - chunk_size);
                    }
                } else {
                    break;
                }
            }
            command = commands_receiver.recv() => {
                println!("received command");
                match command {
                    Ok(DownloadCommand::Cancel) => return DownloadReturnStatus::Canceled,
                    Ok(DownloadCommand::Pause) => {
                        println!("download {} should be paused!", id);
                    }
                    _ => {}
                }
            }
        }
    }

    // handle remaining bytes (final chunk)
    if !buffer.is_empty() {
        let len = buffer.len() as u64;

        file.write_all(&buffer).await.unwrap();
        sender.send(InternalFileUpdate::ChunkCompleted { id, chunk_index: current_chunk, len }).unwrap();
    }

    file.sync_all().await.unwrap();

    DownloadReturnStatus::Completed
}

async fn hash_file(path: PathBuf) -> u128 {
    tokio::task::spawn_blocking(move || {
        let file = std::fs::File::open(&path).expect("Failed to open file for hashing");
        
        let mmap = unsafe { MmapOptions::new().map(&file).expect("Failed to mmap file") };

        xxh3_128_with_seed(&mmap, 0)
    }).await.expect("Hashing task panicked")
}

#[derive(Debug, Clone, Copy, Encode, Decode, Serialize, Deserialize, PartialEq, Eq)]
pub enum DownloadStatus {
    Queued,
    Initializing,
    InProgress,
    Completed,
    Paused,
    Failed(DownloadFailureReason),
    NotFound,
}

#[derive(Debug, Clone, Copy, Encode, Decode, Serialize, Deserialize, PartialEq, Eq)]
pub enum DownloadFailureReason {
    HashMismatch,
}

/// Has either a file or folder as the only item in root
#[derive(Debug, Encode, Decode, Serialize, Deserialize, Clone)]
pub struct Download {
    id: DownloadId,
    url: String,
    relative_path: PathBuf,
    pub(crate) host: Host,
    status: DownloadStatus,
    pub(crate) files: HashMap<usize, DownloadType>,
    name: String,
}

impl Download {
    pub const fn url(&self) -> &String {
        &self.url
    }

    pub fn get_file_mut(&mut self, id: &usize) -> Result<&mut FileDownload, ()> {
        match self.files.get_mut(id) {
            Some(DownloadType::File(file)) => Ok(file),
            Some(DownloadType::Folder(_)) => Err(()),
            None => Err(()),
        }
    }

    pub const fn id(&self) -> DownloadId {
        self.id
    }

    pub const fn files(&self) -> &HashMap<usize, DownloadType> {
        &self.files
    }

    pub const fn files_mut(&mut self) -> &mut HashMap<usize, DownloadType> {
        &mut self.files
    }

    pub const fn relative_path(&self) -> &PathBuf {
        &self.relative_path
    }

    pub const fn host(&self) -> Host {
        self.host
    }

    pub const fn status(&self) -> DownloadStatus {
        self.status
    }

    pub const fn name(&self) -> &String {
        &self.name
    }

    pub fn is_completed(&self) -> bool {
        self.status == DownloadStatus::Completed
    }

    pub fn set_status(&mut self, status: DownloadStatus) {
        self.status = status;
    }
}

impl Download {
    pub fn new(id: usize, value: DownloadTask) -> Self {
        let mut relative_path = PathBuf::new();

        let mut files = HashMap::new();
        let mut current_id = 0;
        let name;

        match value.task_type {
            TaskType::File(file_task) => {
                name = file_task.file_name().clone();
                files.insert(current_id, DownloadType::File(FileDownload::new(&file_task, &relative_path, current_id, None)));
            },
            TaskType::Folder(folder_task) => {
                name = folder_task.folder_name().clone();
                Self::process_folder_creation(&folder_task, &mut relative_path, &mut current_id, &mut files, None);
            },
        }

        Self { 
            id: DownloadId(id),
            url: value.url,
            relative_path: PathBuf::from("./"),
            host: Host::example_host,
            status: DownloadStatus::Queued,
            files: files,
            name,
        }
    }

    fn process_folder_creation(folder_task: &FolderTask, parent_relative_path: &Path, current_id: &mut usize, files: &mut HashMap<usize, DownloadType>, parent_id: Option<usize>) {
        let mut children = Vec::new();
        let mut relative_path = parent_relative_path.join(&folder_task.folder_name());

        let folder_id = *current_id;
        *current_id += 1;

        for file_type in &folder_task.files {
            match file_type {
                TaskType::File(file_task) => {
                    files.insert(*current_id, DownloadType::File(FileDownload::new(&file_task, &relative_path, *current_id, Some(folder_id))));
                    children.push(*current_id);
                    *current_id += 1;
                },
                TaskType::Folder(folder_task) => {
                    Self::process_folder_creation(folder_task, &mut relative_path, current_id, files, Some(folder_id));
                },
            }
        }

        files.insert(folder_id, DownloadType::Folder(FolderDownload::new(&folder_task, &parent_relative_path, folder_id, children, parent_id)));
    }

    fn try_complete_folder(&mut self, folder_id: usize) {
        let folder = self.files.get_mut(&folder_id).unwrap();
        let children_number;
        let mut children_completed = 0;
        let children;

        match folder {
            DownloadType::File(_) => unreachable!("A file can't have a file as its parent."),
            DownloadType::Folder(folder_download) => {
                children_number = folder_download.children.len();
                children = folder_download.children.clone();
            },
        }

        for child in &children {
            if self.files.get(&child).unwrap().status() == DownloadStatus::Completed {
                children_completed += 1;
            }
        }

        let folder = self.files.get_mut(&folder_id).unwrap();

        let mut completed = false;
        let parent_id;

        match folder {
            DownloadType::File(_) => unreachable!("A file can't have a file as its parent."),
            DownloadType::Folder(folder_download) => {
                parent_id = folder_download.parent_id;

                if children_completed == children_number {
                    folder_download.status = DownloadStatus::Completed;
                    completed = true;
                }
            },
        }

        if completed {
            if let Some(parent_id) = parent_id {
                self.try_complete_folder(parent_id);
            }
        }
    }
}

#[derive(Debug, Encode, Decode, Serialize, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum DownloadType {
    File(FileDownload),
    Folder(FolderDownload),
}

impl DownloadItem for DownloadType {
    fn parent_id(&self) -> Option<usize> {
        match self {
            DownloadType::File(f) => f.parent_id(),
            DownloadType::Folder(f) => f.parent_id(),
        }
    }

    fn id(&self) -> usize {
        match self {
            DownloadType::File(f) => f.id(),
            DownloadType::Folder(f) => f.id(),
        }
    }

    fn relative_path(&self) -> &PathBuf {
        match self {
            DownloadType::File(f) => f.relative_path(),
            DownloadType::Folder(f) => f.relative_path(),
        }
    }

    fn status(&self) -> DownloadStatus {
        match self {
            DownloadType::File(f) => f.status(),
            DownloadType::Folder(f) => f.status(),
        }
    }

    fn set_status(&mut self, status: DownloadStatus) {
        match self {
            DownloadType::File(f) => f.set_status(status),
            DownloadType::Folder(f) => f.set_status(status),
        }
    }

    fn name(&self) -> &str {
        match self {
            DownloadType::File(f) => f.name(),
            DownloadType::Folder(f) => f.name(),
        }
    }
}

pub trait DownloadItem {
    fn parent_id(&self) -> Option<usize>;

    fn id(&self) -> usize;

    fn relative_path(&self) -> &PathBuf;

    fn status(&self) -> DownloadStatus;

    fn set_status(&mut self, status: DownloadStatus);

    fn name(&self) -> &str;
}

#[derive(Encode, Decode, Serialize, Deserialize, Clone)]
pub struct FileDownload {
    parent_id: Option<usize>,
    id: usize,
    url: String,
    file_name: String,
    relative_path: PathBuf,
    status: DownloadStatus,
    #[serde(serialize_with = "serialize_hash")] 
    hash: Option<u128>,
    #[serde(serialize_with = "serialize_chunks")]
    #[bincode(with_serde)]
    chunks: BitVec,
    size: Option<FileSize>, // None means we haven't gotten the size yet, unknown means the size can't be known until it
    bytes_downloaded: u64,
}

fn serialize_hash<S>(hash: &Option<u128>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if serializer.is_human_readable() {
        match hash {
            Some(v) => serializer.collect_str(v),
            None => serializer.serialize_none(),
        }
    } else {
        hash.serialize(serializer)
    }
}

fn serialize_chunks<S>(chunks: &BitVec, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if serializer.is_human_readable() {
        serializer.serialize_none()
    } else {
        chunks.serialize(serializer)
    }
}


impl DownloadItem for FileDownload {
    fn parent_id(&self) -> Option<usize> {
        self.parent_id
    }

    fn id(&self) -> usize {
        self.id
    }

    fn relative_path(&self) -> &PathBuf {
        &self.relative_path
    }

    fn status(&self) -> DownloadStatus {
        self.status
    }

    fn set_status(&mut self, status: DownloadStatus) {
        self.status = status;
    }

    fn name(&self) -> &str {
        &self.file_name
    }
}

impl Debug for FileDownload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileDownload")
            .field("id", &self.id)
            .field("url", &self.url)
            .field("file_name", &self.file_name)
            .field("relative_path", &self.relative_path)
            .field("status", &self.status)
            .field("hash", &self.hash)
            .field("chunks", &self.chunks.len())
            .finish()
    }
}

impl FileDownload {
    pub(super) fn new(file_task: &FileTask, relative_path: &Path, id: usize, parent_id: Option<usize>) -> Self {
        let relative_path = relative_path.join(&file_task.file_name());

        Self { 
            parent_id,
            id,
            url: file_task.url.to_owned(),
            file_name: file_task.file_name().to_owned(),
            relative_path,
            status: DownloadStatus::Queued,
            hash: None,
            chunks: BitVec::new(),
            size: None,
            bytes_downloaded: 0,
        }
    }

    pub const fn chunks(&self) -> &BitVec {
        &self.chunks
    }

    pub fn chunks_mut(&mut self) -> &mut BitVec {
        &mut self.chunks
    }

    pub const fn hash(&self) -> Option<u128> {
        self.hash
    }

    pub const fn url(&self) -> &String {
        &self.url
    }

    pub fn size(&self) -> Option<FileSize> {
        self.size
    }

    pub fn set_size(&mut self, size: FileSize) {
        self.size = Some(size);
    }

    pub fn bytes_downloaded(&self) -> u64 {
        self.bytes_downloaded
    }

    pub fn set_bytes_downloaded(&mut self, bytes_downloaded: u64) {
        self.bytes_downloaded = bytes_downloaded
    }
}

#[derive(Debug, Encode, Decode, Serialize, Deserialize, Clone)]
pub struct FolderDownload {
    parent_id: Option<usize>,
    id: usize,
    folder_name: String,
    relative_path: PathBuf,
    status: DownloadStatus,
    children: Vec<usize>,
}

impl FolderDownload {
    pub(super) fn new(folder_task: &FolderTask, parent_relative_path: &Path, id: usize, children: Vec<usize>, parent_id: Option<usize>) -> Self {
        let relative_path = parent_relative_path.join(&folder_task.folder_name());

        Self { 
            parent_id,
            id,
            folder_name: folder_task.folder_name().to_owned(),
            relative_path,
            status: DownloadStatus::Queued,
            children
        }
    }

    pub const fn children(&self) -> &Vec<usize> {
        &self.children
    }
}

impl DownloadItem for FolderDownload {
    fn parent_id(&self) -> Option<usize> {
        self.parent_id
    }

    fn id(&self) -> usize {
        self.id
    }

    fn relative_path(&self) -> &PathBuf {
        &self.relative_path
    }

    fn status(&self) -> DownloadStatus {
        self.status
    }

    fn set_status(&mut self, status: DownloadStatus) {
        self.status = status;
    }

    fn name(&self) -> &str {
        &self.folder_name
    }
}

#[derive(Debug, Encode, Decode, Copy, Clone, Deserialize, PartialEq, Eq)]
pub enum FileSize {
    Unknown,
    Known(u64)
}

impl Serialize for FileSize {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer {
        match self {
            FileSize::Unknown => "unknown".serialize(serializer),
            FileSize::Known(size) => size.serialize(serializer),
        }
    }
}