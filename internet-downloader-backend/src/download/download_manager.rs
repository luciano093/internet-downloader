use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bincode::{Decode, Encode};
use bitvec::vec::BitVec;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize, Serializer};
use thiserror::Error;
use tokio::fs::{create_dir_all, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{Mutex, broadcast, mpsc};
use url::{ParseError, Url};
use xxhash_rust::xxh3::Xxh3;

use crate::client_state_manager::{get_snapshot, DownloadDeltaMap, DownloadSnapshot, UiStateEvent, UiStateHandle, UiStateManager};
use crate::download::hosts::Host;
use crate::state_manager::StateManager;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum DownloadUpdate {
    StatusChanged { id: usize, status: DownloadStatus },
    FileUpdated { id: usize, file_update: FileUpdate },
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

#[derive(Debug, Clone, Copy, Hash, PartialEq, PartialOrd, Eq)]
pub struct DownloadId(usize);

impl Deref for DownloadId {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        &self.0
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

#[derive(Debug, Error)]
pub enum DownloadManagerError {
    #[error(transparent)]
    Parse(#[from] HostParseError)
}

#[derive(Debug)]
pub struct DownloadManager {
    next_id: AtomicUsize,
    db_state_manager: StateManager,
    unprocessed_downloads: IndexMap<usize, Download>,
    unprocessed_downloads_url_index: HashMap<String, usize>,
    task_sender: Option<mpsc::UnboundedSender<Download>>,
    download_command_sender: Arc<Mutex<HashMap<DownloadId, tokio::sync::broadcast::Sender<DownloadCommand>>>>, 
    client: reqwest::Client,
    ui_state_handle: Option<UiStateHandle>,
}

impl DownloadManager {
    pub fn new(db_state_manager: StateManager) -> Self {
        let client = reqwest::Client::builder()
            .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:141.0) Gecko/20100101 Firefox/141.0")
            .build().unwrap();

        DownloadManager {
            next_id: AtomicUsize::new(0), 
            db_state_manager,
            unprocessed_downloads: IndexMap::new(),
            unprocessed_downloads_url_index: HashMap::new(),
            task_sender: None,
            download_command_sender: Arc::new(Mutex::new(HashMap::new())),
            client,
            ui_state_handle: None,
        }
    }

    pub async fn load_state(&mut self) {
        let restored_downloads = self.db_state_manager.load_downloads().await.unwrap();

        let max_id = restored_downloads.keys().max().copied().unwrap_or(0);

        self.next_id.store(max_id + 1, Ordering::Relaxed);

        println!("restored: {:#?}", restored_downloads);

        for (id, download) in restored_downloads {
            // Add to main store
            self.unprocessed_downloads.insert(id, download.clone());
            
            // Add to lookup index 
            self.unprocessed_downloads_url_index.insert(download.url().to_string(), id);
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
                        let hash = hash_file(file_download.relative_path()).await;

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

    pub async fn add_download(&mut self, url: &str) -> Result<(), DownloadManagerError> {
        if self.unprocessed_downloads_url_index.contains_key(url) {
            println!("download already in index");
            return Ok(())
        }

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let host = parse_host(url)?;

        let download_task = host.extract_download_info(url).await;

        let download = Download::new(id, download_task);

        self.ui_state_handle.as_ref().unwrap().add_download(download.clone());

        if let Some(sender) = &self.task_sender {
            sender.send(download).unwrap();
        }

        Ok(())
    }

    pub async fn remove_download(&mut self, id: usize) {
        // If the download is still unprocessed, remove it from the queue
        if let Some(download) = self.unprocessed_downloads.swap_remove(&id) {
            self.unprocessed_downloads_url_index.remove(download.url());
        }

        // If the download is in progress, send a message to cancel it
        match self.download_command_sender.lock().await.get(&DownloadId(id)) {
            Some(sender) => {
                let _ = sender.send(DownloadCommand::Cancel);
                self.db_state_manager.delete_download(id).await;
            }
            None => {
                // Otherwise if the download is already completed, just remove it from the db
                self.db_state_manager.delete_download(id).await;
            },
        };
    }

    pub async fn start_processing(&mut self) {
        let ui_state_manager = UiStateManager::new();
        let ui_state_handle = ui_state_manager.start();
        self.ui_state_handle = Some(ui_state_handle);

        let (sender, mut receiver) = mpsc::unbounded_channel();
        
        while let Some((_, download)) = self.unprocessed_downloads.pop() {
            self.unprocessed_downloads_url_index.remove(download.url());
            println!("{:#?}", self.unprocessed_downloads_url_index);

            if download.status == DownloadStatus::Completed {
                println!("Found completed download: {:?}", download.url());
                continue;
            }

            sender.send(download).unwrap();
        }

        self.task_sender = Some(sender);

        let db_state_manager = self.db_state_manager.clone();
        let client = self.client.clone();
        let download_command_sender = self.download_command_sender.clone();

        let ui_event_sender = self.ui_state_handle.as_ref().unwrap().get_event_sender();
        
        tokio::spawn(async move {
            while let Some(download) = receiver.recv().await {
                let ui_event_sender = ui_event_sender.clone();
                let client = client.clone();
                let db_state_manager = db_state_manager.clone();

                let (commands_sender, commands_receiver) = tokio::sync::broadcast::channel(20);
                
                let _ = download_command_sender.lock().await.insert(DownloadId(download.id()), commands_sender);

                tokio::spawn(async move {
                    process_download(
                        db_state_manager,
                        ui_event_sender,
                        commands_receiver,
                        client,
                        download
                    ).await;
                });
            }
        });
    }

    pub fn download_subscribe(&self) -> broadcast::Receiver<DownloadDeltaMap> {
        self.ui_state_handle.as_ref().unwrap().subscribe()
    }

    pub async fn get_snapshot(&self) -> DownloadSnapshot {
        get_snapshot(&self.db_state_manager).await
    }
}

async fn process_download(state_manager: StateManager, ui_event_sender: mpsc::UnboundedSender<UiStateEvent>, commands_receiver: tokio::sync::broadcast::Receiver<DownloadCommand>
, client: reqwest::Client, mut download: Download) {
    download.status = DownloadStatus::InProgress;

    let mut unprocessed_downloads = VecDeque::new();

    for (&id, download_type) in &download.files {
        if let DownloadType::File(file_download) = download_type {
            unprocessed_downloads.push_back((id, file_download.url.to_owned(), file_download.relative_path().to_owned()));
        }
    }

    let host = download.host;

    let (sender, mut receiver) = mpsc::unbounded_channel::<InternalFileUpdate>();

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
                                _ = ui_event_sender.send(UiStateEvent::AddUpdate(DownloadUpdate::FileUpdated { id: download.id(), file_update }));
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

    for (id, url, path) in unprocessed_downloads {
        let commands_receiver = commands_receiver.resubscribe();

        sender.send(InternalFileUpdate::Status { id, status: DownloadStatus::InProgress }).unwrap();

        let return_status = download_file(id, &url, &path, host, &client, &sender, commands_receiver).await;

        if return_status == DownloadReturnStatus::Canceled {
            drop(sender);
            handle.await.unwrap();
            return;
        }

        sender.send(InternalFileUpdate::Status { id, status: DownloadStatus::Completed }).unwrap();
    }

    drop(sender);

    let (mut download, state_manager) = handle.await.unwrap();
    
    download.status = DownloadStatus::Completed;

    println!("{}", download.url());
    state_manager.write_download(&download).await;
}

async fn handle_download_update(download: &mut Download, update: InternalFileUpdate) {
    match update {
        InternalFileUpdate::Status { id, status } => {
            let file = download.get_file_mut(&id).unwrap();
            file.set_status(status);

            if file.status() == DownloadStatus::Completed {
                let hash = hash_file(file.relative_path()).await;
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

            if let FileSize::Known(size) = &mut file.size && *size != len as u64 {
                *size = len as u64;
            } else {
                file.size = FileSize::Known(len as u64);
            }
        }
    };
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

async fn hash_file(path: &Path) -> u128 {
    let mut file = File::open(path).await.unwrap();
    let mut hasher = Xxh3::with_seed(0);
    let mut buffer = vec![0u8; 8192]; // 8KB chunks

    loop {
        let bytes_read = file.read(&mut buffer).await.unwrap();
        if bytes_read == 0 { break; }

        hasher.update(&buffer[..bytes_read]);
    }

    hasher.digest128()
}

fn parse_host(url: &str) -> Result<Host, HostParseError> {
    let url = Url::parse(url)?;
    let host = url.host_str().ok_or(HostParseError::NoHost)?;

    match host {
        "example.com" => Ok(Host::example_host),
        _ => Err(HostParseError::UnknownHost(host.to_string())),
    }
}

#[derive(Debug, Error)]
pub enum HostParseError {
    #[error("Invalid URL: {0}")]
    InvalidUrl(#[from] ParseError),
     #[error("Url contains no host")]
    NoHost,
    #[error("Unknown host: {0}")]
    UnknownHost(String)
}

#[derive(Debug)]
pub(in crate::download) struct DownloadTask {
    url: String,
    task_type: TaskType,
    host: Host,
}

impl DownloadTask {
    pub fn new(url: String, task_type: TaskType, host: Host) -> Self {
        Self {
            url,
            task_type,
            host
        }
    }
}

#[derive(Debug)]
pub(super) enum TaskType {
    File(FileTask),
    Folder(FolderTask),
}

#[derive(Debug)]
pub(super) struct FileTask {
    url: String,
    file_name: String,
}

impl FileTask {
    pub fn new(url: impl Into<String>, file_name: String) -> Self {
        Self { 
            url: url.into(),
            file_name,
        }
    }

    pub const fn file_name(&self) -> &String {
        &self.file_name
    }
}

#[derive(Debug)]
pub(super) struct FolderTask {
    folder_name: String,
    files: Vec<TaskType>
}

impl FolderTask {
    pub fn new(folder_name: String, files: Vec<TaskType>) -> Self {
        Self { folder_name, files }
    }

    pub const fn folder_name(&self) -> &String {
        &self.folder_name
    }
}

#[derive(Debug, Clone, Copy, Encode, Decode, Serialize, Deserialize, PartialEq, Eq)]
pub enum DownloadStatus {
    Queued,
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
    id: usize,
    url: String,
    relative_path: PathBuf,
    host: Host,
    status: DownloadStatus,
    files: HashMap<usize, DownloadType>,
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

    pub const fn id(&self) -> usize {
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
}

impl Download {
    fn new(id: usize, value: DownloadTask) -> Self {
        let mut relative_path = PathBuf::new();

        let mut files = HashMap::new();
        let mut current_id = 0;
        let name;

        match value.task_type {
            TaskType::File(file_task) => {
                name = file_task.file_name.clone();
                files.insert(current_id, DownloadType::File(FileDownload::new(&file_task, &relative_path, current_id, None)));
            },
            TaskType::Folder(folder_task) => {
                name = folder_task.folder_name.clone();
                Self::process_folder_creation(&folder_task, &mut relative_path, &mut current_id, &mut files, None);
            },
        }

        Self { 
            id,
            url: value.url,
            relative_path: PathBuf::from("./"),
            host: value.host,
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
    size: FileSize,
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
            size: FileSize::Unknown,
            bytes_downloaded: 0,
        }
    }

    pub const fn chunks(&self) -> &BitVec {
        &self.chunks
    }

    pub const fn hash(&self) -> Option<u128> {
        self.hash
    }

    pub const fn url(&self) -> &String {
        &self.url
    }

    pub fn size(&self) -> FileSize {
        self.size
    }

    pub fn bytes_downloaded(&self) -> u64 {
        self.bytes_downloaded
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

#[derive(Debug, Encode, Decode, Copy, Clone, Deserialize)]
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