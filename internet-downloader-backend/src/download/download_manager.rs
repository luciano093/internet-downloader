use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::time::Duration;

use bincode::{Decode, Encode};
use bitvec::vec::BitVec;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::fs::{create_dir_all, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{broadcast, mpsc};
use url::{ParseError, Url};
use xxhash_rust::xxh3::Xxh3;

use crate::download::hosts::Host;
use crate::state_manager::StateManager;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum DownloadUpdate {
    Status { id: usize, status: DownloadStatus },
    Hash { id: usize, hash: u128 },
    ChunkCompleted { id: usize, chunk_index: usize },
    FileSize { id: usize, len: usize },
}

#[derive(Debug, Error)]
pub enum DownloadManagerError {
    #[error(transparent)]
    Parse(#[from] HostParseError)
}

#[derive(Debug)]
pub struct DownloadManager {
    state_manager: StateManager,
    download_queue: IndexMap<String, Download>,
    task_sender: Option<mpsc::UnboundedSender<Download>>,
    update_sender: broadcast::Sender<DownloadUpdate>,
    client: reqwest::Client,
}

impl DownloadManager {
    pub fn new(state_manager: StateManager) -> Self {
        let client = reqwest::Client::builder()
            .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:141.0) Gecko/20100101 Firefox/141.0")
            .build().unwrap();

        let update_sender = broadcast::Sender::new(16);

        DownloadManager {
            state_manager,
            download_queue: IndexMap::new(),
            task_sender: None,
            update_sender,
            client,
        }
    }

    pub async fn load_state(&mut self) {
        let mut restored_downloads = self.state_manager.load_downloads().await.unwrap();

        println!("restored: {:#?}", restored_downloads);

        self.download_queue.append(&mut restored_downloads);
    }

    pub async fn add_download(&mut self, url: &str) -> Result<(), DownloadManagerError> {
        // self.update_sender.send(DownloadUpdate { url: "test".to_owned() }).unwrap();

        let host = parse_host(url)?;

        let download_task = host.extract_download_info(url).await;

        let download = Download::new(download_task);

        if let Some(sender) = &self.task_sender {
            sender.send(download).unwrap();
        } else {
            if !self.download_queue.contains_key(download.url()) {
                self.download_queue.insert(download.url().to_string(), download);
            }
        }

        Ok(())
    }

    pub async fn start_processing(&mut self) {
        let (sender, mut receiver) = mpsc::unbounded_channel();

        while let Some((_, download)) = self.download_queue.pop() {
            if matches!(download.status, DownloadStatus::Completed) {
                println!("Found completed download: {:?}", download.url());
                continue;
            }

            sender.send(download).unwrap();
        }

        self.task_sender = Some(sender);

        let state_manager = self.state_manager.clone();
        let client = self.client.clone();

        tokio::spawn(async move {
            while let Some(download) = receiver.recv().await {
                process_download(state_manager.clone(), client.clone(), download).await;
            }
        });
    }

    pub fn download_subscribe(&self) -> broadcast::Receiver<DownloadUpdate> {
        self.update_sender.subscribe()
    }
}

async fn process_download(state_manager: StateManager, client: reqwest::Client, mut download: Download) {
    download.status = DownloadStatus::InProgress;

    let mut download_queue = VecDeque::new();

    for (&id, download_type) in &download.files {
        if let DownloadType::File(file_download) = download_type {
            download_queue.push_back((id, file_download.url.to_owned(), file_download.relative_path().to_owned()));
        }
    }

    let host = download.host;

    let (sender, mut receiver) = mpsc::unbounded_channel::<DownloadUpdate>();

    let handle = tokio::spawn(async move {
        let state_manager = state_manager;
        let mut download = download;
        let mut save_interval = tokio::time::interval(Duration::from_millis(100));

        loop {
            tokio::select! {
                update = receiver.recv() => {
                    match update {
                        Some(DownloadUpdate::Status { id, status }) => {
                            if let DownloadType::File(file_download) = download.files.get_mut(&id).unwrap() {
                                file_download.status = status;

                                if matches!(file_download.status, DownloadStatus::Completed) {
                                    let hash = hash_file(file_download.relative_path()).await;
                                    file_download.hash = Some(hash);
                                }

                            } else {
                                todo!()
                            }
                        },
                        Some(DownloadUpdate::Hash { id, hash }) => {
                            if let DownloadType::File(file_download) = download.files.get_mut(&id).unwrap() {
                                file_download.hash = Some(hash);
                            } else {
                                todo!()
                            }
                        },
                        Some(DownloadUpdate::ChunkCompleted { id, chunk_index }) => {
                            if let DownloadType::File(file_download) = download.files.get_mut(&id).unwrap() {
                                if chunk_index >= file_download.chunks.len() {
                                    file_download.chunks.resize(chunk_index, false);
                                }

                                file_download.chunks.set(chunk_index, true);
                            } else {
                                todo!()
                            }
                        }
                        Some(DownloadUpdate::FileSize { id, len }) => {
                            if let DownloadType::File(file_download) = download.files.get_mut(&id).unwrap() {
                                if file_download.chunks.len() == 0 {
                                    file_download.chunks.resize(len, false);
                                }
                            } else {
                                todo!()
                            }
                        }
                        None => break,
                    };
                }
                _ = save_interval.tick() => {
                    state_manager.write_download(&download).await;
                }
            }
        }

        state_manager.write_download(&download).await;
        (download, state_manager)
    });

    for (id, url, path) in download_queue {
        sender.send(DownloadUpdate::Status { id, status: DownloadStatus::InProgress }).unwrap();

        download_file(id, &url, &path, host, &client, &sender).await;

        sender.send(DownloadUpdate::Status { id, status: DownloadStatus::Completed }).unwrap();
    }

    drop(sender);

    let (mut download, state_manager) = handle.await.unwrap();
    
    download.status = DownloadStatus::Completed;

    println!("{}", download.url());
    state_manager.write_download(&download).await;
}

async fn download_file(id: usize, url: &str, path: &Path, host: Host, client: &reqwest::Client, sender: &UnboundedSender<DownloadUpdate>) {
    let mut response = client.get(url)
        .headers(host.headers())
        .send()
        .await.unwrap();

    if let Some(file_size) = response.content_length() {
        sender.send(DownloadUpdate::FileSize { id, len: file_size.div_ceil(16384) as usize }).unwrap();
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

    while let Some(chunk) = response.chunk().await.unwrap() {
        buffer.extend_from_slice(&chunk);

        while buffer.len() >= chunk_size {
            file.write_all(&buffer[..chunk_size]).await.unwrap();

            sender.send(DownloadUpdate::ChunkCompleted { id, chunk_index: current_chunk }).unwrap();
            current_chunk += 1;

            buffer.copy_within(chunk_size.., 0);
            buffer.truncate(buffer.len() - chunk_size);
        }
    }

    // handle remaining bytes (final chunk)
    if !buffer.is_empty() {
        file.write_all(&buffer).await.unwrap();
        sender.send(DownloadUpdate::ChunkCompleted { id, chunk_index: current_chunk }).unwrap();
    }

    file.sync_all().await.unwrap();
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

#[derive(Debug, Clone, Copy, Encode, Decode, Serialize, Deserialize)]
pub enum DownloadStatus {
    Queued,
    InProgress,
    Completed,
    Paused,
    Failed,
}

#[derive(Debug, Encode, Decode)]
pub struct Download {
    url: String,
    relative_path: PathBuf,
    host: Host,
    hash: Option<u128>,
    status: DownloadStatus,
    files: HashMap<usize, DownloadType>,
}

impl Download {
    pub const fn url(&self) -> &String {
        &self.url
    }
}

impl Download {
    fn new(value: DownloadTask) -> Self {
        let mut relative_path = PathBuf::new();

        let mut files = HashMap::new();
        let mut current_id = 0;

        match value.task_type {
            TaskType::File(file_task) => {
                files.insert(current_id, DownloadType::File(FileDownload::new(&file_task, &relative_path, current_id)));
            },
            TaskType::Folder(folder_task) => {
                Self::process_folder_creation(&folder_task, &mut relative_path, &mut current_id, &mut files);
            },
        }

        Self { 
            url: value.url,
            relative_path: PathBuf::new(),
            host: value.host,
            hash: None,
            status: DownloadStatus::Queued,
            files: files,
        }
    }

    fn process_folder_creation(folder_task: &FolderTask, parent_relative_path: &Path, current_id: &mut usize, files: &mut HashMap<usize, DownloadType>) {
        let mut children = Vec::new();
        let mut relative_path = parent_relative_path.join(&folder_task.folder_name());

        for file_type in &folder_task.files {
            match file_type {
                TaskType::File(file_task) => {
                    files.insert(*current_id, DownloadType::File(FileDownload::new(&file_task, &relative_path, *current_id)));
                    children.push(*current_id);
                    *current_id += 1;
                },
                TaskType::Folder(folder_task) => {
                    Self::process_folder_creation(folder_task, &mut relative_path, current_id, files);
                },
            }
        }

        files.insert(*current_id, DownloadType::Folder(FolderDownload::new(&folder_task, &parent_relative_path, *current_id, children)));
        *current_id += 1;
    }
}

#[derive(Debug, Encode, Decode)]
enum DownloadType {
    File(FileDownload),
    Folder(FolderDownload),
}

#[derive(Encode, Decode)]
struct FileDownload {
    id: usize,
    url: String,
    file_name: String,
    relative_path: PathBuf,
    status: DownloadStatus,
    hash: Option<u128>,
    #[bincode(with_serde)]
    chunks: BitVec,
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
    pub fn new(file_task: &FileTask, relative_path: &Path, id: usize) -> Self {
        let relative_path = relative_path.join(&file_task.file_name());

        Self { id,
            url: file_task.url.to_owned(),
            file_name: file_task.file_name().to_owned(),
            relative_path,
            status: DownloadStatus::Queued,
            hash: None,
            chunks: BitVec::new(), 
        }
    }   

    pub fn relative_path(&self) -> &Path {
        self.relative_path.as_path()
    }
}

#[derive(Debug, Encode, Decode)]
struct FolderDownload {
    id: usize,
    folder_name: String,
    relative_path: PathBuf,
    status: DownloadStatus,
    hash: Option<u128>,
    children: Vec<usize>,
}

impl FolderDownload {
    pub fn new(folder_task: &FolderTask, parent_relative_path: &Path, id: usize, children: Vec<usize>) -> Self {
        let relative_path = parent_relative_path.join(&folder_task.folder_name());

        Self { id,
            folder_name: folder_task.folder_name().to_owned(),
            relative_path,
            status: DownloadStatus::Queued,
            hash: None,
            children
        }
    }
}