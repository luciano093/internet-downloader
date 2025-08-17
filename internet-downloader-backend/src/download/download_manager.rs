use std::collections::VecDeque;
use std::path::{Path, PathBuf};

use bincode::{Decode, Encode};
use indexmap::IndexMap;
use thiserror::Error;
use tokio::fs::{create_dir_all};
use tokio::io::AsyncWriteExt;
use tokio::sync::{broadcast, mpsc};
use url::{ParseError, Url};
use xxhash_rust::xxh3::Xxh3;

use crate::download::hosts::Host;
use crate::state_manager::StateManager;

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct DownloadUpdate {
    url: String
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

        self.download_queue.append(&mut restored_downloads);
    }

    pub async fn add_download(&mut self, url: &str) -> Result<(), DownloadManagerError> {
        self.update_sender.send(DownloadUpdate { url: "test".to_owned() }).unwrap();

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

    let mut queue = VecDeque::new();
    queue.push_back(&mut download.download_type);

    while let Some(download_type) = queue.pop_front() {
        match download_type {
            DownloadType::File(file_download) => {
                download_file(file_download, download.host, &state_manager, &client).await;
            },
            DownloadType::Folder(folder_download) => {
                folder_download.status = DownloadStatus::InProgress;
                for nested_type in &mut folder_download.files {
                    queue.push_back(nested_type);
                }
                folder_download.status = DownloadStatus::Completed;
            },
        }
    }
    
    download.status = DownloadStatus::Completed;

    println!("{}", download.url());
    state_manager.write_download(download).await;
}

async fn download_file(file_download: &mut FileDownload, host: Host, state_manager: &StateManager, client: &reqwest::Client) {
    let mut response = client.get(&file_download.url)
        .headers(host.headers())
        .send()
        .await.unwrap();

    println!("{}", file_download.relative_path().to_str().unwrap());

    if let Some(parent_path) = file_download.relative_path().parent() {
        create_dir_all(parent_path).await.unwrap();
    }

    let mut file = tokio::fs::File::create(&file_download.relative_path()).await.unwrap();
    let mut hasher = Xxh3::with_seed(0); 

    file_download.status = DownloadStatus::InProgress;

    println!("url: {}", file_download.url);

    while let Ok(Some(chunk)) = response.chunk().await {
        hasher.update(&chunk);
        file.write_all(&chunk).await.unwrap();
    }

    file_download.hash = Some(hasher.digest128());
    file_download.status = DownloadStatus::Completed;
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

#[derive(Debug, Encode, Decode)]
enum DownloadStatus {
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
    download_type: DownloadType,
    host: Host,
    status: DownloadStatus,
    hash: Option<u128>,
}

impl Download {
    pub const fn url(&self) -> &String {
        &self.url
    }
}

impl Download {
    fn new(value: DownloadTask) -> Self {
        let relative_path = PathBuf::new();

        let download_type = match value.task_type {
                TaskType::File(file_task) => {
                    let relative_path = relative_path.clone().join(file_task.file_name());
                    DownloadType::File(FileDownload::new(file_task, relative_path))
                },
                TaskType::Folder(folder_task) => {
                    let relative_path = relative_path.clone().join(folder_task.folder_name());
                    DownloadType::Folder(FolderDownload::new(folder_task, relative_path))
                },
            };

        Self {
            url: value.url,
            relative_path,
            download_type,
            host: value.host,
            status: DownloadStatus::Queued,
            hash: None,
        }
    }
}

#[derive(Debug, Encode, Decode)]
enum DownloadType {
    File(FileDownload),
    Folder(FolderDownload),
}

#[derive(Debug, Encode, Decode)]
struct FileDownload {
    url: String,
    file_name: String,
    relative_path: PathBuf,
    status: DownloadStatus,
    hash: Option<u128>,
}

impl FileDownload {
    fn new(value: FileTask, relative_path: PathBuf) -> Self {
        Self {
            url: value.url,
            file_name: value.file_name,
            relative_path,
            status: DownloadStatus::Queued,
            hash: None,
        }
    }

    pub fn relative_path(&self) -> &Path {
        self.relative_path.as_path()
    }
}


#[derive(Debug, Encode, Decode)]
struct FolderDownload {
    folder_name: String,
    relative_path: PathBuf,
    status: DownloadStatus,
    files: Vec<DownloadType>,
    hash: Option<u128>,
}

impl FolderDownload {
    fn new(value: FolderTask, relative_path: PathBuf) -> Self {
        let files = value.files.into_iter().map(|file| {
                match file {
                    TaskType::File(file_task) => {
                        let relative_path = relative_path.clone().join(file_task.file_name());

                        DownloadType::File(FileDownload::new(file_task, relative_path))
                    },
                    TaskType::Folder(folder_task) => {
                        let relative_path = relative_path.clone().join(folder_task.folder_name());
                        DownloadType::Folder(FolderDownload::new(folder_task, relative_path))
                    },
                }
            
            }).collect();

        Self {
            folder_name: value.folder_name,
            relative_path,
            status: DownloadStatus::Queued,
            files,
            hash: None,
        }
    }
}