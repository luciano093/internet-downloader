use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bitvec::order::Msb0;
use bitvec::vec::BitVec;
use indexmap::IndexMap;
use memmap2::MmapOptions;
use rkyv::munge::munge;
use rkyv::rancor::Fallible;
use rkyv::vec::{ArchivedVec, VecResolver};
use rkyv::Place;
use rkyv::with::{ArchiveWith, AsString};
use serde::{Deserialize, Serialize, Serializer};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{Mutex, Semaphore, broadcast, mpsc};
use xxhash_rust::xxh3::xxh3_128_with_seed;

use crate::client_state_manager::{DownloadSnapshot, FrontendMessage, UiStateEvent, UiStateHandle, UiStateManager, get_snapshot};
use crate::download::hosts::{DownloadTask, FileTask, FolderTask, TaskType};
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

#[derive(Debug, Clone, Copy, Hash, PartialEq, PartialOrd, Eq, Serialize, Deserialize, Ord, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
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

#[derive(Debug)]
pub struct DownloadManager {
    next_id: Option<AtomicUsize>,
    db_state_manager: StateManager,
    unprocessed_downloads: IndexMap<DownloadId, Download>,
    download_command_sender: Arc<Mutex<HashMap<DownloadId, tokio::sync::broadcast::Sender<DownloadCommand>>>>, 
    ui_state_handle: Option<UiStateHandle>,
    command_sender: Option<UnboundedSender<ManagerCommand>>,
    concurrency_limit: Arc<Semaphore>,
}

impl DownloadManager {
    pub fn new(db_state_manager: StateManager) -> Self {
        DownloadManager {
            next_id: Some(AtomicUsize::new(0)), 
            db_state_manager,
            unprocessed_downloads: IndexMap::new(),
            download_command_sender: Arc::new(Mutex::new(HashMap::new())),
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

    pub async fn queue_download(&mut self, url: String) -> Result<(), ()> {
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

                    Ok(_) = concurrency_limit.clone().acquire_owned(), if !queue.is_empty() => {
                        if let Some((_, download)) = queue.shift_remove_index(0) {
                            let _ = command_sender.send(ManagerCommand::QueueDownload(download.url().to_string()));
                        }
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

async fn hash_file(path: PathBuf) -> u128 {
    tokio::task::spawn_blocking(move || {
        let file = std::fs::File::open(&path).expect("Failed to open file for hashing");
        
        let mmap = unsafe { MmapOptions::new().map(&file).expect("Failed to mmap file") };

        xxh3_128_with_seed(&mmap, 0)
    }).await.expect("Hashing task panicked")
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
pub enum DownloadStatus {
    Queued,
    Initializing,
    InProgress,
    Completed,
    Paused,
    Failed(DownloadFailureReason),
    NotFound,
    Retrying,
    Waiting(Option<u64>)
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
#[repr(u8)]
pub enum DownloadFailureReason {
    HashMismatch,
    DiskError,
    ClientError,
    ServerError,
    MetadataFetchError,
}

/// Has either a file or folder as the only item in root
#[derive(Debug, Serialize, Deserialize, Clone, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
pub struct Download {
    id: DownloadId,
    url: String,
    #[rkyv(with = AsString)]
    relative_path: PathBuf,
    status: DownloadStatus,
    pub(crate) files: IndexMap<usize, DownloadType>,
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

    pub const fn files(&self) -> &IndexMap<usize, DownloadType> {
        &self.files
    }

    pub const fn files_mut(&mut self) -> &mut IndexMap<usize, DownloadType> {
        &mut self.files
    }

    pub const fn relative_path(&self) -> &PathBuf {
        &self.relative_path
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

        let mut files = IndexMap::new();
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
            status: DownloadStatus::Queued,
            files: files,
            name,
        }
    }

    fn process_folder_creation(folder_task: &FolderTask, parent_relative_path: &Path, current_id: &mut usize, files: &mut IndexMap<usize, DownloadType>, parent_id: Option<usize>) {
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
}

#[derive(Debug, Serialize, Deserialize, Clone, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
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

#[derive(Serialize, Deserialize, Clone, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
pub struct FileDownload {
    parent_id: Option<usize>,
    id: usize,
    url: Arc<String>,
    file_name: String,
    #[rkyv(with = AsString)]
    relative_path: PathBuf,
    status: DownloadStatus,
    #[serde(serialize_with = "serialize_hash")] 
    hash: Option<u128>,
    #[serde(serialize_with = "serialize_chunks")]
    #[rkyv(with = AsBitVec)]
    chunks: BitVec<u8, Msb0>,
    size: Option<FileSize>, // None means we haven't gotten the size yet, unknown means the size can't be known until it
    bytes_downloaded: u64,
    #[serde(skip)]
    /// tracks consecutive retries
    retries: usize, 
}

pub struct AsBitVec;

pub struct BitVecResolver {
    len: u64,
    inner: VecResolver,
}

#[derive(rkyv::Portable, bytecheck::CheckBytes)]
#[repr(C)]
pub struct ArchivedBitVec {
    pub data: ArchivedVec<u8>,
    pub bit_len: rkyv::rend::u64_le,
}

impl ArchiveWith<BitVec<u8, Msb0>> for AsBitVec {
    type Archived = ArchivedBitVec;
    type Resolver = BitVecResolver;

    fn resolve_with(field: &BitVec<u8, Msb0>, resolver: Self::Resolver, out: Place<Self::Archived>) {
        munge!(let ArchivedBitVec { data, bit_len } = out);

        ArchivedVec::resolve_from_len(field.as_raw_slice().len(), resolver.inner, data);

        bit_len.write(resolver.len.into());
    }
}

impl<S: rkyv::ser::Writer + ?Sized + rkyv::rancor::Fallible + rkyv::ser::Allocator> rkyv::with::SerializeWith<BitVec<u8, Msb0>, S> for AsBitVec {
    fn serialize_with(
        field: &BitVec<u8, Msb0>,
        serializer: &mut S,
    ) -> Result<Self::Resolver, <S as rkyv::rancor::Fallible>::Error> {
        
        Ok(BitVecResolver { 
            len: field.len() as u64,
            inner: ArchivedVec::serialize_from_slice(field.as_raw_slice(), serializer)?
        })
    }
}

impl<D: rkyv::rancor::Fallible + ?Sized> rkyv::with::DeserializeWith<ArchivedBitVec, BitVec<u8, Msb0>, D> for AsBitVec 
    where <D as Fallible>::Error: rkyv::rancor::Source {
    fn deserialize_with(field: &ArchivedBitVec, deserializer: &mut D)
        -> Result<BitVec<u8, Msb0>, <D as rkyv::rancor::Fallible>::Error> {
        let bytes: Vec<u8> = rkyv::Deserialize::deserialize(&field.data, deserializer)?;
        let mut bitvec = BitVec::<u8, Msb0>::from_vec(bytes);
        
        let bit_len: u64 = field.bit_len.into();
        bitvec.truncate(bit_len as usize);
        Ok(bitvec)
    }
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

fn serialize_chunks<S>(chunks: &BitVec<u8, Msb0>, serializer: S) -> Result<S::Ok, S::Error>
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
            url: Arc::new(file_task.url.clone()),
            file_name: file_task.file_name().to_owned(),
            relative_path,
            status: DownloadStatus::Queued,
            hash: None,
            chunks: BitVec::new(),
            size: None,
            bytes_downloaded: 0,
            retries: 0,
        }
    }

    pub const fn chunks(&self) -> &BitVec<u8, Msb0> {
        &self.chunks
    }

    pub fn chunks_mut(&mut self) -> &mut BitVec<u8, Msb0> {
        &mut self.chunks
    }

    pub const fn hash(&self) -> Option<u128> {
        self.hash
    }

    pub fn url(&self) -> Arc<String> {
        self.url.clone()
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

    pub fn retries(&self) -> usize {
        self.retries
    }

    pub fn increment_retries(&mut self) {
        self.retries += 1;
    }

    pub fn reset_retries(&mut self) {
        self.retries = 0;
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
pub struct FolderDownload {
    parent_id: Option<usize>,
    id: usize,
    folder_name: String,
    #[rkyv(with = AsString)]
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

#[derive(Debug, Copy, Clone, Deserialize, PartialEq, Eq, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
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