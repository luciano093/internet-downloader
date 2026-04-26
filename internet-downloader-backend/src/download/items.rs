use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::fmt::Debug;

use bitvec::order::Msb0;
use bitvec::vec::BitVec;
use indexmap::IndexMap;
use os_str_bytes::OsStringBytes;
use serde::{Deserialize, Serialize};

use crate::db::rows::{DownloadItemRow, DownloadRow};
use crate::download::{DownloadFailureReason, DownloadId, FileFailureReason, FileSize};
use crate::download::hosts::{DownloadTask, FileTask, FolderTask, TaskType};
use crate::download::status::{DownloadStatus, FileStatus, StatusBucket, StateBucketCounters};
use crate::download::{serialize_hash, serialize_chunks};

pub trait DownloadItem {
    fn parent_id(&self) -> Option<usize>;

    fn id(&self) -> usize;

    fn relative_path(&self) -> &PathBuf;

    fn name(&self) -> &str;
}

#[derive(Debug, Clone)]
pub enum ChangedItem {
    File { id: usize, status: FileStatus },
    Folder { id: usize, status: DownloadStatus },
    Download(DownloadStatus), 
}

/// Has either a file or folder as the only item in root
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Download {
    id: DownloadId,
    url: String,
    relative_path: PathBuf,
    status: DownloadStatus,
    pub(crate) files: IndexMap<usize, DownloadType>,
    name: String,
}

impl Download {
    pub fn new(id: usize, value: DownloadTask) -> Self {
        let relative_path = PathBuf::new();

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
                Self::process_folder_creation(&folder_task, &relative_path, &mut current_id, &mut files, None);
            },
        }

        Self { 
            id: DownloadId(id),
            url: value.url,
            relative_path: PathBuf::from("./"),
            status: DownloadStatus::Queued,
            files,
            name,
        }
    }

    pub const fn url(&self) -> &String {
        &self.url
    }

    pub fn get_file_mut(&mut self, id: &usize) -> Option<&mut FileDownload> {
        match self.files.get_mut(id) {
            Some(DownloadType::File(file)) => Some(file),
            _ => None,
        }
    }

    pub const fn id(&self) -> DownloadId {
        self.id
    }

    pub const fn files(&self) -> &IndexMap<usize, DownloadType> {
        &self.files
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

    pub fn set_paused(&mut self) -> Vec<ChangedItem> {
        let mut files_to_pause = Vec::new();

        for (&id, item) in &self.files {
            if let DownloadType::File(file) = item {
                if file.status().is_active() {
                    files_to_pause.push(id);
                }
            }
        }

        let mut all_changes = Vec::new();

        for id in files_to_pause {
            if let Some(changes) = self.set_file_status(id, FileStatus::Paused) {
                all_changes.extend(changes);
            }
        }

        all_changes
    }

    pub fn set_queued(&mut self) -> Vec<ChangedItem> {
        let mut files_to_queue = Vec::new();

        for (&id, item) in &self.files {
            if let DownloadType::File(file) = item {
                if file.status().can_set_to_queue() {
                    files_to_queue.push(id);
                }
            }
        }
        
        let mut all_changes = Vec::new();

        for id in files_to_queue {
            if let Some(changes) = self.set_file_status(id, FileStatus::Queued) {
                all_changes.extend(changes);
            }
        }

        all_changes
    }

    pub fn set_file_status(&mut self, id: usize, status: FileStatus) -> Option<Vec<ChangedItem>> {
        let mut changed_items = Vec::new();

        let (mut current_parent_id, mut previous_status_bucket, mut new_status_bucket) = {
            if let Some(DownloadType::File(file)) = self.files.get_mut(&id) {
                if file.status == status {
                    return None; // No change happened at all
                }

                let prev_bucket = file.status.bucket();
                let new_bucket = status.bucket();

                file.status = status;
                changed_items.push(ChangedItem::File {
                    id,
                    status,
                });

                (file.parent_id(), prev_bucket, new_bucket)
            } else {
                return None; // ID was not found, or it was a Folder
            }
        };

        // Parents don't care if the bucket didn't change, as it means they have no need to
        // update their statuses
        if previous_status_bucket == new_status_bucket && new_status_bucket != StatusBucket::Error {
            return Some(changed_items);
        }

        // We update each parent
        while let Some(parent_id) = current_parent_id {
            let (previous_folder_status, next_parent_id) = {
                if let Some(DownloadType::Folder(folder)) = self.files.get_mut(&parent_id) {
                    folder.bucket_counters.decrement(previous_status_bucket);
                    folder.bucket_counters.increment(new_status_bucket);
                    (folder.status, folder.parent_id)
                } else {
                    break; // No more parents to update
                }
            };

            let new_folder_status = {
                if let Some(DownloadType::Folder(folder)) = self.files.get(&parent_id) {
                    folder.calculate_status(&self.files)
                } else {
                    break; // No more parents to update
                }
            };

            if let Some(DownloadType::Folder(folder)) = self.files.get_mut(&parent_id) {
                folder.status = new_folder_status;
            }

            if previous_folder_status != new_folder_status {
                changed_items.push(ChangedItem::Folder {
                    id: parent_id,
                    status: new_folder_status,
                });
            }

            let old_bucket = previous_folder_status.bucket();
            let new_bucket = new_folder_status.bucket();

            // No real state change, parents won't care about the change
            if old_bucket == new_bucket && new_bucket != StatusBucket::Error {
                break; 
            }
        
            previous_status_bucket = old_bucket;
            new_status_bucket = new_bucket;
            current_parent_id = next_parent_id;
        };

        if let Some(root_item) = self.files.get(&0) {
            let new_root_status = root_item.as_download_status();

            if self.status != new_root_status {
                self.status = new_root_status;
                changed_items.push(ChangedItem::Download(new_root_status));
            }
        }

        Some(changed_items)
    }

    fn process_folder_creation(folder_task: &FolderTask, parent_relative_path: &Path, current_id: &mut usize, files: &mut IndexMap<usize, DownloadType>, parent_id: Option<usize>) {
        let mut children = Vec::new();
        let relative_path = parent_relative_path.join(folder_task.folder_name());

        let folder_id = *current_id;
        *current_id += 1;

        for file_type in &folder_task.files {
            match file_type {
                TaskType::File(file_task) => {
                    let file = FileDownload::new(file_task, &relative_path, *current_id, Some(folder_id));
                    let status_bucket = file.status().bucket();

                    files.insert(*current_id, DownloadType::File(file));

                    children.push((*current_id, status_bucket));
                    *current_id += 1;
                },
                TaskType::Folder(folder_task) => {
                    Self::process_folder_creation(folder_task, &relative_path, current_id, files, Some(folder_id));
                },
            }
        }

        files.insert(folder_id, DownloadType::Folder(FolderDownload::new(folder_task, parent_relative_path, folder_id, children, parent_id)));
    }

    pub fn from_db(row: DownloadRow, files: IndexMap<usize, DownloadType>) -> Self {
        let mut status = DownloadStatus::from_db_columns(&row.status, row.failure_reason.as_deref())
            .unwrap_or_default();

        let relative_path = PathBuf::from_io_vec(row.relative_path_raw)
            .unwrap_or_else(|| {
                status = DownloadStatus::Failed(DownloadFailureReason::BadPath);
            
                PathBuf::new()
            });

        Self {
            id: DownloadId(row.id as usize),
            url: row.url,
            relative_path,
            status,
            files,
            name: row.name,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum DownloadType {
    File(FileDownload),
    Folder(FolderDownload),
}

impl DownloadType {
    pub fn as_download_status(&self) -> DownloadStatus {
        match self {
            DownloadType::Folder(folder) => folder.status(),
            DownloadType::File(file) => match file.status() {
                FileStatus::Queued => DownloadStatus::Queued,
                FileStatus::Initializing => DownloadStatus::Initializing,
                FileStatus::FetchingMetadata => DownloadStatus::FetchingMetadata,
                FileStatus::InProgress => DownloadStatus::InProgress,
                FileStatus::Completed => DownloadStatus::Completed,
                FileStatus::Paused => DownloadStatus::Paused,
                FileStatus::NotFound => DownloadStatus::NotFound,
                FileStatus::Retrying => DownloadStatus::Retrying,
                FileStatus::Waiting(_) => DownloadStatus::Waiting,
                
                FileStatus::Failed(reason) => {
                    DownloadStatus::Failed(DownloadFailureReason::AllFilesFailed(reason))
                }
            },
        }
    }
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

    fn name(&self) -> &str {
        match self {
            DownloadType::File(f) => f.name(),
            DownloadType::Folder(f) => f.name(),
        }
    }
}


#[derive(Serialize, Deserialize, Clone)]
pub struct FileDownload {
    parent_id: Option<usize>,
    id: usize,
    url: Arc<String>,
    file_name: String,
    relative_path: PathBuf,
    status: FileStatus,
    #[serde(serialize_with = "serialize_hash")] 
    hash: Option<u128>,
    #[serde(serialize_with = "serialize_chunks")]
    chunks: BitVec<u8, Msb0>,
    chunk_hashes: Vec<Option<[u8; 16]>>,
    size: Option<FileSize>, // None means we haven't gotten the size yet, unknown means the size can't be known until it
    #[serde(skip)]
    /// tracks consecutive retries
    retries: usize, 
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
        let relative_path = relative_path.join(file_task.file_name());

        Self { 
            parent_id,
            id,
            url: Arc::new(file_task.url.clone()),
            file_name: file_task.file_name().to_owned(),
            relative_path,
            status: FileStatus::Queued,
            hash: None,
            chunks: BitVec::new(),
            chunk_hashes: Vec::new(),
            size: None,
            retries: 0,
        }
    }

    pub fn from_db(row: DownloadItemRow, chunk_hashes: Vec<Option<[u8; 16]>>) -> Self {
        // Reconstruct the FileSize
        let size = match row.size_type.as_deref() {
            Some("known") if let Some(size_bytes) = row.size_bytes => Some(FileSize::Known(size_bytes as u64)),
            Some("unknown") => Some(FileSize::Unknown),

            // If we have a known size, but the size was corrupted from the db, set it as None to fetch it again
            Some("known") | Some(_) | None => None,
        };

        // Reconstruct the Hash
        let hash = row.hash.and_then(|bytes| {
            let slice = bytes.get(0..16)?;
            
            let array: [u8; 16] = slice.try_into().ok()?; 

            Some(u128::from_be_bytes(array))
        });

        // Reconstruct the Chunks (BitVec)
        let mut chunks = BitVec::<u8, Msb0>::from_vec(row.chunks_raw.unwrap_or_default());
        if let Some(len) = row.chunks_len {
            chunks.truncate(len as usize);
        }

        let mut status = FileStatus::from_db_columns(&row.status, row.failure_reason.as_deref(), row.wait_time).unwrap_or_default();

        let relative_path = PathBuf::from_io_vec(row.relative_path_raw)
            .unwrap_or_else(|| {
                status = FileStatus::Failed(FileFailureReason::BadPath);
            
                PathBuf::new()
            });

        Self {
            parent_id: row.parent_id.map(|id| id as usize),
            id: row.item_id as usize,
            url: Arc::new(row.url.unwrap_or_default()),
            file_name: row.name,
            relative_path,
            status,
            hash,
            chunks,
            chunk_hashes,
            size,
            retries: row.retries as usize,
        }
    }

    pub const fn chunks(&self) -> &BitVec<u8, Msb0> {
        &self.chunks
    }

    pub fn chunks_mut(&mut self) -> &mut BitVec<u8, Msb0> {
        &mut self.chunks
    }

    pub const fn chunk_hashes(&self) -> &Vec<Option<[u8; 16]>> {
        &self.chunk_hashes
    }

    pub fn chunk_hashes_mut(&mut self) -> &mut Vec<Option<[u8; 16]>> {
        &mut self.chunk_hashes
    }

    pub const fn hash(&self) -> Option<u128> {
        self.hash
    }

    pub fn url(&self) -> Arc<String> {
        self.url.clone()
    }

    pub fn url_ref(&self) -> &String {
        self.url.as_ref()
    }

    pub fn status(&self) -> FileStatus {
        self.status
    }

    pub fn size(&self) -> Option<FileSize> {
        self.size
    }

    pub fn set_size(&mut self, size: FileSize) {
        self.size = Some(size);
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

    pub fn calculate_initial_bytes(&self, chunk_size: u64) -> u64 {
        let chunks = self.chunks();

        if chunks.is_empty() {
            return 0;
        }

        let file_size = match self.size() {
            Some(FileSize::Known(size)) => size,
            _ => return 0,
        };

        if self.status == FileStatus::Completed {
            return file_size;
        }

        let last_chunk_index = chunks.len() - 1;

        // Did we download the very last chunk?
        let has_last_chunk = chunks.get(last_chunk_index).as_deref() == Some(&true);

        let downloaded_chunks = chunks.count_ones() as u64;

        if has_last_chunk {
            // All chunks except the last one are full size
            let standard_bytes = (downloaded_chunks - 1) * chunk_size;
            
            // We calculate the size of the last chunk
            let last_chunk_bytes = self.calculate_chunk_expected_len(
                chunk_size, 
                (last_chunk_index, last_chunk_index + 1), 
                file_size
            );

            standard_bytes + last_chunk_bytes
        } else {
            // If we don't have the last chunk, every chunk we have is standard size
            downloaded_chunks * chunk_size
        }
    }

    fn calculate_chunk_expected_len(&self, chunk_size: u64, range: (usize, usize), file_size: u64) -> u64 {
        let start_byte = range.0 as u64 * chunk_size;
        let theoretical_end = range.1 as u64 * chunk_size;

        let actual_end = std::cmp::min(theoretical_end, file_size);
        let expected_len = actual_end.saturating_sub(start_byte);
        
        expected_len.min(file_size)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FolderDownload {
    parent_id: Option<usize>,
    id: usize,
    folder_name: String,
    relative_path: PathBuf,
    status: DownloadStatus,
    children: Vec<usize>,

    // Counters to keep track of children statuses without having to recalculate them
    #[serde(skip)]
    bucket_counters: StateBucketCounters,
}

impl FolderDownload {
    pub(super) fn new(folder_task: &FolderTask, parent_relative_path: &Path, id: usize, children: Vec<(usize, StatusBucket)>, parent_id: Option<usize>) -> Self {
        let relative_path = parent_relative_path.join(folder_task.folder_name());

        let mut bucket_counters = StateBucketCounters::new();
        let mut children_ids = Vec::with_capacity(children.len());

        for (child_id, bucket) in children {
            children_ids.push(child_id);
            bucket_counters.increment(bucket);
        }

        Self { 
            parent_id,
            id,
            folder_name: folder_task.folder_name().to_owned(),
            relative_path,
            status: DownloadStatus::Queued,
            children: children_ids,

            bucket_counters,
        }
    }

    pub fn from_db(row: DownloadItemRow, children: Vec<usize>, bucket_counters: StateBucketCounters) -> Self {
        let mut status = DownloadStatus::from_db_columns(&row.status, row.failure_reason.as_deref())
            .unwrap_or_default();

        let relative_path = PathBuf::from_io_vec(row.relative_path_raw)
            .unwrap_or_else(|| {
                status = DownloadStatus::Failed(DownloadFailureReason::BadPath);
            
                PathBuf::new()
            });

        Self {
            parent_id: row.parent_id.map(|id| id as usize),
            id: row.item_id as usize,
            folder_name: row.name,
            relative_path,
            status: DownloadStatus::from_db_columns(&row.status, row.failure_reason.as_ref().map(|str| str.as_str())).unwrap_or_default(),
            children,
            bucket_counters,
        }
    }

    pub fn calculate_status(&self, files_map: &IndexMap<usize, DownloadType>) -> DownloadStatus {
        match self.dominant_status() {
            Some(StatusBucket::Queued) => DownloadStatus::Queued,
            Some(StatusBucket::Initializing) => DownloadStatus::Initializing,
            Some(StatusBucket::FetchingMetadata) => DownloadStatus::FetchingMetadata,
            Some(StatusBucket::InProgress) => DownloadStatus::InProgress,
            Some(StatusBucket::Retrying) => DownloadStatus::Retrying,
            Some(StatusBucket::Waiting) => DownloadStatus::Waiting,
            Some(StatusBucket::Paused) => DownloadStatus::Paused,
            Some(StatusBucket::Completed) => DownloadStatus::Completed,
            Some(StatusBucket::CompletedWithErrors) => DownloadStatus::CompletedWithErrors,
            Some(StatusBucket::Error) => self.resolve_error_status(files_map),
            None if self.children.is_empty() => DownloadStatus::Completed, 
            None => DownloadStatus::CompletedWithErrors, 
        }
    }

    fn dominant_status(&self) -> Option<StatusBucket> {
        let total = self.children.len();

        // No children means we are completed, no dominant status
        if total == 0 {
            return None; 
        }

        // Active states, if any of any children has an active state, we adopt the state too
        // Order is important
        // If anything is downloading, the folder is downloading
        if self.bucket_counters.get(StatusBucket::InProgress) > 0 {
            Some(StatusBucket::InProgress)
        } 
        // If nothing is downloading yet, but we are fetching metadata, the whole folder is fetching
        else if self.bucket_counters.get(StatusBucket::FetchingMetadata) > 0 {
            Some(StatusBucket::FetchingMetadata)
        } 
        // If no network IO is happening, but we are allocating space, we are initializing
        else if self.bucket_counters.get(StatusBucket::Initializing) > 0 {
            Some(StatusBucket::Initializing)
        } 
        // If nothing is downloading, but we are retrying a download
        else if self.bucket_counters.get(StatusBucket::Retrying) > 0 {
            Some(StatusBucket::Retrying)
        } 
        // If everything is either waiting or queued
        else if self.bucket_counters.get(StatusBucket::Waiting) > 0 {
            Some(StatusBucket::Waiting)
        } 
        // Every single download that needs to be downloaded is still in queue
        else if self.bucket_counters.get(StatusBucket::Queued) > 0 {
            Some(StatusBucket::Queued)
        } 

        // If no download is active, but some are paused, then we also are paused
        else if self.bucket_counters.get(StatusBucket::Paused) > 0 {
            Some(StatusBucket::Paused)
        } 
        // If all children share the same status, we too share it
        else if self.bucket_counters.get(StatusBucket::Error) == total {
            Some(StatusBucket::Error)
        } else if self.bucket_counters.get(StatusBucket::Completed) == total {
            Some(StatusBucket::Completed)
        } else if self.bucket_counters.get(StatusBucket::CompletedWithErrors) == total {
            Some(StatusBucket::CompletedWithErrors)
        } 
        // There is no dominant status that exists 
        else {
            None
        }
    }

    fn resolve_error_status(&self, files_map: &IndexMap<usize, DownloadType>) -> DownloadStatus {
        let mut first_error = None;
        let mut multiple_errors = false;

        let mut not_found_files = 0;
        let total = self.children.len();

        for &child_id in &self.children {
            if let Some(child) = files_map.get(&child_id) {
                
                let (file_not_found, reason) = match child {
                    DownloadType::File(file) => match file.status() {
                        FileStatus::NotFound => (true, None),
                        FileStatus::Failed(reason) => (false, Some(DownloadFailureReason::AllFilesFailed(reason))),
                        _ => continue,
                    },
                    DownloadType::Folder(folder) => match folder.status() {
                        DownloadStatus::NotFound => (true, None),
                        DownloadStatus::Failed(reason) => (false, Some(reason)),
                        _ => continue,
                    }
                };

                if file_not_found {
                    not_found_files += 1;
                    // If we see a file with a different error, we know we found a mix of errors
                    // and we can skip the rest.
                    if first_error.is_some() {
                        multiple_errors = true;
                        break; 
                    }
                } else if let Some(reason) = reason {
                    // We found a mix of errors, we can exit loop
                    if not_found_files > 0 {
                        multiple_errors = true;
                        break;
                    } 
                    // We found our first error, save it
                    else if first_error.is_none() {
                        first_error = Some(reason);
                    } 
                    
                    // We found multiple errors, exit loop
                    else if first_error != Some(reason) {
                        multiple_errors = true;
                        break;
                    }
                }
            }
        }

        // If no children files were found, we might not exist ourselves
        if not_found_files == total {
            if !self.relative_path.exists() {
                return DownloadStatus::NotFound;
            } 
            
            // If we still exist, but have no children were found...
            else {
                return DownloadStatus::Failed(DownloadFailureReason::FilesMissingFromDisk); 
            }
        }

        if multiple_errors {
            DownloadStatus::Failed(DownloadFailureReason::MultipleErrors)
        } else if let Some(reason) = first_error {
            DownloadStatus::Failed(reason)
        } 
        // We didn't find multiple errors, but also couldn't find a first error, do we even have children?
        else {
            // Supposedly mathematically unreachable code, but who knows, maybe a bit flips in the runtime of this program
            if self.children().len() == 0 {
                return DownloadStatus::Completed;
            }

            // If we still have children, we probably desynced somehow
            DownloadStatus::Failed(DownloadFailureReason::StateDesynchronized)
        }
    }

    pub const fn children(&self) -> &Vec<usize> {
        &self.children
    }

    pub fn status(&self) -> DownloadStatus {
        self.status
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

    fn name(&self) -> &str {
        &self.folder_name
    }
}
