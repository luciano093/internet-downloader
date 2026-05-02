use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::fmt::{Debug, Display};

use bitvec::order::Msb0;
use bitvec::vec::BitVec;
use indexmap::IndexMap;
use os_str_bytes::OsStringBytes;
use serde::{Deserialize, Serialize};

use crate::db::rows::{DownloadFileRow, DownloadFolderRow, DownloadRow};
use crate::download::{DownloadFailureReason, FileFailureReason, FileSize};
use crate::download::hosts::{DownloadTask, FileTask, FolderTask, TaskType};
use crate::download::status::{DownloadStatus, FileStatus, StatusBucket, StateBucketCounters};
use crate::download::{serialize_hash, serialize_chunks};
use crate::download_task::HASH_CHUNK_SIZE;

pub trait DownloadItem {
    type Id;
    type Status;
    
    fn parent_id(&self) -> Option<FolderId>;

    fn id(&self) -> Self::Id;

    fn relative_path(&self) -> &PathBuf;

    fn name(&self) -> &str;

    fn active_operation(&self) -> Option<ActiveOperation>;

    fn status(&self) -> Self::Status;
}

#[derive(Debug, Clone)]
pub enum ChangedItemStatus {
    File { id: FileId, status: FileStatus },
    Folder { id: FolderId, status: DownloadStatus },
    Download(DownloadStatus), 
}

#[derive(Debug, Clone)]
pub enum ChangedItemOperation {
    File { id: FileId, operation: Option<ActiveOperation> },
    Folder { id: FolderId, operation: Option<ActiveOperation> },
    Download(Option<ActiveOperation>), 
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, PartialOrd, Eq, Serialize, Deserialize, Ord, sqlx::Type)]
#[serde(transparent)]
#[sqlx(transparent)]
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

#[derive(Debug, Clone, Copy, Hash, Serialize, Deserialize)]
pub enum ItemId {
    Folder(FolderId),
    File(FileId),
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, PartialOrd, Eq, Serialize, Deserialize, Ord, sqlx::Type)]
#[serde(transparent)]
#[sqlx(transparent)]
pub struct FileId(pub usize);

impl Deref for FileId {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for FileId {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Display for FileId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, PartialOrd, Eq, Serialize, Deserialize, Ord, sqlx::Type)]
#[serde(transparent)]
#[sqlx(transparent)]
pub struct FolderId(pub usize);

impl Deref for FolderId {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for FolderId {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}


impl Display for FolderId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub enum BaseItemRef<'a> {
    File(FileId, &'a FileDownload),
    Folder(FolderId, &'a FolderDownload),
}

impl<'a> DownloadItem for BaseItemRef<'a> {
    type Id = ItemId;
    type Status = DownloadStatus;

    fn parent_id(&self) -> Option<FolderId> {
        match self {
            BaseItemRef::File(_file_id, file) => file.parent_id(),
            BaseItemRef::Folder(_folder_id, folder) => folder.parent_id(),
        }
    }

    fn id(&self) -> Self::Id {
        match self {
            BaseItemRef::File(file_id, _file) => ItemId::File(*file_id),
            BaseItemRef::Folder(folder_id, _folder) => ItemId::Folder(*folder_id),
        }
    }

    fn relative_path(&self) -> &PathBuf {
        match self {
            BaseItemRef::File(_file_id, file) => file.relative_path(),
            BaseItemRef::Folder(_folder_id, folder) => folder.relative_path(),
        }
    }

    fn name(&self) -> &str {
        match self {
            BaseItemRef::File(_file_id, file) => file.name(),
            BaseItemRef::Folder(_folder_id, folder) => folder.name(),
        }
    }

    fn active_operation(&self) -> Option<ActiveOperation> {
        match self {
            BaseItemRef::File(_file_id, file) => file.active_operation(),
            BaseItemRef::Folder(_folder_id, folder) => folder.active_operation(),
        }
    }

    fn status(&self) -> Self::Status {
        match self {
            BaseItemRef::File(_file_id, file) => file.status().as_download_status(),
            BaseItemRef::Folder(_folder_id, folder) => folder.status(),
        }
    }
}

pub struct BaseItemIterator<'a> {
    files_iter: indexmap::map::Iter<'a, FileId, FileDownload>,
    folders_iter: indexmap::map::Iter<'a, FolderId, FolderDownload>,
}

impl<'a> BaseItemIterator<'a> {
    pub fn new(
        files: &'a IndexMap<FileId, FileDownload>,
        folders: &'a IndexMap<FolderId, FolderDownload>,
    ) -> Self
    {
        Self {
            files_iter: files.iter(),
            folders_iter: folders.iter(),
        }
    }
}

impl<'a> Iterator for BaseItemIterator<'a> {
    type Item = BaseItemRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        // If we have a file, return it
        if let Some((&file_id, file)) = self.files_iter.next() {
            return Some(BaseItemRef::File(file_id, file));
        }

        // Otherwise look for folders with no children, if one exists, return it
        while let Some((&folder_id, folder)) = self.folders_iter.next() {
            if folder.child_files.is_empty() && folder.child_folders.is_empty() {
                return Some(BaseItemRef::Folder(folder_id, folder));
            }
        }

        // Both are empty
        None
    }
}

/// Has either a file or folder as the only item in root
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Download {
    id: DownloadId,
    url: String,
    relative_path: PathBuf,
    status: DownloadStatus,
    active_operation: Option<ActiveOperation>,
    root_item: ItemId,
    files: IndexMap<FileId, FileDownload>,
    folders: IndexMap<FolderId, FolderDownload>,
    name: String,
}

impl Download {
    pub fn new(id: usize, value: DownloadTask) -> Self {
        let relative_path = PathBuf::new();

        let mut files = IndexMap::new();
        let mut folders: IndexMap<FolderId, FolderDownload> = IndexMap::new();
        let mut current_file_id = FileId(0);
        let mut current_folder_id = FolderId(0);
        let root_item;
        let name;

        match value.task_type {
            TaskType::File(file_task) => {
                root_item = ItemId::File(current_file_id);
                name = file_task.file_name().clone();
                files.insert(current_file_id, FileDownload::new(&file_task, &relative_path, current_file_id, None));
            },
            TaskType::Folder(folder_task) => {
                let root_folder_id = current_folder_id;
                root_item = ItemId::Folder(root_folder_id);
                name = folder_task.folder_name().clone();
                *current_folder_id += 1;

                // Folders need to be created bottom-up, but the data we have is top down
                // so we gather all of the data we need to create each Folder first, and then we create them
                let mut folder_data_stack = Vec::new();
                let mut stack = vec![(&folder_task, relative_path, None, root_folder_id)];

                while let Some((folder_task, parent_relative_path, parent_id, folder_id)) = stack.pop() {
                    let relative_path = parent_relative_path.join(folder_task.folder_name());

                    let mut child_files = Vec::new();
                    let mut child_folders = Vec::new();

                    for file_type in &folder_task.files {
                        match file_type {
                            TaskType::File(file_task) => {
                                let file = FileDownload::new(
                                    file_task, 
                                    &relative_path, 
                                    current_file_id, 
                                    Some(folder_id)
                                );
                                
                                child_files.push((current_file_id, file.status().bucket()));

                                files.insert(current_file_id, file);
                                *current_file_id += 1;
                            },
                            TaskType::Folder(child_folder_task) => {
                                let child_folder_id = current_folder_id;
                                *current_folder_id += 1;
                                
                                child_folders.push(child_folder_id);
                                
                                stack.push((child_folder_task, relative_path.clone(), Some(folder_id), child_folder_id));
                            },
                        }
                    }

                    folder_data_stack.push((folder_task, parent_relative_path, folder_id, child_files, child_folders, parent_id));
                }

                for (folder_task, parent_relative_path, folder_id, child_files, child_folders, parent_id) in folder_data_stack.into_iter().rev() {
                    let mut child_folders_with_buckets = Vec::with_capacity(child_folders.len());

                    for child_id in child_folders {
                        if let Some(child_folder) = folders.get(&child_id) {
                            child_folders_with_buckets.push((child_id, child_folder.status.bucket()));
                        }
                    }

                    let folder = FolderDownload::new(
                        folder_task, 
                        &parent_relative_path, 
                        folder_id, 
                        child_files, 
                        child_folders_with_buckets, 
                        parent_id
                    );


                    folders.insert(folder_id, folder);
                }
            },
        }

        Self { 
            id: DownloadId(id),
            url: value.url,
            relative_path: PathBuf::from("./"),
            status: DownloadStatus::Queued,
            root_item,
            files,
            folders,
            name,
            active_operation: None,
        }
    }

    pub const fn url(&self) -> &String {
        &self.url
    }

    pub fn get_file_mut(&mut self, id: &FileId) -> Option<&mut FileDownload> {
        match self.files.get_mut(id) {
            Some(file) => Some(file),
            _ => None,
        }
    }

    pub const fn id(&self) -> DownloadId {
        self.id
    }

    pub const fn files(&self) -> &IndexMap<FileId, FileDownload> {
        &self.files
    }

    pub const fn folders(&self) -> &IndexMap<FolderId, FolderDownload> {
        &self.folders
    }

    pub const fn relative_path(&self) -> &PathBuf {
        &self.relative_path
    }

    pub const fn status(&self) -> DownloadStatus {
        self.status
    }

    pub const fn active_operation(&self) -> Option<ActiveOperation> {
        self.active_operation
    }

    pub const fn name(&self) -> &String {
        &self.name
    }

    pub fn is_completed(&self) -> bool {
        self.status == DownloadStatus::Completed
    }

    pub fn root_item(&self) -> Option<DownloadTypeRef<'_>> {
        match self.root_item {
            ItemId::Folder(folder_id) => self.folders.get(&folder_id).map(|folder| DownloadTypeRef::Folder(folder)),
            ItemId::File(file_id) => self.files.get(&file_id).map(|file| DownloadTypeRef::File(file)),
        }
    }

    pub fn base_item_iter(&self) -> BaseItemIterator<'_> {
        BaseItemIterator::new(&self.files, &self.folders)
    }

    pub fn set_paused(&mut self) -> Vec<ChangedItemStatus> {
        let mut files_to_pause = Vec::new();

        for (&id, file) in &self.files {
            if file.status().can_be_paused() {
                files_to_pause.push(id);
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

    pub fn set_queued(&mut self) -> Vec<ChangedItemStatus> {
        let mut files_to_queue = Vec::new();

        for (&id, file) in &self.files {
            if file.status().can_set_to_queue() {
                files_to_queue.push(id);
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
    
    pub fn set_active_operation(&mut self, active_operation: Option<ActiveOperation>) -> Vec<ChangedItemOperation> {
        let mut changed_items = Vec::new();
        let mut base_items_to_change = Vec::new();

        // We first gather all the base items to update
        for item in self.base_item_iter() {
            if item.active_operation() == active_operation { 
                continue; // Already has the operation we wanted
            }

            base_items_to_change.push(item.id());
        }

        // We iterate over all the base items to change 
        // (non-base items calculate their operation based on all their children)
        for item_id in base_items_to_change {
            let mut current_parent_id = None;
            
            match item_id {
                ItemId::File(file_id) => {
                    if let Some(file) = self.files.get_mut(&file_id) {
                        file.active_operation = active_operation;
                        current_parent_id = file.parent_id();
                        
                        changed_items.push(ChangedItemOperation::File { id: file_id, operation: active_operation });
                    }
                },
                ItemId::Folder(folder_id) => {
                    if let Some(folder) = self.folders.get_mut(&folder_id) {
                        folder.active_operation = active_operation;
                        current_parent_id = folder.parent_id();
                        
                        changed_items.push(ChangedItemOperation::Folder { id: folder_id, operation: active_operation });
                    }
                },
            }

            // Bubble up and update all parents
            while let Some(parent_id) = current_parent_id {
                let new_folder_op = self.calculate_folder_operation(parent_id);
    
                if let Some(folder) = self.folders.get_mut(&parent_id) {
    
                    if folder.active_operation != new_folder_op {
                        folder.active_operation = new_folder_op;
                        
                        changed_items.push(ChangedItemOperation::Folder {
                            id: parent_id,
                            operation: new_folder_op,
                        });
    
                        current_parent_id = folder.parent_id;
                    } else {
                        // If this folder didn't change, its parents won't either.
                        break; 
                    }
                } else {
                    break;
                }
            }
        }

        // If the root item changed, then the download has also changed
        if let Some(root_item) = self.root_item() {
            let new_operation = root_item.active_operation();

            if self.active_operation != new_operation {
                self.active_operation = new_operation;
                changed_items.push(ChangedItemOperation::Download(new_operation));
            }
        }

        changed_items
    }

    fn calculate_folder_operation(&self, folder_id: FolderId) -> Option<ActiveOperation> {
        if let Some(folder) = self.folders.get(&folder_id) {
            for child_file_id in &folder.child_files {
                match self.files.get(child_file_id) {
                    Some(child_file) => {
                        if child_file.active_operation.is_some() {
                            return child_file.active_operation;
                        }
                    }
                    None => {}
                }
            }

            for child_folder_id in &folder.child_folders {
                match self.folders.get(child_folder_id) {
                    Some(child_folder) => {
                        if child_folder.active_operation.is_some() {
                            return child_folder.active_operation;
                        }
                    }
                    None => {}
                }
            }
        }

        None
    }

    pub fn set_file_status(&mut self, id: FileId, status: FileStatus) -> Option<Vec<ChangedItemStatus>> {
        let mut changed_items = Vec::new();

        let (mut current_parent_id, mut previous_status_bucket, mut new_status_bucket) = {
            if let Some(file) = self.files.get_mut(&id) {
                if file.status == status {
                    return None; // No change happened at all
                }

                let prev_bucket = file.status.bucket();
                let new_bucket = status.bucket();

                file.status = status;
                changed_items.push(ChangedItemStatus::File {
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
                if let Some(folder) = self.folders.get_mut(&parent_id) {
                    folder.bucket_counters.decrement(previous_status_bucket);
                    folder.bucket_counters.increment(new_status_bucket);
                    (folder.status, folder.parent_id)
                } else {
                    break; // No more parents to update
                }
            };

            let new_folder_status = {
                if let Some(folder) = self.folders.get(&parent_id) {
                    folder.calculate_status(&self.files, &self.folders)
                } else {
                    break; // No more parents to update
                }
            };

            if let Some(folder) = self.folders.get_mut(&parent_id) {
                folder.status = new_folder_status;
            }

            if previous_folder_status != new_folder_status {
                changed_items.push(ChangedItemStatus::Folder {
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

        if let Some(root_item) = self.root_item() {
            let new_root_status = root_item.status();

            if self.status != new_root_status {
                self.status = new_root_status;
                changed_items.push(ChangedItemStatus::Download(new_root_status));
            }
        }

        Some(changed_items)
    }

    pub fn from_db(row: DownloadRow, files: IndexMap<FileId, FileDownload>, folders: IndexMap<FolderId, FolderDownload>) -> Self {
        let mut status = DownloadStatus::from_db_columns(&row.status, row.failure_reason.as_deref())
            .unwrap_or_default();

        let relative_path = PathBuf::from_io_vec(row.relative_path_raw)
            .unwrap_or_else(|| {
                status = DownloadStatus::Failed(DownloadFailureReason::BadPath);
            
                PathBuf::new()
            });

        let root_item = folders.iter()
            .find(|(_, folder)| folder.parent_id().is_none())
            .map(|(&id, _)| ItemId::Folder(id))
            .or_else(|| {
                files.iter()
                    .find(|(_, file)| file.parent_id().is_none())
                    .map(|(&id, _)| ItemId::File(id))
            })
            .expect("Download loaded from DB has no root item!");


        Self {
            id: DownloadId(row.id as usize),
            url: row.url,
            relative_path,
            active_operation: None,
            status,
            root_item,
            files,
            folders,
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

impl From<FileDownload> for DownloadType {
    fn from(file: FileDownload) -> Self {
        DownloadType::File(file)
    }
}

impl From<FolderDownload> for DownloadType {
    fn from(folder: FolderDownload) -> Self {
        DownloadType::Folder(folder)
    }
}

impl DownloadItem for DownloadType {
    type Id = ItemId;
    type Status = DownloadStatus;
    
    fn parent_id(&self) -> Option<FolderId> {
        match self {
            DownloadType::File(file) => file.parent_id(),
            DownloadType::Folder(folder) => folder.parent_id(),
        }
    }

    fn id(&self) -> ItemId {
        match self {
            DownloadType::File(file) => ItemId::File(file.id()),
            DownloadType::Folder(folder) => ItemId::Folder(folder.id()),
        }
    }

    fn relative_path(&self) -> &PathBuf {
        match self {
            DownloadType::File(file) => file.relative_path(),
            DownloadType::Folder(folder) => folder.relative_path(),
        }
    }

    fn name(&self) -> &str {
        match self {
            DownloadType::File(file) => file.name(),
            DownloadType::Folder(folder) => folder.name(),
        }
    }

    fn active_operation(&self) -> Option<ActiveOperation> {
        match self {
            DownloadType::File(file) => file.active_operation(),
            DownloadType::Folder(folder) => folder.active_operation(),
        }
    }

    fn status(&self) -> Self::Status {
        match self {
            DownloadType::File(file) => file.status().as_download_status(),
            DownloadType::Folder(folder) => folder.status(),
        }
    }

}

#[derive(Debug, Clone)]
pub enum DownloadTypeRef<'a> {
    File(&'a FileDownload),
    Folder(&'a FolderDownload),
}

impl<'a> From<&'a FileDownload> for DownloadTypeRef<'a> {
    fn from(file: &'a FileDownload) -> Self {
        DownloadTypeRef::File(file)
    }
}

impl<'a> From<&'a FolderDownload> for DownloadTypeRef<'a> {
    fn from(folder: &'a FolderDownload) -> Self {
        DownloadTypeRef::Folder(folder)
    }
}

impl<'a> DownloadItem for DownloadTypeRef<'a> {
    type Id = ItemId;
    type Status = DownloadStatus;

    fn parent_id(&self) -> Option<FolderId> {
        match self {
            DownloadTypeRef::File(file) => file.parent_id(),
            DownloadTypeRef::Folder(folder) => folder.parent_id(),
        }
    }

    fn id(&self) -> Self::Id {
        match self {
            DownloadTypeRef::File(file) => ItemId::File(file.id()),
            DownloadTypeRef::Folder(folder) => ItemId::Folder(folder.id()),
        }
    }

    fn relative_path(&self) -> &PathBuf {
        match self {
            DownloadTypeRef::File(file) => file.relative_path(),
            DownloadTypeRef::Folder(folder) => folder.relative_path(),
        }
    }

    fn name(&self) -> &str {
        match self {
            DownloadTypeRef::File(file) => file.name(),
            DownloadTypeRef::Folder(folder) => folder.name(),
        }
    }

    fn active_operation(&self) -> Option<ActiveOperation> {
        match self {
            DownloadTypeRef::File(file) => file.active_operation(),
            DownloadTypeRef::Folder(folder) => folder.active_operation(),
        }
    }

    fn status(&self) -> Self::Status {
        match self {
            DownloadTypeRef::File(file) => file.status().as_download_status(),
            DownloadTypeRef::Folder(folder) => folder.status(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
pub enum ActiveOperation {
    Verifying,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct FileDownload {
    parent_id: Option<FolderId>,
    id: FileId,
    url: Arc<String>,
    file_name: String,
    relative_path: PathBuf,
    status: FileStatus,
    // Active operations are never saved to db. They only exist as transient operations in ram
    active_operation: Option<ActiveOperation>,
    #[serde(serialize_with = "serialize_hash")] 
    hash: Option<u128>,
    #[serde(serialize_with = "serialize_chunks")]
    blocks: BitVec<u8, Msb0>,
    #[serde(skip)]
    chunk_hashes: Vec<Option<[u8; 16]>>,
    size: Option<FileSize>, // None means we haven't gotten the size yet, unknown means the size can't be known until it
    #[serde(skip)]
    /// tracks consecutive retries
    retries: usize, 
}

impl DownloadItem for FileDownload {
    type Id = FileId;
    type Status = FileStatus;
    
    fn parent_id(&self) -> Option<FolderId> {
        self.parent_id
    }

    fn id(&self) -> Self::Id {
        self.id
    }

    fn relative_path(&self) -> &PathBuf {
        &self.relative_path
    }

    fn name(&self) -> &str {
        &self.file_name
    }

    fn active_operation(&self) -> Option<ActiveOperation> {
        self.active_operation
    }

    fn status(&self) -> Self::Status {
        self.status
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
            .field("chunks", &self.blocks.len())
            .finish()
    }
}

impl FileDownload {
    pub(super) fn new(file_task: &FileTask, relative_path: &Path, id: FileId, parent_id: Option<FolderId>) -> Self {
        let relative_path = relative_path.join(file_task.file_name());

        Self { 
            parent_id,
            id,
            url: Arc::new(file_task.url.clone()),
            file_name: file_task.file_name().to_owned(),
            relative_path,
            status: FileStatus::Queued,
            hash: None,
            blocks: BitVec::new(),
            chunk_hashes: Vec::new(),
            size: None,
            retries: 0,
            active_operation: None,
        }
    }

    pub fn from_db(row: DownloadFileRow, mut chunk_hashes: Vec<Option<[u8; 16]>>) -> Self {
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

        if let Some(FileSize::Known(file_size)) = size {
            let expected_chunks = file_size.div_ceil(HASH_CHUNK_SIZE as u64);

            chunk_hashes.resize(expected_chunks as usize, None);
        }

        Self {
            parent_id: row.parent_folder_id.map(|id| FolderId(id as usize)),
            id: FileId(row.file_id as usize),
            url: Arc::new(row.url),
            file_name: row.name,
            relative_path,
            status,
            hash,
            blocks: chunks,
            chunk_hashes,
            size,
            retries: row.retries as usize,
            active_operation: None,
        }
    }

    pub const fn blocks(&self) -> &BitVec<u8, Msb0> {
        &self.blocks
    }

    pub fn blocks_mut(&mut self) -> &mut BitVec<u8, Msb0> {
        &mut self.blocks
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

    pub fn active_operation(&self) -> Option<ActiveOperation> {
        self.active_operation
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
        let chunks = self.blocks();

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

    pub fn must_exist_in_disk(&self) -> bool {
        self.must_exist_with_status(&self.status)
    }

    // This and `must_exist_in_disk` are separate functions to allow the case where
    // a file's status has to be modified but the original status is required to check
    // if the file must have existed.
    pub fn must_exist_with_status(&self, status: &FileStatus) -> bool {
        match status {
            FileStatus::Completed => true,

            // A file should only exist on disk once metadata has been fetched (file size is not None).
            FileStatus::Paused | FileStatus::InProgress | FileStatus::Waiting(_) | FileStatus::Retrying => {
                self.size.is_some() 
            },

            FileStatus::Failed(_) |
            FileStatus::Queued |
            FileStatus::Initializing |
            FileStatus::FetchingMetadata |
            FileStatus::NotFound  => false,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FolderDownload {
    parent_id: Option<FolderId>,
    id: FolderId,
    folder_name: String,
    relative_path: PathBuf,
    active_operation: Option<ActiveOperation>,
    status: DownloadStatus,
    child_files: Vec<FileId>,
    child_folders: Vec<FolderId>,

    // Counters to keep track of children statuses without having to recalculate them
    #[serde(skip)]
    bucket_counters: StateBucketCounters,
}

impl FolderDownload {
    pub(super) fn new(
        folder_task: &FolderTask,
        parent_relative_path: &Path,
        id: FolderId,
        child_files: Vec<(FileId, StatusBucket)>,
        child_folders: Vec<(FolderId, StatusBucket)>,
        parent_id: Option<FolderId>
    ) -> Self {
        let relative_path = parent_relative_path.join(folder_task.folder_name());

        let mut bucket_counters = StateBucketCounters::new();
        
        let mut child_file_ids = Vec::with_capacity(child_files.len());
        let mut child_folder_ids = Vec::with_capacity(child_folders.len());

        for (child_file_id, bucket) in child_files {
            child_file_ids.push(child_file_id);
            bucket_counters.increment(bucket);
        }
        
        for (child_folder_id, bucket) in child_folders {
            child_folder_ids.push(child_folder_id);
            bucket_counters.increment(bucket);
        }

        Self { 
            parent_id,
            id,
            folder_name: folder_task.folder_name().to_owned(),
            relative_path,
            status: DownloadStatus::Queued,
            child_files: child_file_ids,
            child_folders: child_folder_ids,

            bucket_counters,
            active_operation: None,
        }
    }

    pub fn from_db(row: DownloadFolderRow, child_files: Vec<FileId>, child_folders: Vec<FolderId>, bucket_counters: StateBucketCounters) -> Self {
        let mut status = DownloadStatus::from_db_columns(&row.status, row.failure_reason.as_deref())
            .unwrap_or_default();

        let relative_path = PathBuf::from_io_vec(row.relative_path_raw)
            .unwrap_or_else(|| {
                status = DownloadStatus::Failed(DownloadFailureReason::BadPath);
            
                PathBuf::new()
            });

        Self {
            parent_id: row.parent_folder_id.map(|id| FolderId(id as usize)),
            id: FolderId(row.folder_id as usize),
            folder_name: row.name,
            relative_path,
            status: DownloadStatus::from_db_columns(&row.status, row.failure_reason.as_ref().map(|str| str.as_str())).unwrap_or_default(),
            child_files,
            child_folders,
            bucket_counters,
            active_operation: None,
        }
    }

    pub fn calculate_status(&self, files_map: &IndexMap<FileId, FileDownload>, folders_map: &IndexMap<FolderId, FolderDownload>) -> DownloadStatus {
        match self.dominant_status() {
            Some(StatusBucket::Queued) => DownloadStatus::Queued,
            Some(StatusBucket::Initializing) => DownloadStatus::Initializing,
            Some(StatusBucket::Verifying) => DownloadStatus::Verifying,
            Some(StatusBucket::FetchingMetadata) => DownloadStatus::FetchingMetadata,
            Some(StatusBucket::InProgress) => DownloadStatus::InProgress,
            Some(StatusBucket::Retrying) => DownloadStatus::Retrying,
            Some(StatusBucket::Waiting) => DownloadStatus::Waiting,
            Some(StatusBucket::Paused) => DownloadStatus::Paused,
            Some(StatusBucket::Completed) => DownloadStatus::Completed,
            Some(StatusBucket::CompletedWithErrors) => DownloadStatus::CompletedWithErrors,
            Some(StatusBucket::Error) => self.resolve_error_status(files_map, folders_map),
            None if self.child_files().is_empty() && self.child_folders().is_empty() => DownloadStatus::Completed, 
            None => DownloadStatus::CompletedWithErrors, 
        }
    }

    fn dominant_status(&self) -> Option<StatusBucket> {
        let total = self.child_files().len() + self.child_folders().len();

        // No children means we are completed, no dominant status
        if total == 0 {
            return None; 
        }

        // Compile time guard
        // A reminder to update this function if a new StatusBucket gets added
        let _assert_exhaustive = |status| match status {
            StatusBucket::InProgress |
            StatusBucket::FetchingMetadata |
            StatusBucket::Initializing |
            StatusBucket::Retrying |
            StatusBucket::Verifying |
            StatusBucket::Waiting |
            StatusBucket::Queued |
            StatusBucket::Paused |
            StatusBucket::Error |
            StatusBucket::Completed |
            StatusBucket::CompletedWithErrors => (),
        };

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
        // If at least some file is still being verified
        else if self.bucket_counters.get(StatusBucket::Verifying) > 0 {
            Some(StatusBucket::Verifying)
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

    fn resolve_error_status(&self, files_map: &IndexMap<FileId, FileDownload>, folders_map: &IndexMap<FolderId, FolderDownload>) -> DownloadStatus {
        let mut first_error = None;
        let mut multiple_errors = false;

        let mut not_found_files = 0;

        let file_errors = self.child_files.iter().filter_map(|id| {
            let status = files_map.get(id)?.status();
            
            match status {
                FileStatus::NotFound => Some((true, None)),
                FileStatus::Failed(reason) => Some((false, Some(DownloadFailureReason::AllFilesFailed(reason)))),
                _ => None,
            }
        });

        let folder_errors = self.child_folders.iter().filter_map(|id| {
            let status = folders_map.get(id)?.status();
            
            match status {
                DownloadStatus::NotFound => Some((true, None)),
                DownloadStatus::Failed(reason) => Some((false, Some(reason))),
                _ => None,
            }
        });

        for (file_not_found, reason) in file_errors.chain(folder_errors) {
            if file_not_found {
                not_found_files += 1;
                // If we see a file with a different error, we know we found a mix of errors
                // And we can skip the rest.
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

        // If no children files were found, we might not exist ourselves
        let total = self.child_files().len() + self.child_folders().len();
        
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
            if total == 0 {
                return DownloadStatus::Completed;
            }

            // If we still have children, we probably desynced somehow
            DownloadStatus::Failed(DownloadFailureReason::StateDesynchronized)
        }
    }

    pub const fn child_files(&self) -> &Vec<FileId> {
        &self.child_files
    }
    
    pub const fn child_folders(&self) -> &Vec<FolderId> {
        &self.child_folders
    }

    pub fn status(&self) -> DownloadStatus {
        self.status
    }

    pub fn active_operation(&self) -> Option<ActiveOperation> {
        self.active_operation
    }
}

impl DownloadItem for FolderDownload {
    type Id = FolderId;
    type Status = DownloadStatus;
    
    fn parent_id(&self) -> Option<FolderId> {
        self.parent_id
    }

    fn id(&self) -> FolderId {
        self.id
    }

    fn relative_path(&self) -> &PathBuf {
        &self.relative_path
    }

    fn name(&self) -> &str {
        &self.folder_name
    }

    fn active_operation(&self) -> Option<ActiveOperation> {
        self.active_operation
    }

    fn status(&self) -> Self::Status {
        self.status
    }

}
