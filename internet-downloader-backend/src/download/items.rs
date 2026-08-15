use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::fmt::{Debug, Display};
use std::time::Duration;

use bitvec::order::Msb0;
use bitvec::vec::BitVec;
use indexmap::IndexMap;
use os_str_bytes::OsStringBytes;
use serde::{Deserialize, Serialize, Serializer};
use url::{Host, Url};

use crate::client_state_manager::{DownloadSnapshot, FileSnapshot};
use crate::db::rows::{DownloadFileRow, DownloadFolderRow, DownloadRow};
use crate::download::error::{DownloadFailureReason, FileFailureReason};
use crate::download::hosts::{DownloadTask, FileTask, FolderTask, TaskType};
use crate::download::status::{DownloadStatus, FileStatus, StatusBucket, StateBucketCounters};
use crate::download::error::{serialize_hash, serialize_chunks};
use crate::download::supervisor::{BLOCK_SIZE, HASH_CHUNK_SIZE};
use crate::utils::file_utils::{is_valid_file_name, normalize_filename};

#[derive(Debug, Copy, Clone, Deserialize, PartialEq, Eq)]
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

pub trait DownloadItem {
    type Id;
    type Status;
    
    fn parent_id(&self) -> Option<FolderId>;

    fn id(&self) -> Self::Id;

    fn relative_path(&self) -> &PathBuf;

    fn name(&self) -> &str;

    fn active_operation(&self) -> Option<ActiveOperation>;

    fn status(&self) -> Self::Status;
    
    fn is_paused(&self) -> bool;
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

#[derive(Debug, Clone)]
pub enum ChangedItemPause {
    File { id: FileId, is_paused: bool },
    Folder { id: FolderId, is_paused: bool },
    Download(bool), 
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
    
    fn is_paused(&self) -> bool {
        match self {
            BaseItemRef::File(_file_id, file) => file.is_paused(),
            BaseItemRef::Folder(_folder_id, folder) => folder.is_paused(),
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

pub struct FolderChildItems<'a> {
    file_ids: std::slice::Iter<'a, FileId>,
    folder_ids: std::slice::Iter<'a, FolderId>,
    file_map: &'a indexmap::IndexMap<FileId, FileDownload>,
    folder_map: &'a indexmap::IndexMap<FolderId, FolderDownload>,
}

impl<'a> FolderChildItems<'a> {
    pub fn new(folder: &'a FolderDownload, download: &'a Download) -> Self {
        let file_ids = folder.child_files().iter();
        let folder_ids = folder.child_folders().iter();
        let file_map = download.files();
        let folder_map = download.folders();
        
        Self {
            file_ids,
            folder_ids,
            file_map,
            folder_map,
        }
    }
}

impl<'a> Iterator for FolderChildItems<'a> {
    type Item = DownloadTypeRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        // We return all folders first, when no more folders are available
        // we start returning files
        while let Some(folder_id) = self.folder_ids.next() {
            if let Some(folder) = self.folder_map.get(folder_id) {
                return Some(folder.into());
            }
        }

        while let Some(file_id) = self.file_ids.next() {
            if let Some(file) = self.file_map.get(file_id) {
                return Some(file.into());
            }
        }

        None
    }
}

/// Has either a file or folder as the only item in root
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Download {
    id: DownloadId,
    url: String,
    save_path: PathBuf,
    status: DownloadStatus,
    is_paused: bool,
    active_operation: Option<ActiveOperation>,
    root_item: ItemId,
    files: IndexMap<FileId, FileDownload>,
    folders: IndexMap<FolderId, FolderDownload>,
}

impl Download {
    pub fn new(id: usize, value: DownloadTask, save_path: PathBuf) -> Self {
        let mut files = IndexMap::new();
        let mut folders: IndexMap<FolderId, FolderDownload> = IndexMap::new();
        let mut current_file_id = FileId(0);
        let mut current_folder_id = FolderId(0);
        let root_item;

        match value.task_type {
            TaskType::File(file_task) => {
                root_item = ItemId::File(current_file_id);

                let file_download = FileDownload::new(&file_task, &save_path, current_file_id, None);
                
                files.insert(current_file_id, file_download);
            },
            TaskType::Folder(folder_task) => {
                let root_folder_id = current_folder_id;
                root_item = ItemId::Folder(root_folder_id);
                *current_folder_id += 1;

                // Folders need to be created bottom-up, but the data we have is top down
                // so we gather all of the data we need to create each Folder first, and then we create them
                let mut folder_data_stack = Vec::new();
                let mut stack = vec![(&folder_task, save_path.clone(), None, root_folder_id)];

                while let Some((folder_task, parent_relative_path, parent_id, folder_id)) = stack.pop() {
                    let normalized_folder_name = {
                        let mut name = normalize_filename(folder_task.folder_name());
                        if name.is_empty() || is_valid_file_name(&name).is_err() {
                            name = "folder".to_string();
                        }
                        name
                    };
                    
                    let relative_path = parent_relative_path.join(normalized_folder_name);

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
            save_path,
            status: DownloadStatus::Uninitialized,
            is_paused: false,
            root_item,
            files,
            folders,
            active_operation: None,
        }
    }

    pub const fn url(&self) -> &String {
        &self.url
    }

    pub fn get_file(&mut self, id: &FileId) -> Option<&FileDownload> {
        match self.files.get(id) {
            Some(file) => Some(file),
            _ => None,
        }
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
    
    pub fn files_mut(&mut self) -> &mut IndexMap<FileId, FileDownload> {
        &mut self.files
    }

    pub const fn folders(&self) -> &IndexMap<FolderId, FolderDownload> {
        &self.folders
    }

    pub const fn relative_path(&self) -> &PathBuf {
        &self.save_path
    }

    pub const fn status(&self) -> DownloadStatus {
        self.status
    }

    pub const fn active_operation(&self) -> Option<ActiveOperation> {
        self.active_operation
    }
    
    pub const fn is_paused(&self) -> bool {
        self.is_paused
    }

    pub fn name(&self) -> &str {
        match self.root_item {
            ItemId::File(id) => self.files[&id].name(),
            ItemId::Folder(id) => &self.folders[&id].folder_name,
        }
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

    pub fn set_all_files_failed(&mut self, reason: FileFailureReason) -> Vec<ChangedItemStatus> {
        let mut files_to_fail = Vec::new();
    
        for (&id, file) in &self.files {
            if file.status().can_be_failed() {
                files_to_fail.push(id);
            }
        }
    
        let mut all_changes = Vec::new();
    
        for id in files_to_fail {
            if let Some(changes) = self.set_file_status(id, FileStatus::Failed(reason)) {
                all_changes.extend(changes);
            }
        }
    
        all_changes
    }

    pub fn set_paused(&mut self, is_paused: bool) -> Vec<ChangedItemPause> {
        let mut changed_items = Vec::new();   
        
        let files_to_change: Vec<FileId> = self.files().keys().cloned().collect();

        // We iterate over all files 
        for file_id in files_to_change {
            let mut current_parent_id = None;

            if let Some(file) = self.files.get_mut(&file_id) {
                // If the file if it's already in the state we wanted it to be
                if file.is_paused == is_paused {
                    continue;
                }

                file.is_paused = is_paused;
                
                current_parent_id = file.parent_id();
                
                changed_items.push(ChangedItemPause::File { id: file_id, is_paused });
            }

            // Bubble up and update all parents
            while let Some(parent_id) = current_parent_id {
                let new_folder_pause_state = self.calculate_folder_pause_state(parent_id);
    
                if let Some(folder) = self.folders.get_mut(&parent_id) {
    
                    if folder.is_paused != new_folder_pause_state {
                        folder.is_paused = new_folder_pause_state;
                        
                        changed_items.push(ChangedItemPause::Folder {
                            id: parent_id,
                            is_paused: new_folder_pause_state,
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
            let new_pause_state = root_item.is_paused();

            if self.is_paused != new_pause_state {
                self.is_paused = new_pause_state;
                changed_items.push(ChangedItemPause::Download(new_pause_state));
            }
        }

        changed_items
    }
    
    pub fn set_active_operation(&mut self, active_operation: Option<ActiveOperation>) -> Vec<ChangedItemOperation> {
        let mut changed_items = Vec::new();   
        
        let files_to_change: Vec<FileId> = self.files().keys().cloned().collect();

        // We iterate over all files 
        for file_id in files_to_change {
            let mut current_parent_id = None;

            if let Some(file) = self.files.get_mut(&file_id) {
                // If the file status is terminal, we can't have an active operation going,
                // so we skip this file
                if file.status().is_terminal() {
                    continue;
                }
                
                file.active_operation = active_operation;
                current_parent_id = file.parent_id();
                
                changed_items.push(ChangedItemOperation::File { id: file_id, operation: active_operation });
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

    pub fn set_file_active_operation(&mut self, file_id: FileId, active_operation: Option<ActiveOperation>) -> Vec<ChangedItemOperation> {
        let mut changed_items = Vec::new();

        let mut current_parent_id = None;

        if let Some(file) = self.files.get_mut(&file_id) {
            // If the file status is terminal, we can't have an active operation going,
            // so we return the empty list
            if file.status().is_terminal() {
                return changed_items;
            }
            
            file.active_operation = active_operation;
            current_parent_id = file.parent_id();
            
            changed_items.push(ChangedItemOperation::File { id: file_id, operation: active_operation });
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

    fn calculate_folder_pause_state(&self, folder_id: FolderId) -> bool {
        let folder = match self.folders.get(&folder_id) {
            Some(folder) => folder,
            None => return false,
        };
        let mut children = FolderChildItems::new(folder, &self).peekable();

        // We return false if we have no children
        if children.peek().is_none() {
            return false;
        }
    
        children.all(|item| item.is_paused())
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

        let save_path = PathBuf::from_io_vec(row.relative_path_raw)
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
            save_path,
            active_operation: None,
            status,
            is_paused: row.is_paused,
            root_item,
            files,
            folders,
        }
    }
}

impl From<Download> for DownloadSnapshot {
        fn from(download: Download) -> DownloadSnapshot {
            let download_name = download.name().to_owned();
            
            let files: IndexMap<FileId, FileSnapshot> = download.files.into_iter().map(|(id, file)| {
                (id, file.into())
            }).collect();
        
            DownloadSnapshot {
                id: download.id,
                name: download_name,
                url: download.url,
                status: download.status,
                active_operation: download.active_operation,
                is_paused: download.is_paused,
                files,
                folders: download.folders,
            }
        }
}

impl From<&Download> for DownloadSnapshot {
    fn from(download: &Download) -> DownloadSnapshot {
        let download_name = download.name().to_owned();
        
        let files: IndexMap<FileId, FileSnapshot> = download.files
            .iter()
            .map(|(id, file)| (*id, file.into()))
            .collect();
    
        DownloadSnapshot {
            id: download.id,
            name: download_name,
            url: download.url.clone(),
            status: download.status,
            active_operation: download.active_operation,
            is_paused: download.is_paused,
            files,
            folders: download.folders.clone(),
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
            DownloadType::File(file) => file.status().as_download_status(),
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
    
    fn is_paused(&self) -> bool {
        match self {
            DownloadType::File(file) => file.is_paused(),
            DownloadType::Folder(folder) => folder.is_paused(),
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

    fn is_paused(&self) -> bool {
        match self {
            DownloadTypeRef::File(file) => file.is_paused(),
            DownloadTypeRef::Folder(folder) => folder.is_paused(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "state", content = "value")]
pub enum ActiveOperation {
    Verifying,
    Queued,
    Downloading,
    Waiting(Duration),
}

#[derive(Serialize, Deserialize, Clone)]
pub struct FileDownload {
    parent_id: Option<FolderId>,
    id: FileId,
    url: Arc<String>,
    host: Arc<Host>,
    filename: FileName,
    relative_path: PathBuf,
    status: FileStatus,
    is_paused: bool,
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
        &self.filename
    }

    fn active_operation(&self) -> Option<ActiveOperation> {
        self.active_operation
    }

    fn status(&self) -> Self::Status {
        self.status
    }

    fn is_paused(&self) -> bool {
        self.is_paused
    }
}

impl Debug for FileDownload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileDownload")
            .field("id", &self.id)
            .field("url", &self.url)
            .field("file_name", &self.filename)
            .field("relative_path", &self.relative_path)
            .field("status", &self.status)
            .field("hash", &self.hash)
            .field("chunks", &self.blocks.len())
            .finish()
    }
}

impl FileDownload {
    pub(super) fn new(file_task: &FileTask, relative_path: &Path, id: FileId, parent_id: Option<FolderId>) -> Self {
        let filename = FileName::new(file_task.url.clone(), file_task.file_name.clone());
        
        let relative_path = relative_path.join(filename.as_str());
        
        let host = Url::parse(&file_task.url)
            .ok()
            .and_then(|url| url.host().map(|host| host.to_owned()))
            .unwrap_or_else(|| Host::Domain(format!("unknown-host ({})", file_task.url)));
        
        Self { 
            parent_id,
            id,
            url: Arc::new(file_task.url.clone()),
            host: Arc::new(host),
            filename,
            relative_path,
            status: FileStatus::Uninitialized,
            is_paused: false,
            hash: None,
            blocks: BitVec::new(),
            chunk_hashes: Vec::new(),
            size: None,
            retries: 0,
            active_operation: None,
        }
    }

    pub fn from_db(row: DownloadFileRow, mut chunk_hashes: Vec<Option<[u8; 16]>>) -> Self {
        // Recosntruct the filename
        let mut filename = FileName::new(row.url.clone(), row.plugin_hint);
        filename.set_server_name(row.server_name);
        
        // Reconstruct the FileSize
        let size = match row.size_type.as_deref() {
            Some("known") => row
                .size_bytes
                .and_then(|value| u64::try_from(value).ok())
                .map(FileSize::Known),
            Some("unknown") => Some(FileSize::Unknown),

            // If we have anything other than a know or unknown file, it means something corrupted, set it as None to fetch it again
            Some(_) | None => None,
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

        let mut status = FileStatus::from_db_columns(&row.status, row.failure_reason.as_deref()).unwrap_or_default();

        let relative_path = PathBuf::from_io_vec(row.relative_path_raw)
            .unwrap_or_else(|| {
                status = FileStatus::Failed(FileFailureReason::BadPath);
            
                PathBuf::new()
            });

        if let Some(FileSize::Known(file_size)) = size {
            let expected_chunks = file_size.div_ceil(HASH_CHUNK_SIZE as u64);

            chunk_hashes.resize(expected_chunks as usize, None);
        }

        let host = Url::parse(&row.url)
            .ok()
            .and_then(|url| url.host().map(|host| host.to_owned()))
            .unwrap_or_else(|| Host::Domain(format!("unknown-host ({})", row.url)));

        Self {
            parent_id: row.parent_folder_id.map(|id| FolderId(id as usize)),
            id: FileId(row.file_id as usize),
            url: Arc::new(row.url),
            host: Arc::new(host),
            filename,
            relative_path,
            status,
            is_paused: row.is_paused,
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

    pub fn host(&self) -> Arc<Host> {
        self.host.clone()
    }
    
    pub fn host_ref(&self) -> &Host {
        self.host.as_ref()
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

    pub fn set_file_name(&mut self, filename: String) {
        self.filename.set_server_name(Some(filename));
        
        if let Some(parent_path) = self.relative_path.parent() {
            self.relative_path = parent_path.join(&self.filename.as_str());
        }
    }
    
    pub const fn filename(&self) -> &FileName {
        &self.filename
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
            FileStatus::Partial |
            FileStatus::Completed => true,

            FileStatus::Uninitialized |
            FileStatus::MetadataFetched |
            FileStatus::Failed(_) |
            FileStatus::NotFound  => false,
        }
    }
}

impl From<FileDownload> for FileSnapshot {
    fn from(file: FileDownload) -> Self {
        
        let bytes_downloaded = file.calculate_initial_bytes(BLOCK_SIZE as u64);
        
        FileSnapshot {
            id: file.id,
            parent_id:
            file.parent_id,
            file_name: file.filename.as_str().to_owned(),
            relative_path: file.relative_path,
            size: file.size,
            bytes_downloaded,
            status: file.status,
            active_operation: file.active_operation,
            is_paused: file.is_paused,
            url: file.url,
            host: file.host.to_string(),
        }
    }
}

impl From<&FileDownload> for FileSnapshot {
    fn from(file: &FileDownload) -> Self {
        FileSnapshot {
            id: file.id,
            parent_id: file.parent_id,
            file_name: file.filename.as_str().to_owned(),
            relative_path: file.relative_path.clone(),
            size: file.size,
            bytes_downloaded: file.calculate_initial_bytes(BLOCK_SIZE as u64),
            status: file.status,
            active_operation: file.active_operation,
            is_paused: file.is_paused,
            url: Arc::clone(&file.url),
            host: file.host.to_string(),
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
    is_paused: bool,
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
        let original_name = folder_task.folder_name().to_owned();

        let mut folder_name = normalize_filename(&original_name);
        if folder_name.is_empty() || is_valid_file_name(&folder_name).is_err() {
            folder_name = "folder".to_string();
        }
        
        let relative_path = parent_relative_path.join(&folder_name);

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
            folder_name: folder_name,
            relative_path,
            status: DownloadStatus::Uninitialized,
            is_paused: false,
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
            status,
            is_paused: row.is_paused,
            child_files,
            child_folders,
            bucket_counters,
            active_operation: None,
        }
    }

    pub fn calculate_status(&self, files_map: &IndexMap<FileId, FileDownload>, folders_map: &IndexMap<FolderId, FolderDownload>) -> DownloadStatus {
        match self.dominant_status() {
            Some(StatusBucket::Uninitialized) => DownloadStatus::Uninitialized,
            Some(StatusBucket::MetadataFetched) => DownloadStatus::MetadataFetched,
            Some(StatusBucket::Partial) => DownloadStatus::Partial,
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
            StatusBucket::Uninitialized |
            StatusBucket::MetadataFetched |
            StatusBucket::Partial |
            StatusBucket::Error |
            StatusBucket::Completed |
            StatusBucket::CompletedWithErrors => (),
        };

        // Active states, if any of any children has an active state, we adopt the state too
        // Order is important
        // If anything is downloading, the folder is downloading
        if self.bucket_counters.get(StatusBucket::Partial) > 0 {
            Some(StatusBucket::Partial)
        } 
        // If all children share the same status, we too share it
        else if self.bucket_counters.get(StatusBucket::Uninitialized) == total {
            Some(StatusBucket::Uninitialized)
        } else if self.bucket_counters.get(StatusBucket::MetadataFetched) == total {
            Some(StatusBucket::MetadataFetched)
        } else if self.bucket_counters.get(StatusBucket::Error) == total {
            Some(StatusBucket::Error)
        } else if self.bucket_counters.get(StatusBucket::Completed) == total {
            Some(StatusBucket::Completed)
        } else if self.bucket_counters.get(StatusBucket::CompletedWithErrors) == total {
            Some(StatusBucket::CompletedWithErrors)
        } 
        // Mixed statuses
        else if self.bucket_counters.get(StatusBucket::Uninitialized) > 0 &&
            self.bucket_counters.get(StatusBucket::MetadataFetched) > 0 
        {
            Some(StatusBucket::MetadataFetched)
        } 
        // If we have a mix of files that are still not downloaded, but without errors, 
        // and files that have been completed, we are still in a partially downloaded status
        else if self.bucket_counters.get(StatusBucket::MetadataFetched) > 0 ||
            self.bucket_counters.get(StatusBucket::Uninitialized) > 0 
        {
            Some(StatusBucket::Partial)
        } 
        // If we have any mix of errors
        else if self.bucket_counters.get(StatusBucket::Error) > 0
            || self.bucket_counters.get(StatusBucket::CompletedWithErrors) > 0
        {
            return Some(StatusBucket::CompletedWithErrors);
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
    
    fn is_paused(&self) -> bool {
        self.is_paused
    }
}

fn filename_from_url(url: &str) -> Option<String> {
    let url_base = url
        .split('#').next().unwrap_or(url)
        .split('?').next().unwrap_or(url)
        .rsplit('/')
        .find(|segment| !segment.is_empty());

    url_base.map(|url_base| url_base.to_string())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileName {
    plugin_hint: Option<String>,
    server_name: Option<String>,
    url_hint: Option<String>,
}

impl FileName {
    fn new(url: String, plugin_hint: Option<String>) -> Self {
        // We normalize all names
        // and make sure they are not empty
        // if they are empty, they are set back to None
        let url_hint = filename_from_url(&url)
            .map(|url_hint| normalize_filename(&url_hint))
            .filter(|url_hint| !url_hint.is_empty());
        
        let plugin_hint = plugin_hint
            .map(|plugin_hint| normalize_filename(&plugin_hint))
            .filter(|plugin_hint| !plugin_hint.is_empty());     
        
        Self {
            plugin_hint,
            server_name: None,
            url_hint,
        }
    }

    pub fn set_server_name(&mut self, name: Option<String>) {
        let server_name = name
            .map(|name| normalize_filename(&name))
            .filter(|server_name| !server_name.is_empty());
        
        self.server_name = server_name;
    }

    pub fn as_str(&self) -> &str {
        self.plugin_hint.as_deref()
            .or_else(|| self.server_name.as_deref())
            .or_else(|| self.url_hint.as_deref())
            .unwrap_or_else(|| "download")
    }

    pub fn plugin_hint(&self) -> Option<&str> {
        self.plugin_hint.as_deref()
    }
    
    pub fn server_name(&self) -> Option<&str> {
        self.server_name.as_deref()
    }
    
    pub fn url_hint(&self) -> Option<&str> {
        self.url_hint.as_deref()
    }
}

impl Deref for FileName {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl AsRef<str> for FileName {
    fn as_ref(&self) -> &str {
        self.deref()
    }
}
