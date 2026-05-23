use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::app_manager::FileSize;
use crate::download::items::ActiveOperation;
use crate::download::items::DownloadId;
use crate::download::items::DownloadItem;
use crate::download::items::FileId;
use crate::download::items::FolderDownload;
use crate::download::items::FolderId;
use crate::download::items::Download;
use crate::download::status::DownloadStatus;
use crate::download::status::FileStatus;
use crate::db::state_manager::StateManager;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum DownloadUpdate {
    StatusChanged { id: DownloadId, status: DownloadStatus },
    OperationChanged { id: DownloadId, operation: Option<ActiveOperation> },
    ItemUpdated { id: DownloadId, item_update: ItemUpdate }, 
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum ItemUpdate {
    File(FileUpdate),
    Folder(FolderUpdate),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum FileUpdate {
    Status { id: FileId, status: FileStatus },
    Operation { id: FileId, operation: Option<ActiveOperation> },
    Hash { id: FileId, hash: u128 },
    FileSize { id: FileId, len: u64 },
    BytesDownloaded { id: FileId, len: u64 },
}

impl FileUpdate {
    pub fn id(&self) -> FileId {
        match self {
            FileUpdate::Status { id, .. } => *id,
            FileUpdate::Operation { id, .. } => *id,
            FileUpdate::Hash { id, .. } => *id,
            FileUpdate::FileSize { id, .. } => *id,
            FileUpdate::BytesDownloaded { id, .. } => *id,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum FolderUpdate {
    Status { id: FolderId, status: DownloadStatus },
    Operation { id: FolderId, operation: Option<ActiveOperation> },
}

pub enum UiStateEvent {
    AddDownload(Download),
    RemoveDownload(DownloadId), 
    AddUpdate(DownloadUpdate),
}

#[derive(Debug, Clone)]
pub enum FrontendMessage {
    // Sent immediately
    DownloadAdded(Download),
    DownloadRemoved { id: DownloadId },

    // Sent on flush interval
    BatchUpdate(DownloadDeltaMap),
}

impl Serialize for FrontendMessage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer {
        match self {
            FrontendMessage::DownloadAdded(download) => {
                serde_json::json!({
                    "id": download.id(),
                    "action": "added",
                    "download": download,
                }).serialize(serializer)
            },
            FrontendMessage::DownloadRemoved { id } => {
                serde_json::json!({
                    "id": id,
                    "action": "deleted",
                }).serialize(serializer)
            },
            FrontendMessage::BatchUpdate(download_delta_map) => {
                serde_json::json!({
                    "action": "changes",
                    "changes": download_delta_map,
                }).serialize(serializer)
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct UiManagerHandle {
    event_sender: mpsc::UnboundedSender<UiStateEvent>,
    delta_sender: broadcast::Sender<FrontendMessage>,
    cancel_token: CancellationToken,
}

impl UiManagerHandle {
    pub fn new() -> Self {
        let delta_sender = broadcast::Sender::new(1000);
        let (event_sender, event_receiver) = mpsc::unbounded_channel();
        let cancel_token = CancellationToken::new();

        let ui_manager = UiManager::new(delta_sender.clone(), event_receiver, cancel_token.clone());

        tokio::spawn(async move {
            ui_manager.run().await;
        });

        UiManagerHandle { 
            event_sender, 
            delta_sender, 
            cancel_token,
        }
    }
    
    pub fn add_download(&self, download: Download) {
        let _ = self.event_sender.send(UiStateEvent::AddDownload(download));
    }
    
    pub fn remove_download(&self, download_id: DownloadId) {
        let _ = self.event_sender.send(UiStateEvent::RemoveDownload(download_id));
    }

    pub fn get_event_sender(&self) -> mpsc::UnboundedSender<UiStateEvent> {
        self.event_sender.clone()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<FrontendMessage> {
        self.delta_sender.subscribe()
    }

    pub fn shutdown(self) {
        self.cancel_token.cancel();
    }
}

#[derive(Debug)]
pub struct UiManager {
    delta_sender: broadcast::Sender<FrontendMessage>,
    event_receiver: mpsc::UnboundedReceiver<UiStateEvent>,
    cancel_token: CancellationToken,
}

impl UiManager {
    pub fn new(delta_sender: broadcast::Sender<FrontendMessage>, event_receiver: mpsc::UnboundedReceiver<UiStateEvent>, cancel_token: CancellationToken,) -> Self {
        Self { 
            delta_sender,
            event_receiver,
            cancel_token,
        }
    }

    pub async fn run(self) {
        let mut delta_manager = DeltaManager::new(); 

        let mut delta_timer = tokio::time::interval(Duration::from_millis(100));
        let mut event_receiver = self.event_receiver;

        let mut removed_ids: HashSet<DownloadId> = HashSet::new();

        loop {
            tokio::select! {
                Some(event) = event_receiver.recv() => {
                    match event {
                        UiStateEvent::AddDownload(download) => {
                            removed_ids.remove(&download.id());
                            let _ = self.delta_sender.send(FrontendMessage::DownloadAdded(download));
                        },
                        UiStateEvent::RemoveDownload(id) => {
                            removed_ids.insert(id);
                            delta_manager.deltas.remove(&id);

                            let _ = self.delta_sender.send(FrontendMessage::DownloadRemoved { id });
                        },
                        UiStateEvent::AddUpdate(download_update) => {
                            let update_id = match &download_update {
                                DownloadUpdate::StatusChanged { id, .. } => *id,
                                DownloadUpdate::ItemUpdated { id, .. } => *id,
                                DownloadUpdate::OperationChanged { id, .. } => *id,
                            };

                            if removed_ids.contains(&update_id) {
                                continue;
                            }

                            let force_flush = matches!(download_update, DownloadUpdate::StatusChanged { .. });

                            delta_manager.add_update(download_update);

                            if force_flush {
                                let _ = self.delta_sender.send(FrontendMessage::BatchUpdate(delta_manager.drain_deltas()));

                                delta_timer.reset();
                            }
                        },
                    }
                }
                _ = delta_timer.tick() => {
                    if !delta_manager.deltas().is_empty() {
                        _ = self.delta_sender.send(FrontendMessage::BatchUpdate(delta_manager.drain_deltas()));
                    }
                }
                _ = self.cancel_token.cancelled() => {
                    info!("UI state manager shutting down");
                    break;
                }
            }
        }
    }
}

pub async fn get_snapshot(db_manager: &StateManager) -> IndexMap<DownloadId, Download> {
    db_manager.load_downloads().await.unwrap()
}

#[derive(Debug, Clone)]
pub struct DownloadDeltaMap(pub HashMap<usize, DownloadDiff>);

impl Serialize for DownloadDeltaMap {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer {
        let vec = self.0.iter().map(|(id, delta)| {
            let mut json = serde_json::to_value(delta).unwrap();
            json["id"] = serde_json::to_value(id).unwrap();

            json
        }).collect::<Vec<_>>();

        vec.serialize(serializer)
    }
}

#[derive(Debug)]
pub struct DeltaManager {
    deltas: HashMap<usize, DownloadDiff>, // download id to download delta
}

impl DeltaManager {
    pub fn new() -> Self {
        Self { deltas: HashMap::new() }
    }

    pub const fn deltas(&self) -> &HashMap<usize, DownloadDiff> {
        &self.deltas
    }

    fn drain_deltas(&mut self) -> DownloadDeltaMap {
        DownloadDeltaMap(std::mem::take(&mut self.deltas))
    }

    pub fn add_update(&mut self, download_update: DownloadUpdate) {
        match download_update {
            DownloadUpdate::StatusChanged { id, status } => {
                let download_diff = self.deltas.entry(*id).or_default();
                
                download_diff.status = Some(status);
            },
            DownloadUpdate::OperationChanged { id, operation } => {
                let download_diff = self.deltas.entry(*id).or_default();
                
                download_diff.active_operation = Some(operation);
            },
            DownloadUpdate::ItemUpdated { id, item_update } => {
                let download_diff = self.deltas.entry(*id).or_default();

                    match item_update {
                        ItemUpdate::File(file_update) => {
                            let file_id = match &file_update {
                                FileUpdate::Status { id, .. } => *id,
                                FileUpdate::Operation { id, .. } => *id,
                                FileUpdate::Hash { id, .. } => *id,
                                FileUpdate::FileSize { id, .. } => *id,
                                FileUpdate::BytesDownloaded { id, .. } => *id,
                            };

                            let file_diff = download_diff.files.entry(file_id).or_insert_with(|| {
                                FileDiff::new()
                            });

                            file_diff.update(file_update);
                        },
                        ItemUpdate::Folder(folder_update) => {
                            let folder_id = match &folder_update {
                                FolderUpdate::Status { id, .. } => *id,
                                FolderUpdate::Operation { id, .. } => *id,
                            };

                            let folder_diff = download_diff.folders.entry(folder_id).or_insert_with(|| {
                                FolderDiff::new()
                            });

                            folder_diff.update(folder_update);
                        }
                    }
            }
        }
    }
}

impl Default for DeltaManager {
    fn default() -> Self {
        Self::new()
    }
}

#[serde_with::skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DownloadDiff {
    url: Option<String>,
    relative_path: Option<PathBuf>,
    status: Option<DownloadStatus>,
    active_operation: Option<Option<ActiveOperation>>,
    files: HashMap<FileId, FileDiff>,
    folders: HashMap<FolderId, FolderDiff>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ItemDiff {
    File(FileDiff),
    Folder(FolderDiff),
}

#[serde_with::skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FileDiff {
    parent_id: Option<Option<FolderId>>, 
    status: Option<FileStatus>,
    active_operation: Option<Option<ActiveOperation>>,
    url: Option<String>,
    file_name: Option<String>,
    relative_path: Option<PathBuf>,
    hash: Option<u128>,
    size: Option<FileSize>,
    bytes_downloaded: Option<u64>,
}

impl FileDiff {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update(&mut self, update: FileUpdate) {
        match update {
            FileUpdate::Status { status, .. } => {
                self.status = Some(status)
            },
            FileUpdate::Operation { operation, .. } => {
                self.active_operation = Some(operation)
            },
            FileUpdate::Hash { hash, .. } => {
                self.hash = Some(hash)
            },
            FileUpdate::FileSize { len, .. } => { 
                self.size = Some(FileSize::Known(len)) 
            },
            FileUpdate::BytesDownloaded { len, .. } => {
                self.bytes_downloaded = Some(len)
            },
        }
    }
}

#[serde_with::skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FolderDiff {
    parent_id: Option<Option<FolderId>>, 
    status: Option<DownloadStatus>,
    active_operation: Option<Option<ActiveOperation>>,
    folder_name: Option<String>,
    relative_path: Option<PathBuf>,
    child_files: Option<Vec<FileId>>,
    child_folders: Option<Vec<FolderId>>,
}

impl FolderDiff {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update(&mut self, update: FolderUpdate) {
        match update {
            FolderUpdate::Status { status, .. } => {
                self.status = Some(status)
            },
            FolderUpdate::Operation { operation, .. } => {
                self.active_operation = Some(operation)
            },
        }
    }
}

impl From<&FolderDownload> for FolderDiff {
    fn from(folder: &FolderDownload) -> Self {
        Self {
            parent_id: Some(folder.parent_id()),
            status: Some(folder.status()),
            active_operation: Some(folder.active_operation()),
            folder_name: Some(folder.name().to_owned()),
            relative_path: Some(folder.relative_path().clone()),
            child_files: Some(folder.child_files().to_owned()),
            child_folders: Some(folder.child_folders().to_owned()),
        }
    }
}

#[derive(Serialize)]
pub struct DownloadSnapshot {
    pub id: DownloadId,
    pub name: String,
    pub url: String,
    pub status: DownloadStatus,
    pub active_operation: Option<ActiveOperation>,
    pub files: IndexMap<FileId, FileSnapshot>,
    pub folders: IndexMap<FolderId, FolderDownload>,
}

#[derive(Serialize)]
pub struct FileSnapshot {
    pub id: FileId,
    pub parent_id: Option<FolderId>,
    pub file_name: String,
    pub relative_path: PathBuf,
    pub size: Option<FileSize>,
    pub bytes_downloaded: u64,
    pub status: FileStatus,
    pub active_operation: Option<ActiveOperation>,
    pub url: Arc<String>,
}
