use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::debug;
use tracing::info;

use crate::download::DownloadId;
use crate::download::FileSize;
use crate::download::DownloadUpdate;
use crate::download::FileDownload;
use crate::download::FolderDownload;
use crate::download::{Download, DownloadItem, DownloadStatus, DownloadType, FileUpdate};
use crate::state_manager::StateManager;

pub enum UiStateEvent {
    AddDownload(Download),
    RemoveDownload(usize), 
    AddUpdate(DownloadUpdate),
}

#[derive(Debug, Clone)]
pub enum FrontendMessage {
    // Sent immediately
    DownloadAdded(Download),
    DownloadRemoved { id: usize },

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

#[derive(Debug)]
pub struct UiStateHandle {
    event_sender: mpsc::UnboundedSender<UiStateEvent>,
    delta_sender: broadcast::Sender<FrontendMessage>,
    shutdown_sender: oneshot::Sender<()>, 
}

impl UiStateHandle {
    pub fn add_download(&self, download: Download) {
        self.event_sender.send(UiStateEvent::AddDownload(download)).unwrap();
    }

    pub fn get_event_sender(&self) -> mpsc::UnboundedSender<UiStateEvent> {
        self.event_sender.clone()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<FrontendMessage> {
        self.delta_sender.subscribe()
    }

    pub fn shutdown(self) {
        let _ = self.shutdown_sender.send(());
    }
}

#[derive(Debug)]
pub struct UiStateManager {
    delta_sender: broadcast::Sender<FrontendMessage>,
    event_receiver: mpsc::UnboundedReceiver<UiStateEvent>,
    event_sender: mpsc::UnboundedSender<UiStateEvent>,
}

impl UiStateManager {
    pub fn new() -> Self {
        let delta_sender = broadcast::Sender::new(1000);
        let (event_sender, event_receiver) = mpsc::unbounded_channel();

        Self { 
            delta_sender,
            event_receiver,
            event_sender,
        }
    }

    pub fn start(self) -> UiStateHandle {
        let mut delta_manager = DeltaManager::new(); 
        let (shutdown_sender, mut shutdown_receiver) = oneshot::channel();

        let delta_sender = self.delta_sender.clone();

        tokio::spawn(async move {
            let mut delta_timer = tokio::time::interval(Duration::from_millis(100));
            let mut event_receiver = self.event_receiver;

            let mut removed_ids: HashSet<usize> = HashSet::new();

            loop {
                tokio::select! {
                    Some(event) = event_receiver.recv() => {
                        match event {
                            UiStateEvent::AddDownload(download) => {
                                removed_ids.remove(&download.id());
                                let _ = delta_sender.send(FrontendMessage::DownloadAdded(download));
                            },
                            UiStateEvent::RemoveDownload(id) => {
                                removed_ids.insert(id);
                                delta_manager.deltas.remove(&id);

                                let _ = delta_sender.send(FrontendMessage::DownloadRemoved { id });
                            },
                            UiStateEvent::AddUpdate(download_update) => {
                                let update_id = match &download_update {
                                    DownloadUpdate::StatusChanged { id, .. } => *id,
                                    DownloadUpdate::FileUpdated { id, .. } => *id,
                                };

                                if removed_ids.contains(&update_id) {
                                    continue;
                                }

                                let force_flush = matches!(download_update, DownloadUpdate::StatusChanged { .. });

                                delta_manager.add_update(download_update);

                                if force_flush {
                                    let _ = delta_sender.send(FrontendMessage::BatchUpdate(delta_manager.drain_deltas()));

                                    delta_timer.reset();
                                }
                            },
                        }
                    }
                    _ = delta_timer.tick() => {
                        if !delta_manager.deltas().is_empty() {
                            _ = delta_sender.send(FrontendMessage::BatchUpdate(delta_manager.drain_deltas()));
                        }
                    }
                    _ = &mut shutdown_receiver => {
                        info!("UI state manager shutting down");
                        break;
                    }
                }
            }
        });

        UiStateHandle {
            event_sender: self.event_sender,
            delta_sender: self.delta_sender,
            shutdown_sender,
        }
    }
}

pub async fn get_snapshot(db_state_manager: &StateManager) -> DownloadSnapshot {
    DownloadSnapshot(db_state_manager.load_downloads().await.unwrap())
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
                let download_diff = self.deltas.entry(*id).or_insert(DownloadDiff::default());
            
                download_diff.status = Some(status);
            },
            DownloadUpdate::FileUpdated { id, file_update } => {
                let download_diff = self.deltas.entry(*id).or_insert(DownloadDiff::default());

                let file_id = file_update.id();
                if let None = download_diff.files.get(&file_id) {
                    let mut file_diff = FileDiff::new();

                    file_diff.update(file_update);

                    download_diff.files.insert(file_id, ItemDiff::File(file_diff));
                } else {
                    let file_diff = download_diff.files.get_mut(&file_id).unwrap();

                    if let ItemDiff::File(file_diff) = file_diff {
                        file_diff.update(file_update);
                    }
                }
            }
        }
    }
}

#[serde_with::skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DownloadDiff {
    url: Option<String>,
    relative_path: Option<PathBuf>,
    status: Option<DownloadStatus>,
    files: HashMap<usize, ItemDiff>,
}

impl From<&Download> for DownloadDiff {
    fn from(download: &Download) -> Self {
        let file_diffs = download.files().into_iter().map(|(&id, download_type)| {
            (id, ItemDiff::from(download_type))
        }).collect();

        DownloadDiff { url: Some(download.url().clone()),
            relative_path: Some(download.relative_path().clone()),
            status: Some(download.status()),
            files: file_diffs,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ItemDiff {
    File(FileDiff),
    Folder(FolderDiff),
}

impl From<&DownloadType> for ItemDiff {
    fn from(download_type: &DownloadType) -> Self {
        match download_type {
            DownloadType::File(file_download) => {
                ItemDiff::File(FileDiff::from(file_download))
            },
            DownloadType::Folder(folder_download) => {
                ItemDiff::Folder(FolderDiff::from(folder_download))
            },
        }
    }
}

#[serde_with::skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FileDiff {
    status: Option<DownloadStatus>,
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

impl From<&FileDownload> for FileDiff {
    fn from(file: &FileDownload) -> Self {
        FileDiff { 
            status: Some(file.status()),
            url: Some(file.url().to_string()),
            file_name: Some(file.name().to_string()),
            relative_path: Some(file.relative_path().clone()),
            hash: file.hash(),
            size: file.size(),
            bytes_downloaded: Some(file.bytes_downloaded()),
        }
    }
}

#[serde_with::skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FolderDiff {
    status: Option<DownloadStatus>,
    folder_name: Option<String>,
    relative_path: Option<PathBuf>,
    children: Option<Vec<usize>>,
}

impl FolderDiff {
    pub fn new() -> Self {
        Self::default()
    }
}

impl From<&FolderDownload> for FolderDiff {
    fn from(folder: &FolderDownload) -> Self {
        Self {
            status: Some(folder.status()),
            folder_name: Some(folder.name().to_owned()),
            relative_path: Some(folder.relative_path().clone()),
            children: Some(folder.children().to_owned())
        }
    }
}

#[derive(Debug, Clone)]
pub struct DownloadSnapshot(pub IndexMap<DownloadId, Download>);

impl Serialize for DownloadSnapshot {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer {
        self.build_tree().serialize(serializer)
    }
}

impl DownloadSnapshot {
    fn build_tree(&self) -> serde_json::Value {
        let downloads = self.0.iter().map(|(_url, download)| {
            download_to_json(download)
        }).collect::<Vec<_>>();

        downloads.into()
    }
}

pub fn download_to_json(download: &Download) -> serde_json::Value {
    let mut files_json = serde_json::to_value(download.files()).unwrap();

    if let Some(files_map) = files_json.as_object_mut() {
        for (_id, file_value) in files_map.iter_mut() {
            if let Some(file_obj) = file_value.as_object_mut() {
                file_obj.remove("chunks"); 
            }
        }
    }

    serde_json::json!({
        "id": download.id(),
        "name": download.name(),
        "status": download.status(),
        "url": download.url(),
        "files": files_json,
    })
}