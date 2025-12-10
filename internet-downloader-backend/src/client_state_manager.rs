use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::download::hosts::Host;
use crate::download::DownloadUpdate;
use crate::download::FileDownload;
use crate::download::FolderDownload;
use crate::download::{Download, DownloadItem, DownloadStatus, DownloadType, FileUpdate};
use crate::state_manager::StateManager;

pub enum UiStateEvent {
    AddDownload(Download),
    AddFile(Download),
    AddUpdate(DownloadUpdate),
}

#[derive(Debug)]
pub struct UiStateHandle {
    event_sender: mpsc::UnboundedSender<UiStateEvent>,
    delta_sender: broadcast::Sender<DownloadDeltaMap>,
    shutdown_sender: oneshot::Sender<()>, 
}

impl UiStateHandle {
    pub fn add_download(&self, download: Download) {
        self.event_sender.send(UiStateEvent::AddDownload(download)).unwrap();
    }

    pub fn get_event_sender(&self) -> mpsc::UnboundedSender<UiStateEvent> {
        self.event_sender.clone()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<DownloadDeltaMap> {
        self.delta_sender.subscribe()
    }

    pub fn shutdown(self) {
        let _ = self.shutdown_sender.send(());
    }
}

#[derive(Debug)]
pub struct UiStateManager {
    delta_sender: broadcast::Sender<DownloadDeltaMap>,
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

            loop {
                tokio::select! {
                    Some(event) = event_receiver.recv() => {
                        match event {
                            UiStateEvent::AddDownload(download) => {
                                delta_manager.add_download(&download);
                            },
                            UiStateEvent::AddFile(download) => todo!(),
                            UiStateEvent::AddUpdate(download_update) => {
                                delta_manager.add_update(download_update);
                            },
                        }
                    }
                    _ = delta_timer.tick() => {
                        if delta_manager.deltas().len() > 0 {
                            _ = delta_sender.send(delta_manager.drain_deltas());
                        }
                    }
                    _ = &mut shutdown_receiver => {
                        println!("UI state manager shutting down");
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
pub struct DownloadDeltaMap(pub HashMap<usize, DownloadDelta>);

impl Serialize for DownloadDeltaMap {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer {
        let vec = self.0.iter().map(|(id, delta)| {
            let (action, json) = match delta {
                DownloadDelta::DownloadAdded(download_diff) => {
                    ("added", serde_json::to_value(download_diff).unwrap())
                },
                DownloadDelta::DownloadModified(download_diff) => {
                    ("modified", serde_json::to_value(download_diff).unwrap())
                },
            };

            let mut delta_json = serde_json::json!({
                "id": id,
                "action": action,
            });

            if action == "added" {
                delta_json["download"] = json;
            } else {
                delta_json["changes"] = json;
            }

            delta_json
        }).collect::<Vec<_>>();

        serde_json::json!({
            "deltas": vec,
        }).serialize(serializer)
    }
}

#[derive(Debug)]
pub struct DeltaManager {
    deltas: HashMap<usize, DownloadDelta>, // download id to download delta
}

impl DeltaManager {
    pub fn new() -> Self {
        Self { deltas: HashMap::new() }
    }

    pub const fn deltas(&self) -> &HashMap<usize, DownloadDelta> {
        &self.deltas
    }

    fn drain_deltas(&mut self) -> DownloadDeltaMap {
        DownloadDeltaMap(std::mem::take(&mut self.deltas))
    }
    
    pub fn add_download(&mut self, download: &Download) {
        self.deltas.insert(download.id(), DownloadDelta::DownloadAdded(DownloadDiff::from(download)));
    }

    pub fn add_update(&mut self, download_update: DownloadUpdate) {
        match download_update {
            DownloadUpdate::StatusChanged { id, status } => {
                match self.deltas.get_mut(&id).unwrap() {
                    DownloadDelta::DownloadAdded(_) => {
                        todo!()
                    },
                    DownloadDelta::DownloadModified(download_diff) => {
                        download_diff.status = Some(status);
                    },
                }
            },
            DownloadUpdate::FileUpdated { id, file_update } => match self.deltas.entry(id).or_insert(DownloadDelta::DownloadModified(DownloadDiff::default())) {
                    DownloadDelta::DownloadAdded(download_diff) => {
                        if let None = download_diff.files.get(&id) {
                            todo!()
                        } else {
                            let file_diff = download_diff.files.get_mut(&id).unwrap();

                            match file_diff {
                                ItemDelta::ItemAdded(item_diff) => {
                                    if let ItemDiff::File(file_diff) = item_diff {
                                        file_diff.update(file_update);
                                    }
                                },
                                ItemDelta::ItemModified(item_diff) => {
                                    if let ItemDiff::File(file_diff) = item_diff {
                                        file_diff.update(file_update);
                                    }
                                },
                            }
                        }
                    },
                    DownloadDelta::DownloadModified(download_diff) => {
                        if let None = download_diff.files.get(&id) {
                            let mut file_diff = FileDiff::new();

                            file_diff.update(file_update);

                            download_diff.files.insert(id, ItemDelta::ItemModified(ItemDiff::File(file_diff)));
                        } else {
                            let file_diff = download_diff.files.get_mut(&id).unwrap();

                            match file_diff {
                                ItemDelta::ItemAdded(download_type) => {
                                    todo!()
                                },
                                ItemDelta::ItemModified(item_diff) => {
                                    if let ItemDiff::File(file_diff) = item_diff {
                                        file_diff.update(file_update);
                                    }
                                },
                            }
                            
                        }
                    },
                },
        }
    }
}

#[derive(Debug, Clone)]
pub enum DownloadDelta {
    DownloadAdded(DownloadDiff),
    DownloadModified(DownloadDiff),
}

impl Serialize for DownloadDelta {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer {
        match self {
            DownloadDelta::DownloadAdded(download_diff) => {
                serde_json::to_value(download_diff).unwrap().serialize(serializer)
            },
            DownloadDelta::DownloadModified(download_diff) => todo!(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DownloadDiff {
    url: Option<String>,
    relative_path: Option<PathBuf>,
    host: Option<Host>,
    status: Option<DownloadStatus>,
    files: HashMap<usize, ItemDelta>,
}

impl From<&Download> for DownloadDiff {
    fn from(download: &Download) -> Self {
        let file_diffs = download.files().into_iter().map(|(&id, download_type)| {
            (id, ItemDelta::ItemAdded(ItemDiff::from(download_type)))
        }).collect();

        DownloadDiff { url: Some(download.url().clone()),
            relative_path: Some(download.relative_path().clone()),
            host: Some(download.host()),
            status: Some(download.status()),
            files: file_diffs,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ItemDelta {
    ItemAdded(ItemDiff),
    ItemModified(ItemDiff),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FileDiff {
    status: Option<DownloadStatus>,
    url: Option<String>,
    file_name: Option<String>,
    relative_path: Option<PathBuf>,
    hash: Option<u128>,
    progress: Option<f64>,
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
            FileUpdate::Progress { progress, .. } => { 
                self.progress = Some(progress)
            },
            FileUpdate::FileSize { .. } => { },
        }
    }
}

impl From<&FileDownload> for FileDiff {
    fn from(file: &FileDownload) -> Self {
        FileDiff { 
            status: Some(file.status()),
            url: Some(file.url().clone()),
            file_name: Some(file.name().to_string()),
            relative_path: Some(file.relative_path().clone()),
            hash: file.hash(),
            progress: Some(file.get_progress_percent())
        }
    }
}

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
pub struct DownloadSnapshot(pub IndexMap<String, Download>);

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

        serde_json::json!({
            "downloads": downloads
        })
    }
}

pub fn download_to_json(download: &Download) -> serde_json::Value {
    let root_item_id = download.files().iter().find_map(|(&id, file_type)| 
        if file_type.parent_id().is_none() {
            Some(id)
        } else {
            None
        }
    )
    .expect("Download must have exactly one root item");

    let mut download_json = serde_json::json!({
        "id": download.id(),
        "url": download.url(),
        "host": download.host(),
        "status": download.status(),
    });
    
    let root_item = build_json_download_node(download, root_item_id);

    match download.files().get(&root_item_id).unwrap() {
        DownloadType::File(_) => {
            download_json["file"] = root_item;
        },
        DownloadType::Folder(_) => {
            download_json["folder"] = root_item;
        },
    };

    download_json
}

fn build_json_download_node(download: &Download, id: usize) -> serde_json::Value {
        match &download.files().get(&id).unwrap() {
            DownloadType::File(file_download) => {
                
                serde_json::json!({
                    "name": file_download.name(),
                    "status": file_download.status(),
                    "url": file_download.url(),
                    "hash": file_download.hash().as_ref().map(|hash| hash.to_string()),
                    "progress": format!("{:.1}%", download.get_progress_percent(&file_download.id())),
                })
            },
            DownloadType::Folder(folder_download) => {
                let mut files = Vec::new();
                let mut subfolders = Vec::new();

                for &child_id in folder_download.children() {
                    let node = build_json_download_node(download, child_id);
                    match download.files().get(&child_id).unwrap() {
                        DownloadType::File(_) => files.push(node),
                        DownloadType::Folder(_) => subfolders.push(node),
                    }
                }
                
                serde_json::json!({
                    "name": folder_download.name(),
                    "status": folder_download.status(),
                    "files": files,
                    "subfolders": subfolders,
                    "progress": format!("{:.1}%", download.get_progress_percent(&folder_download.id())),
                })
            },
        }
    }