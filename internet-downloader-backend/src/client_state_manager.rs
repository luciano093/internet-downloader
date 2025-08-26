use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::download::{Download, DownloadItem, DownloadType, DownloadUpdate};

#[derive(Debug)]
pub struct UiStateManager {
    sender: mpsc::UnboundedSender<DownloadDelta>,
}

impl UiStateManager {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();

        Self { 
            sender,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DownloadDelta {

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

            let root_item_id = download.files().iter().find_map(|(&id, file_type)| 
                if file_type.parent_id().is_none() {
                    Some(id)
                } else {
                    None
                }
            )
            .expect("Download must have exactly one root item");
        
            let mut download_json = serde_json::json!({
                "url": download.url(),
                "host": download.host(),
                "status": download.status(),
            });
            
            let root_item = self.build_node(download, root_item_id);

            match download.files().get(&root_item_id).unwrap() {
                DownloadType::File(_) => {
                    download_json["file"] = root_item;
                },
                DownloadType::Folder(_) => {
                    download_json["folder"] = root_item;
                },
            };

            download_json
        }).collect::<Vec<_>>();

        serde_json::json!({
            "downloads": downloads
        })
    }

    fn build_node(&self, download: &Download, id: usize) -> serde_json::Value {
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
                    let node = self.build_node(download, child_id);
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
}