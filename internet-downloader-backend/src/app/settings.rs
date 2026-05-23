use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::db::rows::{GlobalSettingsRow, HostSettingsRow, JoinedDownloadSettingsRow};
use crate::download::items::{DownloadId, FileId};

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct FileSettings {
    pub speed_limit: Option<u64>,
}

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct DownloadSettings {
    pub speed_limit: Option<u64>,
    pub file_settings: HashMap<FileId, FileSettings>, 
}

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct HostSettings {
    pub speed_limit: Option<u64>,
}

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct AppSettings {
    pub global_speed_limit: Option<u64>,
    pub download_settings: HashMap<DownloadId, DownloadSettings>,
    pub host_settings: HashMap<String, HostSettings>
}

impl AppSettings {
    pub fn new() -> Self {
        Self {
            global_speed_limit: None,
            download_settings: HashMap::new(),
            host_settings: HashMap::new(),
        }
    }

    pub fn global_speed_limit(&self) -> Option<u64> {
        self.global_speed_limit
    }

    pub fn set_global_speed_limit(&mut self, new_speed_limit: Option<u64>) {
        self.global_speed_limit = new_speed_limit;
    }

    pub fn get_download_settings(&self, download_id: DownloadId) -> Option<DownloadSettings> {
        self.download_settings.get(&download_id).cloned()
    }

    pub fn from_db(global_settings_row: GlobalSettingsRow, host_settings_rows: Vec<HostSettingsRow>, joined_download_settings: Vec<JoinedDownloadSettingsRow>) -> Self {
        let mut host_settings = HashMap::new();

        for row in host_settings_rows {
            let host_settings_object = HostSettings {
                speed_limit: row.speed_limit.map(|speed_limit| speed_limit as u64),
            };

            host_settings.insert(row.host, host_settings_object);
        }

        let mut download_settings = HashMap::new();

        for row in joined_download_settings {
            let download_settings_object = download_settings.entry(DownloadId(row.download_id as usize)).or_insert_with(|| 
                DownloadSettings {
                    speed_limit: row.download_speed_limit.map(|speed_limit| speed_limit as u64),
                    file_settings: HashMap::new()
                });

            if let Some(item_id) = row.file_id {
                download_settings_object.file_settings.insert(FileId(item_id as usize), 
                    FileSettings { speed_limit: row.file_speed_limit.map(|speed_limit| speed_limit as u64) }
                );
            }
        }

        Self {
            global_speed_limit: global_settings_row.global_speed_limit.map(|speed_limit| speed_limit as u64),
            download_settings: download_settings,
            host_settings: host_settings,
        }
    }
}
