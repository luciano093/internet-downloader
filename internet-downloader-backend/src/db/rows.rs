use indexmap::IndexMap;

use crate::download::{items::{Download, DownloadType, FileDownload, FolderDownload}, status::StateBucketCounters};

#[derive(sqlx::FromRow)]
pub struct DownloadRow {
    pub id: i64,
    pub url: String,
    pub name: String,
    pub relative_path_raw: Vec<u8>,
    pub relative_path: String,
    pub status: String,
    pub failure_reason: Option<String>,
}

impl DownloadRow {
    pub fn into_download(self, files: IndexMap<usize, DownloadType>) -> Download {
        Download::from_db(self, files)
    }
}

#[derive(sqlx::FromRow)]
pub struct DownloadItemRow {
    pub download_id: i64,
    pub item_id: i64,
    pub parent_id: Option<i64>,
    pub item_type: String, // 'file' or 'folder'
    
    // Shared
    pub name: String,
    pub relative_path_raw: Vec<u8>,
    pub relative_path: String,
    pub status: String,
    pub failure_reason: Option<String>,
    
    // File specific
    pub url: Option<String>,
    pub hash: Option<Vec<u8>>,
    pub chunks_raw: Option<Vec<u8>>,
    pub chunks_len: Option<i64>,
    pub size_type: Option<String>,
    pub size_bytes: Option<i64>,
    pub retries: i64,
    pub wait_time: Option<i64>,
}

impl DownloadItemRow {
    pub fn into_download_type(self, children: Vec<usize>, buckets: StateBucketCounters) -> DownloadType {
        if self.item_type == "folder" {
            DownloadType::Folder(FolderDownload::from_db(self, children, buckets))
        } else {
            DownloadType::File(FileDownload::from_db(self))
        }
    }
}

#[derive(sqlx::FromRow)]
pub struct GlobalSettingsRow {
    pub global_speed_limit: Option<i64>,
}

#[derive(sqlx::FromRow)]
pub struct HostSettingsRow {
    pub host: String,
    pub speed_limit: Option<i64>,
}

#[derive(sqlx::FromRow)]
pub struct DownloadSettingsRow {
    pub download_id: i64,
    pub speed_limit: Option<i64>,
}

#[derive(sqlx::FromRow)]
pub struct FileSettingsRow {
    pub download_id: i64,
    pub item_id: i64,
    pub speed_limit: Option<i64>,
}

#[derive(sqlx::FromRow)]
pub struct JoinedDownloadSettingsRow {
    // Download settings fields
    pub download_id: i64,
    pub download_speed_limit: Option<i64>,
    
    // File settings fields (wrapped in Option because of left join)
    pub item_id: Option<i64>,
    pub file_speed_limit: Option<i64>,
}