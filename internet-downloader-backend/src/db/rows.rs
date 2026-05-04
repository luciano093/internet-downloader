use std::collections::HashMap;

use indexmap::IndexMap;

use crate::{download::{items::{Download, FileDownload, FileId, FolderDownload, FolderId}, status::StateBucketCounters}, download_task::HASH_CHUNK_SIZE};

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
    pub fn into_download(self, files: IndexMap<FileId, FileDownload>, folders: IndexMap<FolderId, FolderDownload>) -> Download {
        Download::from_db(self, files, folders)
    }
}

#[derive(sqlx::FromRow)]
pub struct DownloadFolderRow {
    pub download_id: i64,
    pub folder_id: i64,
    pub parent_folder_id: Option<i64>,
    pub name: String,
    pub relative_path_raw: Vec<u8>,
    pub relative_path: String,
    pub status: String,
    pub failure_reason: Option<String>,
}

impl DownloadFolderRow {
    pub fn into_download_type(self, child_files: Vec<FileId>, child_folders: Vec<FolderId>, buckets: StateBucketCounters) -> FolderDownload {
        FolderDownload::from_db(self, child_files, child_folders, buckets)
    }
}

#[derive(sqlx::FromRow)]
pub struct DownloadFileRow {
    pub download_id: i64,
    pub file_id: i64,
    pub parent_folder_id: Option<i64>,
    pub name: String,
    pub relative_path_raw: Vec<u8>,
    pub relative_path: String,
    pub status: String,
    pub failure_reason: Option<String>,
    
    // File specific
    pub url: String,
    pub hash: Option<Vec<u8>>,
    pub chunks_raw: Option<Vec<u8>>,
    pub chunks_len: Option<i64>,
    pub size_type: Option<String>,
    pub size_bytes: Option<i64>,
    pub retries: i64,
    pub wait_time: Option<i64>,
}

impl DownloadFileRow {
    pub fn into_download_type(self, chunk_hashes: &mut HashMap<FileId, Vec<Option<[u8; 16]>>>) -> FileDownload {
        let file_id = FileId(self.file_id as usize);
        
        let chunk_hashes = chunk_hashes.remove(&file_id).unwrap_or_else(|| {
            // We have to reconstruct the vector of chunks with None values
            // Otherwise we would have no space for chunk hashes

            if let Some(size) = self.size_bytes.and_then(|v| u64::try_from(v).ok()) {
                let size = size as u64;

                let num_chunk_hashes = size.div_ceil(HASH_CHUNK_SIZE as u64);

                vec![None; num_chunk_hashes as usize]
            } else {
                Vec::new()
            }
        });

        FileDownload::from_db(self, chunk_hashes)
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
    pub file_id: i64,
    pub speed_limit: Option<i64>,
}

#[derive(sqlx::FromRow)]
pub struct JoinedDownloadSettingsRow {
    // Download settings fields
    pub download_id: i64,
    pub download_speed_limit: Option<i64>,
    
    // File settings fields (wrapped in Option because of left join)
    pub file_id: Option<i64>,
    pub file_speed_limit: Option<i64>,
}

#[derive(sqlx::FromRow)]
pub struct ChunkHashRow {
    pub download_id: i64,
    pub file_id: i64,
    pub chunk_index: i64,
    pub hash: Option<Vec<u8>>,
}
