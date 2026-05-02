use std::{borrow::Cow, collections::HashMap, time::Duration};

use indexmap::IndexMap;
use os_str_bytes::OsStrBytes;
use sqlx::{QueryBuilder, Row, SqlitePool, Transaction, sqlite::SqlitePoolOptions};
use thiserror::Error;
use tokio::time::sleep;
use tracing::{debug, warn};

use crate::{db::rows::{ChunkHashRow, DownloadFileRow, DownloadFolderRow, DownloadRow, GlobalSettingsRow, HostSettingsRow, JoinedDownloadSettingsRow}, download::{AppSettings, FileSize, items::{Download, DownloadId, DownloadItem, FileDownload, FileId, FolderDownload, FolderId}, status::{DownloadStatus, FileStatus, StateBucketCounters}}};

#[derive(Debug, Error)]
pub enum StateManagerError {
    #[error("Connection error: {0}")]
    ConnectionError(sqlx::Error),
    #[error("Error creating database tables: {0}")]
    TableCreationError(sqlx::Error),
}

#[derive(Debug, Error)]
#[error("{message}: {kind}")]
pub struct DownloadFetchError {
    message: Cow<'static, str>,
    kind: DbFetchErrorKind,

    #[source]
    source: Box<dyn std::error::Error + Send + Sync + 'static>,
}

impl DownloadFetchError {
    pub fn new(message: impl Into<Cow<'static, str>>, err: sqlx::Error) -> Self {
        Self { 
            message: message.into(),
            kind: (&err).into(),
            source: Box::new(err),
        }
    }
    
    pub fn kind(&self) -> &DbFetchErrorKind {
        &self.kind
    }
    
    fn from_sqlx(err: sqlx::Error) -> Self {
        Self {
            message: "Failed to fetch download".into(),
            kind: DbFetchErrorKind::from(&err),
            source: Box::new(err),
        }
    }
}

impl From<ChunkHashLoadError> for DownloadFetchError {
    fn from(error: ChunkHashLoadError) -> Self {
        Self {
            message: "Failed to fetch chunk hashes for download".into(),
            kind: error.kind(),
            source: Box::new(error),
        }
    }
}

impl From<sqlx::Error> for DownloadFetchError {
    fn from(err: sqlx::Error) -> Self {
        Self::from_sqlx(err)
    }
}

#[derive(Debug, Error, Clone, Copy)]
pub enum DbFetchErrorKind {
    #[error("Failed to acquire a database connection")]
    ConnectionFailed,

    #[error("Failed to execute database query")]
    QueryFailed,

    #[error("Item was not found")]
    NotFound,

    #[error("Database schema corrupted")]
    SchemaCorrupted,

    #[error("Failed to decode database row")]
    DataCorrupted,

    #[error("Unexpected database error")]
    Unexpected,
}

impl From<&sqlx::Error> for DbFetchErrorKind {
    fn from(err: &sqlx::Error) -> Self {
        match &err {
            // Connection pool issues
            sqlx::Error::Io(_) 
            | sqlx::Error::Protocol(_) 
            | sqlx::Error::PoolTimedOut 
            | sqlx::Error::PoolClosed
            | sqlx::Error::WorkerCrashed 
            | sqlx::Error::BeginFailed => {
                DbFetchErrorKind::ConnectionFailed
            }
            // Actual SQL errors returned by the DB
            sqlx::Error::Database(_) => {
                DbFetchErrorKind::QueryFailed
            }

            // Schema issues (non recoverable)
            sqlx::Error::ColumnNotFound(_) | sqlx::Error::TypeNotFound { .. } => {
                DbFetchErrorKind::SchemaCorrupted
            }

            sqlx::Error::RowNotFound => {
                DbFetchErrorKind::NotFound
            }
            
            // Parsing and decoding errors
            | sqlx::Error::Decode(_) 
            | sqlx::Error::ColumnDecode { .. } 
            | sqlx::Error::ColumnIndexOutOfBounds { .. } => {
                DbFetchErrorKind::DataCorrupted
            }
            
            // Catch-all
            _ => DbFetchErrorKind::Unexpected
        }
    }
}

#[derive(Debug, Error)]
#[error("{message}: {kind}")]
pub struct ChunkHashLoadError {
    message: Cow<'static, str>,
    kind: DbFetchErrorKind,
    
    #[source]
    source: sqlx::Error,
}

impl ChunkHashLoadError {
    pub fn new(message: impl Into<Cow<'static, str>>, err: sqlx::Error) -> Self {
        Self { 
            message: message.into(),
            kind: (&err).into(),
            source: err,
        }
    }
    
    pub fn kind(&self) -> DbFetchErrorKind {
        self.kind
    }
    
    fn from_sqlx(err: sqlx::Error) -> Self {
        Self {
            message: "Failed to load chunk hashes".into(),
            kind: DbFetchErrorKind::from(&err),
            source: err,
        }
    }
}

impl From<sqlx::Error> for ChunkHashLoadError {
    fn from(err: sqlx::Error) -> Self {
        Self::from_sqlx(err)
    }
}

#[derive(Debug, Clone)]
pub struct StateManager {
    pool: SqlitePool,
}

impl StateManager {
    pub async fn new(url: &str) -> Result<Self, StateManagerError> {
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(url)
            .await
            .map_err(StateManagerError::ConnectionError)?;

        sqlx::query("PRAGMA journal_mode = WAL;").execute(&pool).await.map_err(StateManagerError::ConnectionError)?;
        sqlx::query("PRAGMA synchronous = NORMAL;").execute(&pool).await.map_err(StateManagerError::ConnectionError)?;
        sqlx::query("PRAGMA foreign_keys = ON;").execute(&pool).await.map_err(StateManagerError::ConnectionError)?;

        Ok(Self {
            pool
        })
    }

    pub async fn create_tables(&self) -> Result<(), StateManagerError> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS downloads (
                id INTEGER PRIMARY KEY,
                url TEXT NOT NULL,
                name TEXT NOT NULL,
                relative_path_raw BLOB NOT NULL, -- used to store actual relative path to support not utf-8
                relative_path TEXT NOT NULL,    -- a utf-8 version of the relative path for query purposes
                status TEXT NOT NULL,
                failure_reason TEXT
            );

            -- Folders table
            CREATE TABLE IF NOT EXISTS download_folders {
                download_id INTEGER NOT NULL REFERENCES downloads(id) ON DELETE CASCADE,
                folder_id INTEGER NOT NULL,        
                parent_folder_id INTEGER,

                name TEXT NOT NULL,
                relative_path_raw BLOB NOT NULL,
                relative_path TEXT NOT NULL,
                status TEXT NOT NULL,
                failure_reason TEXT,
                
                PRIMARY KEY (download_id, folder_id),

                FOREIGN KEY (download_id, parent_folder_id) 
                    REFERENCES download_folders(download_id, folder_id) ON DELETE CASCADE
            }

            -- Files table
            CREATE TABLE IF NOT EXISTS download_files (
                download_id INTEGER NOT NULL REFERENCES downloads(id) ON DELETE CASCADE,
                file_id INTEGER NOT NULL,        
                parent_folder_id INTEGER,
                
                name TEXT NOT NULL,
                relative_path_raw BLOB NOT NULL,
                relative_path TEXT NOT NULL,
                status TEXT NOT NULL,
                failure_reason TEXT,
                
                -- File-specific fields
                url TEXT,
                hash BLOB,
                chunks_raw BLOB,
                chunks_len INTEGER,
                size_type TEXT,
                size_bytes INTEGER,
                retries INTEGER DEFAULT 0,
                wait_time INTEGER,
                
                PRIMARY KEY (download_id, file_id),
    
                FOREIGN KEY (download_id, parent_folder_id) 
                    REFERENCES download_folders(download_id, folder_id) ON DELETE CASCADE,
    
                -- Constraints
                CONSTRAINT check_size_logic CHECK (
                    (size_type = 'known' AND size_bytes IS NOT NULL) OR 
                    (size_type = 'unknown' AND size_bytes IS NULL) OR
                    (size_type IS NULL AND size_bytes IS NULL)
                ), 
                CONSTRAINT check_hash_length CHECK (hash IS NULL OR length(hash) = 16)
            );

            CREATE INDEX IF NOT EXISTS idx_download_folders_parent ON download_folders(download_id, parent_folder_id);
            CREATE INDEX IF NOT EXISTS idx_download_files_parent ON download_files(download_id, parent_folder_id);

            -- Chunk hashes
            CREATE TABLE IF NOT EXISTS chunk_hashes (
                download_id INTEGER NOT NULL,
                file_id INTEGER NOT NULL,
                chunk_index   INTEGER NOT NULL,
                hash        BLOB(16),
                
                PRIMARY KEY (download_id, file_id, chunk_index),
                FOREIGN KEY (download_id, file_id)
                    REFERENCES download_files(download_id, file_id)
                    ON DELETE CASCADE,
                    
                CONSTRAINT check_hash_length CHECK (hash IS NULL OR length(hash) = 16)
            );

            -- AppSettings
            CREATE TABLE IF NOT EXISTS app_settings (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                global_speed_limit INTEGER
            );

            -- HostSettings
            CREATE TABLE IF NOT EXISTS host_settings (
                host TEXT PRIMARY KEY,
                speed_limit INTEGER
            );

            -- DownloadSettings
            CREATE TABLE IF NOT EXISTS download_settings (
                download_id INTEGER PRIMARY KEY REFERENCES downloads(id) ON DELETE CASCADE,
                speed_limit INTEGER
            );

            -- FileSettings
            CREATE TABLE IF NOT EXISTS file_settings (
                download_id INTEGER NOT NULL,
                file_id INTEGER NOT NULL,
                speed_limit INTEGER,
                
                PRIMARY KEY (download_id, file_id),
                FOREIGN KEY (download_id, file_id) REFERENCES download_files(download_id, file_id) ON DELETE CASCADE
            );
            "#
        )
        .execute(&self.pool)
        .await
        .map_err(StateManagerError::TableCreationError)?;

        let default_settings = AppSettings::default();

        sqlx::query(
            r#"
            INSERT OR IGNORE INTO app_settings (id, global_speed_limit) 
            VALUES (1, ?)
            "#
        )
        .bind(default_settings.global_speed_limit.map(|speed| speed as i64))
        .execute(&self.pool)
        .await
        .map_err(StateManagerError::TableCreationError)?;

        Ok(())
    }

    pub async fn write_download(&self, download: &Download) {
        let mut transaction = self.pool.begin().await.unwrap();

        // We don't crash if foreign keys are violated before the end of the transaction
        sqlx::query("PRAGMA defer_foreign_keys = ON")
            .execute(&mut *transaction)
            .await
            .unwrap();

        let (status, reason) = download.status().to_db_columns();
        let path_bytes = download.relative_path().to_io_bytes_lossy();

        sqlx::query(
            r#"
            INSERT INTO downloads (id, url, name, relative_path_raw, relative_path, status, failure_reason) 
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                url = excluded.url,
                name = excluded.name,
                relative_path_raw = excluded.relative_path_raw,
                relative_path = excluded.relative_path,
                status = excluded.status, 
                failure_reason = excluded.failure_reason
            "#
        )
        .bind(*download.id() as i64)
        .bind(download.url())
        .bind(download.name())
        .bind(path_bytes.as_ref())
        .bind(download.relative_path().to_string_lossy())
        .bind(status)
        .bind(reason)
        .execute(&mut *transaction)
        .await
        .unwrap();
        
        // We chunk querys to be 1000 files at a time due to a few reasons:
        // Doing one big query with more files will probably be slower
        // SQLITE_MAX_VARIABLE_NUMBER allows up to 32766 placeholders in a single query
        // and here every file query uses more than a dozen at a time, this might accumulate
        // and pass the max limit if we aren't careful
        let mut folders_iter = download.folders().iter().peekable();

        while folders_iter.peek().is_some() {
            let mut builder = QueryBuilder::new(
                "INSERT INTO download_folders (
                    download_id, folder_id, parent_folder_id,
                    name, relative_path_raw, relative_path, 
                    status, failure_reason
                ) "
            );

            let batch: Vec<_> = folders_iter.by_ref().take(1000).collect();

            builder.push_values(batch, |mut builder, (&folder_id, folder)| {
                let (status, reason) = folder.status().to_db_columns(); 
                let path_bytes = folder.relative_path().to_io_bytes_lossy();

                builder
                    .push_bind(*download.id() as i64)
                    .push_bind(*folder_id as i64)
                    .push_bind(folder.parent_id().map(|id| *id as i64))
                    .push_bind(folder.name())
                    .push_bind(path_bytes)
                    .push_bind(folder.relative_path().to_string_lossy())
                    .push_bind(status)
                    .push_bind(reason);
            });

            builder.push(
            r#" ON CONFLICT(download_id, folder_id) DO UPDATE SET 
                name = excluded.name, 
                relative_path_raw = excluded.relative_path_raw,
                relative_path = excluded.relative_path, 
                status = excluded.status, 
                failure_reason = excluded.failure_reason"#
            );

            let query = builder.build();
            query.execute(&mut *transaction).await.unwrap();
        }

        
        let mut files_iter = download.files().iter().peekable();

        while files_iter.peek().is_some() {
            let mut builder = QueryBuilder::new(
                "INSERT INTO download_files (
                    download_id, file_id, parent_folder_id,
                    name, relative_path_raw, relative_path, 
                    status, failure_reason, 
                    url, hash, chunks_raw, chunks_len, size_type, size_bytes, retries, wait_time
                ) "
            );

            let batch: Vec<_> = files_iter.by_ref().take(1000).collect();

            builder.push_values(&batch, |mut builder, (file_id, file)| {
                let (status, reason, wait_time) = file.status().to_db_columns();
                let path_bytes = file.relative_path().to_io_bytes_lossy();
                
                let hash = file.hash().map(|hash| hash.to_be_bytes().to_vec());

                let (size_type, size_bytes) = match file.size() {
                    None => (None, None),
                    Some(FileSize::Unknown) => (Some("unknown"), None),
                    Some(FileSize::Known(size)) => (Some("known"), Some(size as i64)),
                };

                builder
                    .push_bind(*download.id() as i64)
                    .push_bind(***file_id as i64)
                    .push_bind(file.parent_id().map(|id| *id as i64))
                    .push_bind(file.name())
                    .push_bind(path_bytes)
                    .push_bind(file.relative_path().to_string_lossy())
                    .push_bind(status)
                    .push_bind(reason)
                    .push_bind(file.url_ref()) 
                    .push_bind(hash)
                    .push_bind(file.blocks().as_raw_slice())   
                    .push_bind(file.blocks().len() as i64)   
                    .push_bind(size_type)
                    .push_bind(size_bytes)
                    .push_bind(file.retries() as i64)
                    .push_bind(wait_time);
            });
            
            builder.push(
            r#" ON CONFLICT(download_id, file_id) DO UPDATE SET 
                name = excluded.name, 
                relative_path_raw = excluded.relative_path_raw,
                relative_path = excluded.relative_path, 
                status = excluded.status, 
                failure_reason = excluded.failure_reason,
                url = excluded.url,
                hash = excluded.hash,
                wait_time = excluded.wait_time,
                chunks_raw = excluded.chunks_raw,
                chunks_len = excluded.chunks_len,
                size_type = excluded.size_type,
                size_bytes = excluded.size_bytes,
                retries = excluded.retries"#
            );

            let query = builder.build();
            query.execute(&mut *transaction).await.unwrap();

            for (_, file) in batch {
                write_chunk_hashes(&mut transaction, download.id(), file)
                    .await
                    .unwrap();
            }
        }

        transaction.commit().await.unwrap();
    }

    pub async fn delete_download(&self, id: DownloadId) {  
        sqlx::query("DELETE FROM downloads WHERE id = ?")
            .bind(*id as i64)
            .execute(&self.pool)
            .await
            .unwrap();
    }

    pub async fn load_download(&self, id: DownloadId) -> Result<Option<Download>, DownloadFetchError> {
        let download_row = sqlx::query_as::<_, DownloadRow>("SELECT * FROM downloads WHERE id = ?")
            .bind(*id as i64)
            .fetch_optional(&self.pool)
            .await
            .map_err(|err| DownloadFetchError::new("Failed to fetch download row", err))?;

        let download_row = match download_row {
            Some(download_row) => download_row,
            None => return Ok(None),
        };

        let files_rows = sqlx::query_as::<_, DownloadFileRow>(
            "SELECT * FROM download_files WHERE download_id = ? ORDER BY file_id ASC"
        ).bind(*id as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|err| DownloadFetchError::new("Failed to fetch files rows", err))?;
        
        let folders_rows = sqlx::query_as::<_, DownloadFolderRow>(
            "SELECT * FROM download_folders WHERE download_id = ? ORDER BY folder_id ASC"
        ).bind(*id as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|err| DownloadFetchError::new("Failed to fetch folders rows", err))?;

        
        let chunk_hashes_map = self.load_download_chunk_hashes_with_recovery(id).await?;
        let (files, folders) = reconstruct_file_tree(files_rows, folders_rows, chunk_hashes_map);

        let download = Download::from_db(download_row, files, folders);

        Ok(Some(download))
    }

    pub async fn load_downloads(&self) -> Result<IndexMap<DownloadId, Download>, DownloadFetchError> {
        let download_rows = sqlx::query_as::<_, DownloadRow>("SELECT * FROM downloads ORDER BY id ASC")
            .fetch_all(&self.pool)
            .await
            .map_err(|err| DownloadFetchError::new("Failed to fetch all downloads from db", err))?;

        let file_rows = sqlx::query_as::<_, DownloadFileRow>(
            "SELECT * FROM download_files ORDER BY download_id ASC, file_id ASC"
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|err| DownloadFetchError::new("Failed to fetch all download files from db", err))?;

        let mut files_by_download: HashMap<i64, Vec<DownloadFileRow>> = HashMap::new();

        for row in file_rows {
            files_by_download
                .entry(row.download_id)
                .or_default()
                .push(row);
        }

        let folder_rows = sqlx::query_as::<_, DownloadFolderRow>(
            "SELECT * FROM download_folders ORDER BY download_id ASC, folder_id ASC"
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|err| DownloadFetchError::new("Failed to fetch all download folders from db", err))?;

        let mut folders_by_download: HashMap<i64, Vec<DownloadFolderRow>> = HashMap::new();

        for row in folder_rows {
            folders_by_download
                .entry(row.download_id)
                .or_default()
                .push(row);
        }

        let mut downloads = IndexMap::with_capacity(download_rows.len());

        let mut chunk_hashes = match self.load_chunk_hashes().await {
            Ok(chunk_hashes) => chunk_hashes,
            Err(err) => match err.kind() {
                // We failed to load all the chunks at once due to a corruption somewhere, 
                // we should isolate where the corruption came from first.
                // We treat NoFound as corrupted as there shouldn't be an error in the first place
                // if the chunk hashes were not found.
                DbFetchErrorKind::DataCorrupted | DbFetchErrorKind::NotFound => {
                    warn!(error = &err as &dyn std::error::Error, "Chunk hashes are corrupted, recovery process started.");
                    let mut recovered_chunk_hashes = HashMap::new();
                    
                    for download_row in &download_rows {
                        let download_id = DownloadId(download_row.id as usize);

                        // We request hashes download by download instead of all at once
                        let download_chunk_hashes = self.load_download_chunk_hashes_with_recovery(download_id).await?;

                        recovered_chunk_hashes.insert(download_id, download_chunk_hashes);
                    }
                    
                    recovered_chunk_hashes
                }
                DbFetchErrorKind::ConnectionFailed |
                DbFetchErrorKind::QueryFailed |
                DbFetchErrorKind::SchemaCorrupted |
                DbFetchErrorKind::Unexpected => return Err(err.into()),
            }
        };

        for download_row in download_rows {
            let download_id_val = download_row.id;
            let download_id = DownloadId(download_id_val as usize);
            
            let current_file_rows = files_by_download.remove(&download_id_val).unwrap_or_default();
            let current_folder_rows = folders_by_download.remove(&download_id_val).unwrap_or_default();
            
            let chunk_hashes_map = chunk_hashes.remove(&download_id).unwrap_or_default();
            
            let (files, folders) = reconstruct_file_tree(current_file_rows, current_folder_rows, chunk_hashes_map);

            let download = Download::from_db(download_row, files, folders);
            downloads.insert(download_id, download);
        }

        Ok(downloads)
    }

    async fn load_download_chunk_hashes(&self, download_id: DownloadId) -> Result<HashMap<FileId, Vec<Option<[u8; 16]>>>, ChunkHashLoadError> {
        let rows = sqlx::query_as::<_, ChunkHashRow>(
            "SELECT * FROM chunk_hashes WHERE download_id = ? ORDER BY file_id, chunk_index"
        )
        .bind(*download_id as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| ChunkHashLoadError::new("Failed to query chunk hash rows", err))?;

        let mut map = HashMap::new();

        for row in rows { 
            let hashes: &mut Vec<Option<[u8; 16]>> = map.entry(FileId(row.file_id as usize)).or_default();
            let index = row.chunk_index as usize;

            if row.chunk_index < 0 {
                warn!("Corrupted chunk index {} for file {}. Deleting from DB.", row.chunk_index, row.file_id);
                
                let _ = sqlx::query(
                        "DELETE FROM chunk_hashes WHERE download_id = ? AND file_id = ? AND chunk_index = ?"
                    )
                    .bind(*download_id as i64) 
                    .bind(row.file_id)           
                    .bind(row.chunk_index)       
                    .execute(&self.pool)      
                    .await;
                
                continue;
            }

            if hashes.len() <= index {
                hashes.resize(index + 1, None);
            }

            if let Some(hash_vec) = row.hash {
                if let Ok(arr) = hash_vec.try_into() {
                    hashes[index] = Some(arr);
                } else {
                    warn!("Malformed chunk hash found at index {} for download. Treating as missing.", index);
                }
            }
        }

        Ok(map)
    }

    async fn load_chunk_hashes(&self) -> Result<HashMap<DownloadId, HashMap<FileId, Vec<Option<[u8; 16]>>>>, ChunkHashLoadError> {
        let rows = sqlx::query_as::<_, ChunkHashRow>(
            "SELECT * FROM chunk_hashes ORDER BY download_id ASC, file_id, chunk_index"
        )
        .fetch_all(&self.pool)
        .await
        .map_err(ChunkHashLoadError::from_sqlx)?;

        let mut map = HashMap::new();

        for row in rows {
            let chunk_hash_map: &mut HashMap<FileId, Vec<Option<[u8; 16]>>> = map.entry(DownloadId(row.download_id as usize)).or_default();

            let hashes: &mut Vec<Option<[u8; 16]>> = chunk_hash_map.entry(FileId(row.file_id as usize)).or_default();

            if row.chunk_index < 0 {
                warn!("Negative chunk_index found in bulk load: {}", row.chunk_index);
                return Err(ChunkHashLoadError::new(format!("Corrupted negative chunk_index found in database at index {}", row.chunk_index), sqlx::Error::RowNotFound));
            }
            
            let index = row.chunk_index as usize;

            if hashes.len() <= index {
                hashes.resize(index + 1, None);
            }

            if let Some(hash_vec) = row.hash {
                if let Ok(arr) = hash_vec.try_into() {
                    hashes[index] = Some(arr);
                } else {
                    warn!("Malformed chunk hash found at index {} for download. Treating as missing.", index);
                }
            }
        }

        Ok(map)
    }

    pub async fn get_all_download_urls(&self) -> Vec<(usize, String)> {
        sqlx::query_as::<_, (i64, String)>("SELECT id, url FROM downloads")
            .fetch_all(&self.pool)
            .await
            .unwrap()
            .into_iter()
            .map(|(id, url)| (id as usize, url))
            .collect()
    }

    pub async fn file_exists(&self, download_id: DownloadId, file_id: FileId) -> bool {
        let result: Result<bool, sqlx::Error> = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM download_files WHERE download_id = ? AND file_id = ?)"
        )
        .bind(*download_id as i64)
        .bind(*file_id as i64)
        .fetch_one(&self.pool)
        .await;

        result.unwrap_or(false)
    }

    pub async fn load_app_settings(&self) -> Result<Option<AppSettings>, sqlx::Error> {
        let Some(global_row) = sqlx::query_as::<_, GlobalSettingsRow>("SELECT global_speed_limit FROM app_settings WHERE id = 1")
            .fetch_optional(&self.pool)
            .await? 
        else {
            return Ok(None);
        };

        let host_rows = sqlx::query_as::<_, HostSettingsRow>("SELECT host, speed_limit FROM host_settings")
            .fetch_all(&self.pool)
            .await?;
        
        let joined_download_settings_rows = sqlx::query_as::<_, JoinedDownloadSettingsRow>(
            r#"
            SELECT 
                download.download_id, 
                download.speed_limit AS download_speed_limit,
                file.file_id, 
                file.speed_limit AS file_speed_limit
            FROM download_settings download
            LEFT JOIN file_settings file ON download.download_id = file.download_id
            "#
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(Some(AppSettings::from_db(global_row, host_rows, joined_download_settings_rows)))
    }

    pub async fn write_app_settings(&self, app_settings: &AppSettings) {
        let mut transaction = self.pool.begin().await.unwrap();

        sqlx::query("PRAGMA defer_foreign_keys = ON")
            .execute(&mut *transaction)
            .await
            .unwrap();

        sqlx::query(r#"
            INSERT INTO app_settings (id, global_speed_limit)
            VALUES (1, ?)
            ON CONFLICT(id) DO UPDATE SET
                global_speed_limit = excluded.global_speed_limit
        "#)
        .bind(app_settings.global_speed_limit.map(|speed_limit| speed_limit as i64))
        .execute(&mut *transaction)
        .await
        .unwrap();

        if !app_settings.host_settings.is_empty() {
            let mut host_builder = QueryBuilder::new(r#"
                INSERT INTO host_settings (host, speed_limit)
            "#);

            host_builder.push_values(&app_settings.host_settings, |mut builder, (host, host_settings)| {
                builder.push_bind(host);
                builder.push_bind(host_settings.speed_limit.map(|speed_limit| speed_limit as i64));
            });

            host_builder.push(r#"
                ON CONFLICT(host) DO UPDATE SET
                    speed_limit = excluded.speed_limit
            "#);

            let host_query = host_builder.build();
            host_query.execute(&mut *transaction).await.unwrap();
        }

        if !app_settings.download_settings.is_empty() {
            let mut downloads_builder = QueryBuilder::new(r#"
                INSERT INTO download_settings (download_id, speed_limit)
            "#);

            downloads_builder.push_values(&app_settings.download_settings, |mut builder, (download_id, download_settings)| {
                builder
                    .push_bind(**download_id as i64)
                    .push_bind(download_settings.speed_limit.map(|speed_limit| speed_limit as i64));
            });

            downloads_builder.push(r#"
                ON CONFLICT(download_id) DO UPDATE SET
                    speed_limit = excluded.speed_limit
            "#);

            let downloads_query = downloads_builder.build();
            downloads_query.execute(&mut *transaction).await.unwrap();
        }

        let has_any_files = app_settings.download_settings.values().any(|d| !d.file_settings.is_empty());

        if has_any_files {
            let mut files_builder = QueryBuilder::new(r#"
                INSERT INTO file_settings (download_id, file_id, speed_limit)
            "#);

            let all_files_iterator = app_settings.download_settings.iter().flat_map(|(download_id, download_settings)| {
                download_settings.file_settings.iter().map(move |(file_id, file_settings)| {
                    (download_id, file_id, file_settings)
                })
            });

            files_builder.push_values(all_files_iterator, |mut builder, (download_id, file_id, file_settings)| {
                builder.push_bind(**download_id as i64)
                .push_bind(**file_id as i64)
                .push_bind(file_settings.speed_limit.map(|speed_limit| speed_limit as i64));
            });

            files_builder.push(r#"
                ON CONFLICT(download_id, file_id) DO UPDATE SET
                    speed_limit = excluded.speed_limit
            "#);

            let files_query = files_builder.build();
            files_query.execute(&mut *transaction).await.unwrap();
        }

        transaction.commit().await.unwrap();
    }

    async fn load_download_chunk_hashes_with_recovery(&self, download_id: DownloadId) -> Result<HashMap<FileId, Vec<Option<[u8; 16]>>>, ChunkHashLoadError> {
        match self.load_download_chunk_hashes(download_id).await {
            Ok(download_chunk_hashes) => Ok(download_chunk_hashes),
            Err(err) => match err.kind() {
    
                // When one fails again with one of these, it means we found which download was failing
                // and can start looking more deeply into the specific failing chunk hash(es)
                DbFetchErrorKind::DataCorrupted | DbFetchErrorKind::NotFound => {
                    warn!("Isolating corrupted chunk hash for download_id {}", *download_id);
    
                    let mut recovered_chunk_hashes: HashMap<FileId, Vec<Option<[u8; 16]>>> = HashMap::new();
    
                    let keys_query = sqlx::query(
                            "SELECT file_id, chunk_index FROM chunk_hashes WHERE download_id = ?"
                        )
                        .bind(*download_id as i64)
                        .fetch_all(&self.pool)
                        .await;
    
                    match keys_query {
                            Ok(keys) => {
                                let mut transaction = self.pool.begin().await.map_err(|err| {
                                    ChunkHashLoadError::new("Failed to start recovery transaction", err)
                                })?;
                                
                                for key_row in keys {
                                let file_id: i64 = key_row.get("file_id");
                                let chunk_index: i64 = key_row.get("chunk_index");
    
                                if chunk_index < 0 {
                                    warn!(
                                        "Corrupted negative keys found (file_id: {}, chunk_index: {}). Deleting it from DB.", 
                                        file_id, chunk_index
                                    );
                            
                                    let _ = sqlx::query(
                                        "DELETE FROM chunk_hashes WHERE download_id = ? AND file_id = ? AND chunk_index = ?"
                                    )
                                    .bind(*download_id as i64)
                                    .bind(file_id)
                                    .bind(chunk_index)
                                    .execute(&mut *transaction)
                                    .await;
                                    
                                    continue;
                                }
    
                                let chunk_row_result = sqlx::query_as::<_, ChunkHashRow>(
                                        "SELECT * FROM chunk_hashes WHERE download_id = ? AND file_id = ? AND chunk_index = ?"
                                    )
                                    .bind(*download_id as i64)
                                    .bind(file_id)
                                    .bind(chunk_index)
                                    .fetch_one(&mut *transaction)
                                    .await;
    
                                match chunk_row_result {
                                    Ok(row) => {
                                        // Row is healthy! We insert it into our map
                                        let hashes = recovered_chunk_hashes.entry(FileId(file_id as usize)).or_default();
                                        let index = chunk_index as usize;
                
                                        if hashes.len() <= index {
                                            hashes.resize(index + 1, None);
                                        }
                
                                        if let Some(hash_vec) = row.hash {
                                            if let Ok(arr) = hash_vec.try_into() {
                                                hashes[index] = Some(arr);
                                            }
                                        }
                                    }
                                    Err(err) => {
                                        // We found the corrupted row, delete it
                                        warn!(
                                            "Corrupted chunk hash found (file_id: {}, chunk_index: {}). Deleting it from DB. Err: {}", 
                                            file_id, chunk_index, err
                                        );
    
                                        let _ = sqlx::query(
                                            "DELETE FROM chunk_hashes WHERE download_id = ? AND file_id = ? AND chunk_index = ?"
                                        )
                                        .bind(*download_id as i64)
                                        .bind(file_id)
                                        .bind(chunk_index)
                                        .execute(&mut *transaction)
                                        .await;
                                    }
                                }
                            }
    
                            transaction.commit().await.map_err(|err| {
                                ChunkHashLoadError::new("Failed to commit recovery transaction", err)
                            })?;
                            
                            Ok(recovered_chunk_hashes)
                        }
                        Err(err) => {
                            // If we can't even run `SELECT file_id, chunk_index`, the SQLite table is broken.
                            warn!("Wasn't able to get any rows for download {}. Schema is corrupted.", *download_id);
                            
                            return Err(ChunkHashLoadError::new("Schema corrupted: unable to query chunk hash keys", err).into());
                        }
                    }
                }
                DbFetchErrorKind::ConnectionFailed |
                DbFetchErrorKind::QueryFailed |
                DbFetchErrorKind::SchemaCorrupted |
                DbFetchErrorKind::Unexpected => return Err(err.into()),
            }
        }
    }
}

async fn write_chunk_hashes(transaction: &mut Transaction<'_, sqlx::Sqlite>, download_id: DownloadId, file: &FileDownload) -> Result<(), sqlx::Error> {
    let hashes = file.chunk_hashes();

    if hashes.is_empty() {
        return Ok(());
    }

    let mut range = (0..hashes.len())
        .filter_map(|chunk_index| {
            let hash = hashes.get(chunk_index)?.as_ref()?;
            Some((chunk_index, hash))
        })
        .peekable();

    while range.peek().is_some() {
        let mut builder = QueryBuilder::new(
            "INSERT INTO chunk_hashes (download_id, file_id, chunk_index, hash)"
        );

        builder.push_values(range.by_ref().take(1000), |mut builder, (chunk_index, hash)| {
            builder.push_bind(*download_id as i64)
                .push_bind(*file.id() as i64)
                .push_bind(chunk_index as i64)
                .push_bind(&hash[..]);
        });

        builder.push(r#"
            ON CONFLICT(download_id, file_id, chunk_index) DO UPDATE SET 
                hash = excluded.hash
                    WHERE chunk_hashes.hash IS DISTINCT FROM excluded.hash
        "#);

        builder.build().execute(&mut **transaction).await?;
    }

    Ok(())
}

fn reconstruct_file_tree(file_rows: Vec<DownloadFileRow>, folder_rows: Vec<DownloadFolderRow>, mut chunk_hashes: HashMap<FileId, Vec<Option<[u8; 16]>>>) -> (IndexMap<FileId, FileDownload>, IndexMap<FolderId, FolderDownload>) {

    // We keep track of what children belong to which parents to create the parents later
    let mut child_files: HashMap<FolderId, Vec<FileId>> = HashMap::new();
    let mut child_folders: HashMap<FolderId, Vec<FolderId>> = HashMap::new();

    // We keep track of the state buckets that will be needed to reconstruct parents
    let mut folder_buckets = std::collections::HashMap::new();

    for file_row in &file_rows {
        if let Some(parent_id) = file_row.parent_folder_id {
            let parent_id = FolderId(parent_id as usize);

            child_files
                .entry(parent_id)
                .or_default()
                .push(FileId(file_row.file_id as usize));

            let bucket = FileStatus::from_db_columns(&file_row.status, file_row.failure_reason.as_deref(), file_row.wait_time).unwrap_or_default().bucket();

            folder_buckets
                .entry(parent_id)
                .or_insert_with(StateBucketCounters::new)
                .increment(bucket);
        }
    }

    for folder_row in &folder_rows {
        if let Some(parent_id) = folder_row.parent_folder_id {
            let parent_id = FolderId(parent_id as usize);

            child_folders
                .entry(parent_id)
                .or_default()
                .push(FolderId(folder_row.folder_id as usize));

            let bucket = DownloadStatus::from_db_columns(&folder_row.status, folder_row.failure_reason.as_deref()).unwrap_or_default().bucket();

            folder_buckets
                .entry(parent_id)
                .or_insert_with(StateBucketCounters::new)
                .increment(bucket);
        }
    }

    let mut files = IndexMap::with_capacity(file_rows.len());
    let mut folders = IndexMap::with_capacity(folder_rows.len());

    for file_row in file_rows {
        let file_id = FileId(file_row.file_id as usize);

        files.insert(file_id, file_row.into_download_type(&mut chunk_hashes));
    }
    
    for folder_row in folder_rows {
        let folder_id = FolderId(folder_row.folder_id as usize);

        let child_files = child_files.remove(&folder_id).unwrap_or_default();
        let child_folders = child_folders.remove(&folder_id).unwrap_or_default();
        
        let counters = folder_buckets.remove(&folder_id).unwrap_or_default();

        folders.insert(folder_id, folder_row.into_download_type(child_files, child_folders, counters));
    }

    (files, folders)
}

async fn with_retry<F, Fut, T, E>(mut retries: usize, delay: Duration, mut action: F, mut is_retriable: impl FnMut(&E) -> bool) -> Result<T, E>
    where F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>
{
    loop {
        match action().await {
            Ok(val) => return Ok(val),
            Err(err) => {
                // If we have retries left and the caller says this specific error is retriable
                if retries > 0 && is_retriable(&err) {
                    debug!(
                        "Operation failed, retrying in {:?}... ({} attempts left)", 
                        delay, retries
                    );
                    retries -= 1;
                    sleep(delay).await;
                    continue;
                }
                // Otherwise, we are out of retries or the error is fatal
                return Err(err);
            }
        }
    }
}
