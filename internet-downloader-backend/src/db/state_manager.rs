use std::collections::HashMap;

use indexmap::IndexMap;
use os_str_bytes::OsStrBytes;
use sqlx::{QueryBuilder, SqlitePool, sqlite::SqlitePoolOptions};
use thiserror::Error;

use crate::{db::rows::{DownloadItemRow, DownloadRow, GlobalSettingsRow, HostSettingsRow, JoinedDownloadSettingsRow}, download::{AppSettings, DownloadId, FileSize, items::{Download, DownloadItem, DownloadType}, status::{DownloadStatus, FileStatus, StateBucketCounters}}};

#[derive(Debug, Error)]
pub enum StateManagerError {
    #[error("Connection error: {0}")]
    ConnectionError(sqlx::Error),
    #[error("Error createing database tables: {0}")]
    TableCreationError(sqlx::Error),
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

            CREATE TABLE IF NOT EXISTS download_items (
                download_id INTEGER NOT NULL REFERENCES downloads(id) ON DELETE CASCADE,
                
                item_id INTEGER NOT NULL,        
                parent_id INTEGER,
                
                item_type TEXT NOT NULL, -- 'file' or 'folder'
                
                -- Shared fields
                name TEXT NOT NULL,
                relative_path_raw BLOB NOT NULL,
                relative_path TEXT NOT NULL,
                status TEXT NOT NULL,
                failure_reason TEXT,
                
                -- File-specific fields (These will be NULL for Folders)
                url TEXT,
                hash BLOB,
                chunks_raw BLOB,
                chunks_len INTEGER,
                size_type TEXT,
                size_bytes INTEGER,
                retries INTEGER DEFAULT 0,
                wait_time INTEGER,
                
                -- Ensure item_id is unique per download
                PRIMARY KEY (download_id, item_id),

                -- Ensures every parent id exists in the download_id we are referencing
                FOREIGN KEY (download_id, parent_id) REFERENCES download_items(download_id, item_id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_download_items_parent ON download_items(download_id, parent_id);

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
                item_id INTEGER NOT NULL,
                speed_limit INTEGER,
                
                PRIMARY KEY (download_id, item_id),
                FOREIGN KEY (download_id, item_id) REFERENCES download_items(download_id, item_id) ON DELETE CASCADE
            );
            "#
        )
        .execute(&self.pool)
        .await
        .map_err(StateManagerError::TableCreationError)?;

        let default_settings = AppSettings::default();
        let default_blob = rkyv::to_bytes::<rkyv::rancor::Error>(&default_settings).unwrap().into_vec();

        sqlx::query(
            r#"
            INSERT OR IGNORE INTO app_settings (id, settings_blob) 
            VALUES (1, ?)
            "#
        )
        .bind(default_blob)
        .execute(&self.pool)
        .await
        .unwrap();

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
        let mut files_iter = download.files.iter().peekable();

        while files_iter.peek().is_some() {
            let mut builder = QueryBuilder::new(
                "INSERT INTO download_items (
                    download_id, item_id, parent_id, item_type, 
                    name, relative_path_raw, relative_path, 
                    status, failure_reason, wait_time,
                    url, hash, chunks_raw, chunks_len, size_type, size_bytes, retries
                ) "
            );

            builder.push_values(files_iter.by_ref().take(1000), |mut builder, (item_id, item_type)| {
                match item_type {
                    DownloadType::File(file) => {
                        let (status, reason, wait_time) = file.status().to_db_columns();
                        let path_bytes = file.relative_path().to_io_bytes_lossy();
                        
                        let hash = file.hash().map(|hash| hash.to_be_bytes().to_vec());

                        let (size_type, size_bytes) = match file.size() {
                            None => (None, None),
                            Some(FileSize::Unknown) => (Some("unknown"), None),
                            Some(FileSize::Known(size)) => (Some("known"), Some(size as i64)),
                        };

                        builder.push_bind(*download.id() as i64)
                            .push_bind(*item_id as i64)
                            .push_bind(file.parent_id().map(|id| id as i64))
                            .push_bind("file")
                            .push_bind(file.name())
                            .push_bind(path_bytes)
                            .push_bind(file.relative_path().to_string_lossy())
                            .push_bind(status)
                            .push_bind(reason)
                            .push_bind(wait_time)
                            .push_bind(file.url_ref()) 
                            .push_bind(hash)
                            .push_bind(file.chunks().as_raw_slice())   
                            .push_bind(file.chunks().len() as i64)   
                            .push_bind(size_type)
                            .push_bind(size_bytes)
                            .push_bind(file.retries() as i64);
                    }
                    DownloadType::Folder(folder) => {
                        let (status, reason) = folder.status().to_db_columns(); 
                        let path_bytes = folder.relative_path().to_io_bytes_lossy();

                        builder.push_bind(*download.id() as i64)
                        .push_bind(*item_id as i64)
                        .push_bind(folder.parent_id().map(|id| id as i64))
                        .push_bind("folder")
                        .push_bind(folder.name())
                        .push_bind(path_bytes)
                        .push_bind(folder.relative_path().to_string_lossy())
                        .push_bind(status)
                        .push_bind(reason)
                        .push_bind(None::<i64>)
                        .push_bind(None::<&str>) 
                        .push_bind(None::<Vec<u8>>)
                        .push_bind(None::<&[u8]>)
                        .push_bind(None::<i64>) 
                        .push_bind(None::<&str>)
                        .push_bind(None::<i64>) 
                        .push_bind(None::<i64>);
                    },
                }
            });

            builder.push(
            " ON CONFLICT(download_id, item_id) DO UPDATE SET 
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
                retries = excluded.retries"
            );

            let query = builder.build();
            query.execute(&mut *transaction).await.unwrap();
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

    pub async fn load_download(&self, id: DownloadId) -> Result<Download, ()> {
        let download_row = sqlx::query_as::<_, DownloadRow>("SELECT * FROM downloads WHERE id = ?")
            .bind(*id as i64)
            .fetch_optional(&self.pool)
            .await
            .unwrap()
            .ok_or(())?;

        let item_rows = sqlx::query_as::<_, DownloadItemRow>(
            "SELECT * FROM download_items WHERE download_id = ? ORDER BY item_id ASC"
        ).bind(*id as i64)
            .fetch_all(&self.pool)
            .await
            .unwrap();

        let files = reconstruct_file_tree(item_rows);

        let download = Download::from_db(download_row, files);

        Ok(download)
    }

    pub async fn load_downloads(&self) -> Result<IndexMap<DownloadId, Download>, ()> {
        let download_rows = sqlx::query_as::<_, DownloadRow>("SELECT * FROM downloads ORDER BY id ASC")
            .fetch_all(&self.pool)
            .await
            .unwrap();

        let item_rows = sqlx::query_as::<_, DownloadItemRow>(
            "SELECT * FROM download_items ORDER BY download_id ASC, item_id ASC"
        )
        .fetch_all(&self.pool)
        .await
        .unwrap();

        let mut items_by_download: HashMap<i64, Vec<DownloadItemRow>> = HashMap::new();

        for row in item_rows {
            items_by_download
                .entry(row.download_id)
                .or_default()
                .push(row);
        }

        let mut downloads = IndexMap::with_capacity(download_rows.len());

        for download_row in download_rows {
            let download_id_val = download_row.id;
            
            let current_item_rows = items_by_download.remove(&download_id_val).unwrap_or_default();
            let files = reconstruct_file_tree(current_item_rows);

            let download = Download::from_db(download_row, files);
            downloads.insert(DownloadId(download_id_val as usize), download);
        }

        Ok(downloads)
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

    pub async fn file_exists(&self, download_id: DownloadId, file_id: usize) -> bool {
        let result: Result<bool, sqlx::Error> = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM download_items WHERE download_id = ? AND item_id = ?)"
        )
        .bind(*download_id as i64)
        .bind(file_id as i64)
        .fetch_one(&self.pool)
        .await;

        result.unwrap_or(false)
    }

    pub async fn load_app_settings(&self) -> Result<AppSettings, sqlx::Error> {
        let global_row = sqlx::query_as::<_, GlobalSettingsRow>("SELECT global_speed_limit FROM app_settings WHERE id = 1")
            .fetch_optional(&self.pool)
            .await
            .unwrap()
            .unwrap();

        let host_rows = sqlx::query_as::<_, HostSettingsRow>("SELECT host, speed_limit FROM host_settings")
            .fetch_all(&self.pool)
            .await.unwrap();
        
        let joined_download_settings_rows = sqlx::query_as::<_, JoinedDownloadSettingsRow>(
            r#"
            SELECT 
                download.download_id, 
                download.speed_limit AS download_speed_limit,
                file.item_id, 
                file.speed_limit AS file_speed_limit
            FROM download_settings download
            LEFT JOIN file_settings file ON download.download_id = file.download_id
            "#
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(AppSettings::from_db(global_row, host_rows, joined_download_settings_rows))
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
                INSERT INTO file_settings (download_id, item_id, speed_limit)
            "#);

            let all_files_iterator = app_settings.download_settings.iter().flat_map(|(download_id, download_settings)| {
                download_settings.file_settings.iter().map(move |(file_id, file_settings)| {
                    (download_id, file_id, file_settings)
                })
            });

            files_builder.push_values(all_files_iterator, |mut builder, (download_id, file_id, file_settings)| {
                builder.push_bind(**download_id as i64)
                .push_bind(*file_id as i64)
                .push_bind(file_settings.speed_limit.map(|speed_limit| speed_limit as i64));
            });

            files_builder.push(r#"
                ON CONFLICT(download_id, item_id) DO UPDATE SET
                    speed_limit = excluded.speed_limit
            "#);

            let files_query = files_builder.build();
            files_query.execute(&mut *transaction).await.unwrap();
        }

        transaction.commit().await.unwrap();
    }
}

fn reconstruct_file_tree(item_rows: Vec<DownloadItemRow>) -> IndexMap<usize, DownloadType> {
    let mut children: HashMap<usize, Vec<usize>> = HashMap::new();
    let mut folder_buckets = std::collections::HashMap::new();

    for row in &item_rows {
        if let Some(parent_id) = row.parent_id {
            let parent_id = parent_id as usize;
            
            children
                .entry(parent_id)
                .or_default()
                .push(row.item_id as usize);

            let bucket = if row.item_type == "file" {
                FileStatus::from_db_columns(&row.status, row.failure_reason.as_deref(), row.wait_time).bucket()
            } else {
                DownloadStatus::from_db_columns(&row.status, row.failure_reason.as_deref()).bucket()
            };

            folder_buckets
                .entry(parent_id)
                .or_insert_with(StateBucketCounters::new)
                .increment(bucket);
        }
    }

    let mut files = IndexMap::with_capacity(item_rows.len());

    for row in item_rows {
        let item_id = row.item_id as usize;

        let children = children.remove(&item_id).unwrap_or_default();
        let counters = folder_buckets.remove(&item_id).unwrap_or_else(StateBucketCounters::new);

        files.insert(item_id, row.into_download_type(children, counters));
    }

    files
}