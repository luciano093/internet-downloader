use indexmap::IndexMap;
use sqlx::{sqlite::SqlitePoolOptions, SqlitePool};
use thiserror::Error;

use crate::download::{Download, DownloadId};

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

        Ok(Self {
            pool
        })
    }

    pub async fn create_tables(&self) -> Result<(), StateManagerError> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS download_states (
                id INTEGER PRIMARY KEY,
                url TEXT NOT NULL,
                state_blob BLOB NOT NULL,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            "#
        )
        .execute(&self.pool)
        .await
        .map_err(StateManagerError::TableCreationError)?;

        Ok(())
    }

    pub async fn write_download(&self, download: &Download) {
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(download).unwrap();

        sqlx::query("INSERT OR REPLACE INTO download_states (id, url, state_blob) VALUES (?, ?, ?)")
            .bind(*download.id() as i64) // SQLite uses i64
            .bind(download.url())
            .bind(bytes.as_slice())
            .execute(&self.pool)
            .await
            .unwrap();
    }

    pub async fn delete_download(&self, id: DownloadId) {  
        sqlx::query("DELETE FROM download_states WHERE id = ?")
            .bind(*id as i64)
            .execute(&self.pool)
            .await
            .unwrap();
    }

    pub async fn load_downloads(&self) -> Result<IndexMap<DownloadId, Download>, ()> {
        let rows: Vec<Vec<u8>> = sqlx::query_scalar("SELECT state_blob FROM download_states ORDER BY id ASC" )
            .fetch_all(&self.pool)
            .await
            .unwrap();

        let mut downloads: IndexMap<DownloadId, Download> = IndexMap::new();

        for blob in rows {
            let download = rkyv::from_bytes::<Download, rkyv::rancor::Error>(&blob).unwrap();

            downloads.insert(download.id(), download);
        }

        Ok(downloads)
    }

    pub async fn get_all_download_urls(&self) -> Vec<(usize, String)> {
        sqlx::query_as::<_, (i64, String)>("SELECT id, url FROM download_states")
            .fetch_all(&self.pool)
            .await
            .unwrap()
            .into_iter()
            .map(|(id, url)| (id as usize, url))
            .collect()
    }
}