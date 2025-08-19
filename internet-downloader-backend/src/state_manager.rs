use indexmap::IndexMap;
use sqlx::{sqlite::SqlitePoolOptions, SqlitePool};
use thiserror::Error;

use crate::download::Download;

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
                url TEXT PRIMARY KEY,
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
        let url = download.url().to_string();

        let mut state_blob = Vec::with_capacity(512);

        loop {
            state_blob.resize(state_blob.capacity(), 0);

            match bincode::encode_into_slice(&download, &mut state_blob, bincode::config::standard()) {
                Ok(encoded_len) => {
                    let state_blob = &state_blob[..encoded_len];

                    sqlx::query("REPLACE INTO download_states (url, state_blob) VALUES (?, ?)")
                        .bind(&url)
                        .bind(state_blob)
                        .execute(&self.pool)
                        .await
                        .unwrap();

                    println!("saved!");
                    break;
                }
                Err(bincode::error::EncodeError::UnexpectedEnd) => {
                    println!("extending {}", state_blob.capacity());
                    state_blob.reserve(state_blob.capacity());
                },
                Err(err) => {
                    panic!("{}", err);
                }
            }
        }
    }

    pub async fn load_downloads(&self) -> Result<IndexMap<String, Download>, ()> {
        let rows: Vec<Vec<u8>> = sqlx::query_scalar(
            "SELECT state_blob FROM download_states"
        )
        .fetch_all(&self.pool)
        .await
        .unwrap();

        let mut downloads: IndexMap<String, Download> = IndexMap::new();

        for blob in rows {
            let (download, _): (Download, _) = bincode::decode_from_slice(&blob, bincode::config::standard()).unwrap();

            downloads.insert(download.url().to_string(), download);
        }

        Ok(downloads)
    }
}