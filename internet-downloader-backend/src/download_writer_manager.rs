use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::oneshot;
use tokio::task::JoinError;
use tracing::info;

use crate::shared_file_map::SharedFileMap;

#[derive(Debug)]
pub enum FileInitializationError {
    IoError(io::Error),
    TaskDropped(JoinError),
}

pub struct FileChunk {
    pub file_map: Arc<SharedFileMap>,
    pub offset: u64,
    pub data: Bytes,
    pub ack: oneshot::Sender<std::io::Result<()>>, 
}

#[derive(Clone)]
pub struct DownloadWriterManager {
    writer: flume::Sender<FileChunk>,
}

impl DownloadWriterManager {
    pub fn new() -> Self {
        let (sender, receiver) = flume::bounded::<FileChunk>(32);

        for _ in 0..8 {
            let thread_receiver = receiver.clone();
            std::thread::spawn(move || {
                while let Ok(chunk) = thread_receiver.recv() {
                    let result = chunk.file_map.write_chunk(chunk.offset as u64, &chunk.data);
                    let _ = chunk.ack.send(result);
                }
            });
        }

        Self {
            writer: sender,
        }
    }

    pub async fn create_file(&self, path: PathBuf, size: u64) -> Result<Arc<SharedFileMap>, FileInitializationError> {
        let join_result = tokio::task::spawn_blocking(move || {
            let file = SharedFileMap::new(path, size)?;

            Ok(Arc::new(file))
        })
        .await;

        match join_result {
            // Both the task and IO tasks finished successfully
            Ok(Ok(shared_map)) => Ok(shared_map),
            
            // The task finished successfully, but the IO failed
            Ok(Err(io_err)) => Err(FileInitializationError::IoError(io_err)),
            
            // The Tokio task itself panicked or was cancelled
            Err(join_err) => Err(FileInitializationError::TaskDropped(join_err)),
        }
    }

    pub fn sender(&self) -> flume::Sender<FileChunk> {
        self.writer.clone()
    }
}

impl Drop for DownloadWriterManager {
    fn drop(&mut self) {
        info!("Writer dropped");
    }
}