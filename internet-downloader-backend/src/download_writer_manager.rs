use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::oneshot;
use tracing::info;

use crate::shared_file_map::SharedFileMap;

pub struct FileChunk {
    pub file_map: Arc<SharedFileMap>,
    pub offset: usize,
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

    pub async fn create_file(&self, path: PathBuf, size: u64) -> Arc<SharedFileMap> {
        tokio::task::spawn_blocking(move || {
            Arc::new(SharedFileMap::new(path, size))
        })
        .await
        .unwrap()
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