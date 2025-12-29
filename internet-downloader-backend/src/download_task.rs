use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use reqwest::{Client, StatusCode, header};
use thiserror::Error;
use tokio::fs::create_dir_all;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::oneshot;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use crate::client_state_manager::UiStateEvent;
use crate::download::{Download, DownloadFailureReason, DownloadItem, DownloadStatus, DownloadType, DownloadUpdate, FileSize, FileUpdate};
use crate::host_manager::{ActiveDownloadPermit, HostMessage, ValidDownloadPermit};
use crate::shared_file_map::SharedFileMap;
use crate::state_manager::StateManager;

#[derive(Debug, Error)]
pub enum DownloadError {
    #[error("File system error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Network error: {0}")]
    Network(#[from] reqwest::Error),
    #[error("Rate limited (429): {}", match .0 { 
        Some(retry) => format!("try again in {retry}s"), 
        None => "try again later".to_string() 
    })]
    RateLimited(Option<u64>),
    #[error("Server error ({0})")]
    ServerError(StatusCode),
    #[error("Client error ({0})")]
    ClientError(StatusCode),

}

#[derive(Debug, Error)]
pub enum RangeDownloadError {
    #[error(transparent)]
    Download(#[from] DownloadError),
    #[error("Received unexpected status: ({0})")]
    UnexpectedStatus(StatusCode),
    #[error("Received download piece with unexpected length: ({0}). Expected ({1})")]
    UnexpectedLength(u64, u64),
    #[error("Download does not support range downloads")]
    RangeNotSupported,
}

trait FileDownloadRetry {
    fn file_id(&self) -> usize;
    fn url(&self) -> Arc<String>;
}

pub enum RetryKind {
    Metadata(MetadataRetry),
    StreamDownload(StreamRetry),
    RangeDownload(RangeDownload),
}

impl FileDownloadRetry for RetryKind {
    fn file_id(&self) -> usize {
        match self {
            RetryKind::Metadata(metadata_retry) => metadata_retry.file_id(),
            RetryKind::StreamDownload(stream_retry) => stream_retry.file_id(),
            RetryKind::RangeDownload(range_download) => range_download.file_id(),
        }
    }

    fn url(&self) -> Arc<String> {
        match self {
            RetryKind::Metadata(metadata_retry) => metadata_retry.url(),
            RetryKind::StreamDownload(stream_retry) => stream_retry.url(),
            RetryKind::RangeDownload(range_download) => range_download.url(),
        }
    }
}

struct MetadataRetry {
    file_id: usize,
    url: Arc<String>,
}

impl FileDownloadRetry for MetadataRetry {
    fn file_id(&self) -> usize {
        self.file_id
    }

    fn url(&self) -> Arc<String> {
        self.url.clone()
    }
}

struct StreamRetry {
    file_id: usize,
    url: Arc<String>,
    path: PathBuf,
}

impl FileDownloadRetry for StreamRetry {
    fn file_id(&self) -> usize {
        self.file_id
    }

    fn url(&self) -> Arc<String> {
        self.url.clone()
    }
}

struct RangeDownload {
    file_id: usize,
    range: (usize, usize),
    url: Arc<String>,
    file_map: Arc<SharedFileMap>,
    expected_len: u64,
}

impl FileDownloadRetry for RangeDownload {
    fn file_id(&self) -> usize {
        self.file_id
    }

    fn url(&self) -> Arc<String> {
        self.url.clone()
    }
}

pub enum SizeResult {
    Known(u64),
    Stream,
    Retryable(u16),
    PermanentFail,
}

pub enum SupervisorMessage {
    ProcessPermit(ActiveDownloadPermit),
    SpawnWorker(ValidDownloadPermit),
    RangeSuccess(ActiveDownloadPermit, usize, (usize, usize)), // permit, id, range
    RangeFailed(ActiveDownloadPermit, usize, (usize, usize), Arc<String>, Arc<SharedFileMap>, u64, RangeDownloadError), // permit, id, range, url
    StreamSuccess(ActiveDownloadPermit, usize, usize), // permit, id, size of file
    StreamFailed(ActiveDownloadPermit, usize, Arc<String>, PathBuf, DownloadError), // permit, id, url, path, error
    MetadataFetched(ActiveDownloadPermit, usize, Arc<String>, SizeResult), 
    IoError(ActiveDownloadPermit, std::io::Error, RetryKind),
    RetryAfter(ActiveDownloadPermit, Duration, RetryKind),
    RetryReady(RetryKind),
    RateLimited(ActiveDownloadPermit, Option<u64>, RetryKind), 
    NetworkError(ActiveDownloadPermit, reqwest::Error, RetryKind),
    ServerError(ActiveDownloadPermit, StatusCode, RetryKind),
    ClientError(ActiveDownloadPermit, StatusCode, RetryKind),
}

#[derive(Debug)]
pub enum Job {
    GetSize { file_id: usize, url: Arc<String> },
    DownloadChunk {
        file_id: usize, 
        url: Arc<String>,
        range: (usize, usize),
        file_map: Arc<SharedFileMap>,
        expected_len: u64,
    }, // file id, url, range
    DownloadStream(usize, Arc<String>, PathBuf), // file id, url
    AwaitingMetadata,
}

// TODO: try to see if i can implement a get_next_chunk()

struct SupervisorState {
    client: Client,
    download: Download,
    chunk_cursors: HashMap<usize, usize>, // used to keep track of last tracked chunk in a file to avoid looping through all the chunks every time
    uninitialized_cursor: usize, // track the last known initialized file
    streams_cursor: usize, // track the last known stream-only file
    // TODO: change this to a vec
    retry_ranges: HashMap<usize, Vec<((usize, usize), Arc<String>, Arc<SharedFileMap>, u64)>>, // ranges that failed but are still buffered
    retry_uninitialized: Vec<(usize, Arc<String>)>, // tracks the files that failed to get metadata
    retry_streams: Vec<(usize, Arc<String>, PathBuf)>, // tracks the files that failed to get metadata
    host_sender: UnboundedSender<HostMessage>,
    permit_count: Arc<AtomicUsize>, 
    active_downloads: usize, // tracks how many permits we are using to download files
    active_metadata_requests: usize, // tracks how many permits we are using to gather metadata
    file_maps: HashMap<usize, Arc<SharedFileMap>>, // Tracks file maps to get memory mapped files
    ui_sender: UnboundedSender<UiStateEvent>,
    db_manager: StateManager,
    idle_permits: Vec<ActiveDownloadPermit>,
    retry_queue_count: usize, // tracks how many downloads we are trying. Useful for when there are no jobs and nothing in the retry queue due to retry timeout or delay
}

impl SupervisorState {
    fn new(client: Client, download: Download, host_sender: UnboundedSender<HostMessage>, ui_sender: UnboundedSender<UiStateEvent>, db_manager: StateManager, permit_count: Arc<AtomicUsize>) -> Self {
        Self { 
            client,
            download,
            chunk_cursors: HashMap::new(),
            uninitialized_cursor: 0,
            streams_cursor: 0,
            retry_ranges: HashMap::new(),
            retry_uninitialized: Vec::new(),
            retry_streams: Vec::new(),
            host_sender,
            permit_count,
            active_downloads: 0,
            active_metadata_requests: 0,
            file_maps: HashMap::new(),
            ui_sender,
            db_manager,
            idle_permits: Vec::new(),
            retry_queue_count: 0,
        }
    }

    // Gets the next job the supervisor should perform. It can either be a file download, 
    // or gathering the metadata from a file whose size is still unknown
    async fn get_next_job(&mut self) -> Option<Job> {
        // Check for files that need sizes
        let metadata_info = self.get_next_uninitialized_file();

        if let Some((next_metadata_id, ref url)) = metadata_info {
            // if we hold only one permit, prioritize downloading metadata 
            if self.permit_count.load(Ordering::SeqCst) == 1 {
                return Some(self.take_metadata_job(next_metadata_id, url.clone()));
            }
        }

        // we prefer to file downloads over metadata if less than half of the active permits are being used
        // this way, we can gather the metadata with the permits left
        let prefer_downloads = self.active_downloads < (self.permit_count.load(Ordering::SeqCst) / 2);

        if prefer_downloads {
            if let Some(job) = self.try_take_chunk_job().await { return Some(job); }
            if let Some(job) = self.try_take_stream_job() { return Some(job); }
            if let Some((id, url)) = metadata_info { return Some(self.take_metadata_job(id, url)); }
        } else {
            if let Some((id, url)) = metadata_info { return Some(self.take_metadata_job(id, url)); }
            if let Some(job) = self.try_take_chunk_job().await { return Some(job); }
            if let Some(job) = self.try_take_stream_job() { return Some(job); }
        }

        // we found no job, but there are metadata requests active, meaning we have to wait for them to finish
        if self.active_metadata_requests > 0 {
            return Some(Job::AwaitingMetadata);
        }

        None
    }

    /// Tries to get a stream job, if a stream job is found, updates the cursor and returns the job.
    /// Otherwise, None is returned and the cursor is left unchanged.
    fn try_take_stream_job(&mut self) -> Option<Job> {
        if let Some((file_id, url, path)) = self.retry_streams.pop() {
            return Some(Job::DownloadStream(file_id, url, path));
        }
        
        let cursor = self.streams_cursor;

        for (&file_id, file) in self.download.files()[cursor..].iter() {
            if let DownloadType::File(file) = file && file.size() == Some(FileSize::Unknown) {
                self.streams_cursor += 1;

                return Some(Job::DownloadStream(file_id, file.url(), file.relative_path().to_owned()));
            }
        };

        None
    }

    async fn try_take_chunk_job(&mut self) -> Option<Job> {
        for (&file_id, download_type) in self.download.files().iter() {
            // skip files that are already completed and folders
            let file_download = match download_type {
                DownloadType::File(file) if file.status() != DownloadStatus::Completed => file,
                _ => continue, // we skip folders
            };

            // Check for retries first on this file
            if let Some(retry_range) = self.retry_ranges.get_mut(&file_id) {
                if !retry_range.is_empty() {
                    let (range, url, file_map, expected_len) = retry_range.pop()?;
                    return Some(Job::DownloadChunk { file_id, url, range, file_map, expected_len });
                }
            }

            // get cursor for this particular file
            let cursor = *self.chunk_cursors.entry(file_id).or_insert(0);

            let chunks = file_download.chunks();

            // This means that the metadata is still not fetched, so we can skip it
            if chunks.is_empty() {
                continue;
            }

            // Try to find an undownloaded chunk
            if let Some(relative_start) = chunks[cursor..].first_zero() {
                let start_index = relative_start + cursor;
                
                let target_chunk_size = 5242880 / 16384; // 5 MB / 16 KB
                let max_end = (start_index + target_chunk_size).min(chunks.len());

                let end_index = chunks[start_index..max_end]
                    .first_one()
                    .map(|idx| idx + start_index)
                    .unwrap_or(max_end);

                self.chunk_cursors.insert(file_id, end_index);

                let range = (start_index, end_index);

                let file_size = match file_download.size()? {
                    FileSize::Unknown => return None,
                    FileSize::Known(file_size) => file_size,
                };

                let expected_len = self.calculate_chunk_expected_len(16384, range, file_size);

                let path = file_download.relative_path();

                if !file_download.relative_path().exists() {
                    if let Some(parent_path) = path.parent() {
                        create_dir_all(parent_path).await.unwrap();
                    }

                    let file = tokio::fs::OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(&path)
                        .await;

                    if let Ok(f) = file {
                        f.set_len(file_size).await.unwrap();
                    }
                }

                let file_map = if self.file_maps.contains_key(&file_id) {
                    self.file_maps.get(&file_id).unwrap().clone()
                } else {
                    self.file_maps.insert(file_id, Arc::new(SharedFileMap::new(path, file_size)));
                    self.file_maps.get(&file_id).unwrap().clone()
                };

                return Some(Job::DownloadChunk { 
                    file_id, 
                    url: file_download.url(), 
                    range, 
                    file_map, 
                    expected_len,
                })
            }
        }

        None
    }

    /// Gets a metadata job and automatically updates its cursor as needed
    fn take_metadata_job(&mut self, id: usize, url: Arc<String>) -> Job {
        if id >= self.uninitialized_cursor {
            self.uninitialized_cursor = id + 1;
        }
        Job::GetSize { file_id: id, url }
    }

    fn get_next_uninitialized_file(&mut self) -> Option<(usize, Arc<String>)> {
        if let Some(uninitialized) = self.retry_uninitialized.pop() {
            return Some(uninitialized);
        }

        if self.download.files().is_empty() {
            return None;
        }

        // get the cursor of the file, if no cursor exists in the map, then insert one and return 0
        let cursor = self.uninitialized_cursor;

        for (&index, download_type) in self.download.files()[cursor..].iter() {
            if let DownloadType::File(file) = download_type {
                if file.size() == None {
                    return Some((index, file.url()));
                }
            }
        }
        
        None
    }

    fn calculate_chunk_expected_len(&self, chunk_size: u64, range: (usize, usize), file_size: u64) -> u64 {
        let start_byte = range.0 as u64 * chunk_size;
        let theoretical_end = range.1 as u64 * chunk_size;

        let actual_end = std::cmp::min(theoretical_end, file_size);
        let expected_len = actual_end.saturating_sub(start_byte);
        let expected_len = expected_len.min(file_size);

        expected_len
    }
}

pub struct DownloadSupervisor {
    state: Option<SupervisorState>,
    sender: Option<UnboundedSender<SupervisorMessage>>,
    shutdown_receiver: Option<oneshot::Receiver<SupervisorState>>,
    saturated: Arc<AtomicBool>,
    permit_count: Arc<AtomicUsize>,
}

impl DownloadSupervisor {
    pub fn new(client: Client, download: Download, host_sender: UnboundedSender<HostMessage>, ui_sender: UnboundedSender<UiStateEvent>, db_manager: StateManager) -> Self {
        println!("Supervisor created for: {}", download.name());
        let permit_count: Arc<AtomicUsize> = Arc::new(0.into());

        Self {
            state: Some(SupervisorState::new(client, download, host_sender, ui_sender, db_manager, permit_count.clone())),
            sender: None,
            shutdown_receiver: None,
            saturated: Arc::new(false.into()),
            permit_count,
        }
    }

    fn spawn(&mut self, sender: UnboundedSender<SupervisorMessage>, mut receiver: UnboundedReceiver<SupervisorMessage>) {
        let mut state = self.state.take();
        let mut previous_shutdown_receiver = self.shutdown_receiver.take();

        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        self.shutdown_receiver = Some(shutdown_receiver);
        let saturated = self.saturated.clone();

        tokio::spawn(async move {
            let mut state = if let Some(state) = state.take() {
                state
            } else if let Some(previous_shutdown_receiver) = previous_shutdown_receiver.take() {
                match previous_shutdown_receiver.await {
                    Ok(state) => state,
                    Err(_) => panic!("Previous supervisor panicked and lost the state! Download corrupted."), // TODO: replace this with a message to host
                }
            } else {
                panic!("Supervisor in inconsistent state: No local state and no recovery channel!");
            };

            let mut save_interval = tokio::time::interval(Duration::from_millis(100));

            
                loop {
                    tokio::select! {
                        Some(message) = receiver.recv() => {
                            match message {
                                SupervisorMessage::ProcessPermit(permit) => {
                                    if let Some(permit) = permit.validate() {
                                        let _ = sender.send(SupervisorMessage::SpawnWorker(permit));
                                    } else {

                                    }
                                }
                                SupervisorMessage::SpawnWorker(permit) => {
                                    println!("spawning worker for download: {}, permits: {}, downloads: {}", state.download.name(), state.permit_count.load(Ordering::SeqCst), state.active_downloads);

                                    let sender = sender.clone();

                                    // no next job means either we are finished or all remaining jobs are already taken
                                    // in any case, we send the permit back to the host
                                    let job = match state.get_next_job().await {
                                        Some(job) => {
                                            println!("found job {:?} for download: {}", job, state.download.name());
                                            job
                                        },
                                        None => {
                                            if state.retry_queue_count > 0 {
                                                println!("there are still retries in queue");
                                                // we put ourselves as saturated (not accepting more permits) until at least one retry is ready
                                                drop(permit);
                                                saturated.store(true, Ordering::Relaxed);
                                                continue;
                                            }

                                            println!("no jobs left");


                                            // We tell the host manager that we are saturated so it doesn't try to send more permits
                                            saturated.store(true, Ordering::Relaxed);
                        
                                            // We drop the permit so it gets sent back to the host manager
                                            drop(permit);

                                            // check if there is no more work to do
                                            if state.permit_count.load(Ordering::SeqCst) == 0 && state.active_downloads == 0 {
                                                let mut download_complete = true;
                                                for file in state.download.files().values() {
                                                    if let DownloadType::File(f) = file {
                                                        if f.status() != DownloadStatus::Completed {
                                                            download_complete = false;
                                                            break;
                                                        }
                                                    }
                                                }

                                                if download_complete {                 
                                                    state.db_manager.write_download(&state.download).await;
                                                    state.download.set_status(DownloadStatus::Completed);
                                                    let _ = state.host_sender.send(HostMessage::DownloadFinished(state.download.id()));
                                                    break;
                                                } else {
                                                    // the download might be in a stalled state so we reset all cursors to find the missing chunks
                                                    // this should hopefully never happen if the worker reports correctly when it failed

                                                }
                                            }

                                            continue;
                                        },
                                    };

                                    match job {
                                        Job::AwaitingMetadata => {
                                            state.idle_permits.push(permit.downgrade());
                                            saturated.store(true, Ordering::Relaxed);
                                        }
                                        Job::GetSize { file_id, url } => {
                                            println!("getting size for download: {}", state.download.name());
                                            state.active_metadata_requests += 1;

                                            let client = state.client.clone();

                                            tokio::spawn(async move {  
                                                let size_result = fetch_file_size(&client, &url).await;

                                                let _ = sender.send(SupervisorMessage::MetadataFetched(permit.downgrade(), file_id, url, size_result));
                                            });
                                        },
                                        Job::DownloadChunk { file_id, url, range, file_map, expected_len } => {
                                            state.active_downloads += 1;

                                            let client = state.client.clone();
          
                                            tokio::spawn(async move {
                                                // Do worker stuff
                                                match download_range(client, &url, range, file_map.clone(), expected_len).await {
                                                    Ok(_) => {
                                                        let _ = sender.send(SupervisorMessage::RangeSuccess(permit.downgrade(), file_id, range));
                                                    }
                                                    Err(download_error) => {
                                                        let _ = sender.send(SupervisorMessage::RangeFailed(permit.downgrade(), file_id, range, url, file_map, expected_len, download_error));
                                                    }
                                                }
                                            });
                                        },
                                        Job::DownloadStream(file_id, url, path) => {
                                            state.active_downloads += 1;
                                            let client = state.client.clone();
                                            
                                            tokio::spawn(async move {
                                                // Do worker stuff
                                                match download_stream(client, &path, &url).await {
                                                    Ok(size_downloaded) => {
                                                        let _ = sender.send(SupervisorMessage::StreamSuccess(permit.downgrade(), file_id, size_downloaded));
                                                    },
                                                    Err(download_error) => {
                                                        let _ = sender.send(SupervisorMessage::StreamFailed(permit.downgrade(), file_id, url, path, download_error));
                                                    },
                                                }
                                            });
                                        }
                                    }
                                }
                                SupervisorMessage::StreamFailed(permit, file_id, url, path, result) => {
                                    state.active_downloads -= 1;

                                    let retry_kind = RetryKind::StreamDownload(StreamRetry { file_id, url, path });

                                    match result {
                                        DownloadError::Io(error) =>  {
                                            let _ = sender.send(SupervisorMessage::IoError(permit, error, retry_kind));
                                        },
                                        DownloadError::Network(error) => {
                                            let _ = sender.send(SupervisorMessage::NetworkError(permit, error, retry_kind));
                                        },
                                        DownloadError::RateLimited(retry_after) => {
                                            let _ = sender.send(SupervisorMessage::RateLimited(permit, retry_after, retry_kind));
                                        },
                                        DownloadError::ServerError(status_code) => {
                                            let _ = sender.send(SupervisorMessage::ServerError(permit, status_code, retry_kind));
                                        },
                                        DownloadError::ClientError(status_code) => {
                                            let _ = sender.send(SupervisorMessage::ClientError(permit, status_code, retry_kind));
                                        },
                                    }
                                }
                                SupervisorMessage::StreamSuccess(permit, file_id, size) => {
                                    state.active_downloads -= 1;

                                    let download_id = state.download.id();
                                    let chunk_size = 16384;

                                    match state.download.files_mut().get_mut(&file_id).unwrap() {
                                        DownloadType::File(file) => {
                                            file.reset_retries();
                                            file.set_size(FileSize::Known(size as u64));
                                            file.set_bytes_downloaded(size as u64);

                                            if size > 0 {
                                                let chunk_count = (size + chunk_size - 1) / chunk_size;
                                                file.chunks_mut().resize(chunk_count as usize, true);
                                                println!("got chunk size completed: {}/{}", file.chunks_mut().count_ones(), file.chunks().len());
                                                file.set_status(DownloadStatus::Completed); 
                                            } else {
                                                println!("got 0 bytes: {}", file.name());
                                                // 0 Byte file
                                                file.set_status(DownloadStatus::Completed);
                                            }

                                            let _ = state.ui_sender.send(UiStateEvent::AddUpdate(DownloadUpdate::FileUpdated { id: download_id, file_update: FileUpdate::FileSize { id: file_id, len: size as u64 } }));
                                            let _ = state.ui_sender.send(UiStateEvent::AddUpdate(DownloadUpdate::FileUpdated { id: download_id, file_update: FileUpdate::BytesDownloaded { id: file_id, len: size as u64 } }));
                                            let _ = state.ui_sender.send(UiStateEvent::AddUpdate(DownloadUpdate::FileUpdated { id: download_id, file_update: FileUpdate::Status { id: file_id, status: DownloadStatus::Completed } }));
                                        },
                                        DownloadType::Folder(_) => todo!(),
                                    }

                                    // try to spawn another worked
                                    let _ = sender.send(SupervisorMessage::ProcessPermit(permit));

                                },
                                SupervisorMessage::RangeSuccess(permit, file_id, range) => {
                                    state.active_downloads -= 1;

                                    let download_id = state.download.id();

                                    let chunk_size = 16384;
                                    
                                    match state.download.files_mut().get_mut(&file_id).unwrap() {
                                        crate::download::DownloadType::File(file_download) => {
                                            file_download.reset_retries();

                                            let total_size = match file_download.size() {
                                                Some(FileSize::Known(size)) => size,
                                                Some(FileSize::Unknown) => {
                                                    eprintln!("A file with not yet unknown size has had a piece downloaded!");
                                                    continue;
                                                }
                                                None => {
                                                    eprintln!("A file with not yet resolved size has had a piece downloaded!");
                                                    continue;
                                                },
                                            };

                                            let start_byte = range.0 as u64 * chunk_size;
                                            let theoretical_end = range.1 as u64 * chunk_size;

                                            let actual_end = std::cmp::min(theoretical_end, total_size);
                                            let bytes_in_range = actual_end.saturating_sub(start_byte);
                                    

                                            let bytes_downloaded = file_download.bytes_downloaded() + bytes_in_range;
                                            file_download.set_bytes_downloaded(bytes_downloaded);

                                            let name = file_download.name().to_string();
                                            let bytes_downloaded = file_download.bytes_downloaded();

                                            let _ = state.ui_sender.send(UiStateEvent::AddUpdate(DownloadUpdate::FileUpdated { id: download_id, file_update: FileUpdate::BytesDownloaded { id: file_id, len: bytes_downloaded } }));


                                            file_download.chunks_mut()[range.0..range.1].fill(true);

                                            let all_chunks_done = file_download.chunks().all();

                                            if all_chunks_done {
                                                println!("file {} finished! got {} bytes", name, bytes_downloaded);
                                                let _ = state.ui_sender.send(UiStateEvent::AddUpdate(DownloadUpdate::FileUpdated { id: download_id, file_update: FileUpdate::Status { id: file_id, status: DownloadStatus::Completed } }));
                                                file_download.set_status(DownloadStatus::Completed);
                                                state.file_maps.remove(&file_id);
                                            }
                                        },
                                        crate::download::DownloadType::Folder(_) => todo!(),
                                    } 

                                    // try to spawn another worked
                                    let _ = sender.send(SupervisorMessage::ProcessPermit(permit));
                                },
                                SupervisorMessage::RangeFailed(permit, file_id, range, url, file_map, expected_len, download_error) => {
                                    let retry_kind = RetryKind::RangeDownload(RangeDownload { file_id, range, url: url.clone(), file_map, expected_len });
                                    let file_id = retry_kind.file_id();
                                    let download_name = state.download.name().clone();

                                    let file = match state.download.files_mut().get_mut(&file_id) {
                                        Some(DownloadType::File(file)) => file,
                                        _ => continue,
                                    };

                                    match download_error {
                                        RangeDownloadError::Download(download_error) => {
                                            match download_error {
                                                DownloadError::Io(error) =>  {
                                                    let _ = sender.send(SupervisorMessage::IoError(permit, error, retry_kind));
                                                },
                                                DownloadError::Network(error) => {
                                                    let _ = sender.send(SupervisorMessage::NetworkError(permit, error, retry_kind));
                                                },
                                                DownloadError::RateLimited(retry_after) => {
                                                    let _ = sender.send(SupervisorMessage::RateLimited(permit, retry_after, retry_kind));
                                                },
                                                DownloadError::ServerError(status_code) => {
                                                    let _ = sender.send(SupervisorMessage::ServerError(permit, status_code, retry_kind));
                                                },
                                                DownloadError::ClientError(status_code) => {
                                                    let _ = sender.send(SupervisorMessage::ClientError(permit, status_code, retry_kind));
                                                },
                                            }
                                        },
                                        RangeDownloadError::UnexpectedStatus(status_code) => {
                                            println!("got unexpected status code length for: {} {}. received: {}", download_name, file_id, status_code);
                                            file.increment_retries();
                                            if file.retries() > 5 { 
                                                // Try to download this as chunked as fallback
                                                file.set_size(FileSize::Unknown);
                                                file.reset_retries();
                                                state.retry_streams.push((file_id, url, file.relative_path().to_owned()));
                                            } else {
                                                let _ = sender.send(SupervisorMessage::RetryAfter(permit, Duration::from_secs(5), retry_kind));
                                            }
                                        },
                                        RangeDownloadError::UnexpectedLength(bytes_received, bytes_expected) => {
                                            println!("got unexpected length for: {} {}. received: {}, expected: {}", download_name, file_id, bytes_received, bytes_expected);

                                            file.increment_retries();
                                            if file.retries() > 5 { 
                                                // Try to download this as chunked as fallback
                                                file.set_size(FileSize::Unknown);
                                                file.reset_retries();
                                                state.retry_streams.push((file_id, url, file.relative_path().to_owned()));
                                                let _ = sender.send(SupervisorMessage::ProcessPermit(permit));
                                            } else {
                                                // This error is usually from a droppped connection, so don't wait much before retrying
                                                let _ = sender.send(SupervisorMessage::RetryAfter(permit, Duration::from_millis(300), retry_kind));
                                            }
                                        },
                                        RangeDownloadError::RangeNotSupported => {
                                            println!("got non-range response for: {} {}.", download_name, file_id);

                                            // Set this file as having an unknown length so it can be downloaded as chunked
                                            file.set_size(FileSize::Unknown);
                                            file.reset_retries();
                                            state.retry_streams.push((file_id, url, file.relative_path().to_owned()));
                                            let _ = sender.send(SupervisorMessage::ProcessPermit(permit));
                                        },
                                    }
                                }
                                SupervisorMessage::MetadataFetched(permit, file_id, url, size_result) => {
                                    println!("got metadata for: {} {}", state.download.name(), file_id);
                                    state.active_metadata_requests -= 1;
                                    saturated.store(false, Ordering::Relaxed);
                                    let _ = state.host_sender.send(HostMessage::RequestPermits(state.download.id())); 

                                    let download_id = state.download.id();
                                    let file = match state.download.files_mut().get_mut(&file_id) {
                                        Some(DownloadType::File(file)) => file,
                                        _ => continue,
                                    };

                                    match size_result {
                                        SizeResult::Known(size) => {
                                            println!("Got known metadata size for {}. Got {}", file_id, size);
                                            let download_id = state.download.id();

                                            if let Some(DownloadType::File(file)) = state.download.files_mut().get_mut(&file_id) {
                                                file.reset_retries();
                                                file.set_size(FileSize::Known(size));
                                                let _ = state.ui_sender.send(UiStateEvent::AddUpdate(DownloadUpdate::FileUpdated { id: download_id, file_update: FileUpdate::FileSize { id: file_id, len: size } }));
                                                
                                                // todo, make this a global or store it somewhere
                                                let chunk_size = 16384; // 16 KB

                                                // Initialize chunks
                                                if size > 0 {
                                                    let chunk_count = (size + chunk_size - 1) / chunk_size;
                                                    file.chunks_mut().resize(chunk_count as usize, false);
                                                    file.set_status(DownloadStatus::InProgress); 
                                                } else {
                                                    println!("got 0 bytes: {}", file.name());
                                                    // 0 Byte file
                                                    file.set_status(DownloadStatus::Completed);
                                                }
                                            }
                                        },
                                        SizeResult::Stream => {
                                            println!("Got no known metadata size for {}. Setting to stream.", file_id);
                                            if let Some(DownloadType::File(file)) = state.download.files_mut().get_mut(&file_id) {
                                                file.reset_retries();
                                                file.set_size(FileSize::Unknown);
                                            }
                                        },
                                        SizeResult::Retryable(_) => {
                                            file.increment_retries();
                                            if file.retries() > 5 { 
                                                println!("Failed to get metadata size for {}.", file_id);
                                                file.set_status(DownloadStatus::Failed(DownloadFailureReason::MetadataFetchError));
                                                let _ = state.ui_sender.send(UiStateEvent::AddUpdate(DownloadUpdate::FileUpdated { id: download_id, file_update: FileUpdate::Status { id: file_id, status: file.status() } }));
                                            } else {
                                                println!("Failed to get metadata size for {}. Retrying.", file_id);
                                                state.retry_uninitialized.push((file_id, url));
                                            }
                                        },
                                        SizeResult::PermanentFail => todo!(),
                                    }

                                    while let Some(permit) = state.idle_permits.pop() {
                                        let _ = sender.send(SupervisorMessage::ProcessPermit(permit));
                                    }

                                    let _ = sender.send(SupervisorMessage::ProcessPermit(permit));
                                },
                                SupervisorMessage::RetryAfter(permit, duration, retry_kind) => {
                                    let _ = sender.send(SupervisorMessage::ProcessPermit(permit));

                                    let download_id = state.download.id();
                                    let file_id = retry_kind.file_id();

                                    let file = match state.download.files_mut().get_mut(&file_id).unwrap() {
                                        DownloadType::File(file) => file,
                                        DownloadType::Folder(_) => continue,
                                    };

                                    file.set_status(DownloadStatus::Retrying);

                                    let _ = state.ui_sender.send(UiStateEvent::AddUpdate(DownloadUpdate::FileUpdated { id: download_id, file_update: FileUpdate::Status { id: file_id, status: DownloadStatus::Retrying } }));

                                    let sender = sender.clone();

                                    state.retry_queue_count += 1;
                                    tokio::spawn(async move {
                                        tokio::time::sleep(duration).await;

                                        let _ = sender.send(SupervisorMessage::RetryReady(retry_kind)); 
                                    });
                                },
                                SupervisorMessage::RetryReady(retry_kind) => {
                                    state.retry_queue_count -= 1;
                                    saturated.store(false, Ordering::Relaxed);

                                    match retry_kind {
                                        RetryKind::Metadata(metadata_retry) => {
                                            state.retry_uninitialized.push((metadata_retry.file_id, metadata_retry.url));
                                        },
                                        RetryKind::StreamDownload(stream_retry) => {
                                            state.retry_streams.push((stream_retry.file_id, stream_retry.url, stream_retry.path));
                                        },
                                        RetryKind::RangeDownload(range_download) => {
                                            state.retry_ranges
                                                .entry(range_download.file_id)
                                                .or_default()
                                                .push((range_download.range, range_download.url, range_download.file_map, range_download.expected_len));
                                        },
                                    }

                                    let _ = state.host_sender.send(HostMessage::RequestPermits(state.download.id())); 
                                }
                                SupervisorMessage::NetworkError(permit, error, retry_kind) => {
                                    eprintln!("Network Error for {}: {}. Retrying...", retry_kind.file_id(), error);

                                    let _ = sender.send(SupervisorMessage::RetryAfter(permit, Duration::from_secs(5), retry_kind)); 
                                },
                                SupervisorMessage::ServerError(permit, status_code, retry_kind) => {
                                    eprintln!("Server error ({}). Retrying...", status_code);

                                    let download_id = state.download.id();
                                    let file_id = retry_kind.file_id();

                                    let file = match state.download.files_mut().get_mut(&file_id).unwrap() {
                                        DownloadType::File(file) => file,
                                        DownloadType::Folder(_) => continue,
                                    };

                                    file.increment_retries();
                                    if file.retries() > 5 { 
                                        file.set_status(DownloadStatus::Failed(DownloadFailureReason::ServerError));
                                        let _ = state.ui_sender.send(UiStateEvent::AddUpdate(DownloadUpdate::FileUpdated { id: download_id, file_update: FileUpdate::Status { id: file_id, status: file.status() } }));
                                    } else {
                                        let _ = sender.send(SupervisorMessage::RetryAfter(permit, Duration::from_secs(5), retry_kind));
                                    }
                                },
                                SupervisorMessage::ClientError(permit, status_code, retry_kind) => {
                                    eprintln!("Client error ({}).", status_code);

                                    let download_id = state.download.id();
                                    let file_id = retry_kind.file_id();

                                    let file = match state.download.files_mut().get_mut(&file_id).unwrap() {
                                        DownloadType::File(file) => file,
                                        DownloadType::Folder(_) => todo!(),
                                    };

                                    file.set_status(DownloadStatus::Failed(DownloadFailureReason::ClientError));

                                    let _ = sender.send(SupervisorMessage::ProcessPermit(permit));
                                    let _ = state.ui_sender.send(UiStateEvent::AddUpdate(DownloadUpdate::FileUpdated { id: download_id, file_update: FileUpdate::Status { id: file_id, status: file.status() } }));
                                }
                                SupervisorMessage::RateLimited(_permit, retry_after, retry_kind) => {
                                    let file = match state.download.files_mut().get_mut(&retry_kind.file_id()).unwrap() {
                                        DownloadType::File(file) => file,
                                        DownloadType::Folder(_) => continue,
                                    };

                                    eprintln!("Rate limited for {}.", retry_kind.file_id());
                                    file.set_status(DownloadStatus::Waiting(retry_after));
                                    
                                    state.retry_queue_count += 1;
                                    let _ = sender.send(SupervisorMessage::RetryReady(retry_kind));
                                    let _ = state.host_sender.send(HostMessage::RateLimited(retry_after));
                                },
                                SupervisorMessage::IoError(permit, error, retry_kind) => {
                                    let download_id = state.download.id();
                                    let file_id = retry_kind.file_id();

                                    let file = match state.download.files_mut().get_mut(&retry_kind.file_id()).unwrap() {
                                        DownloadType::File(file) => file,
                                        DownloadType::Folder(_) => continue,
                                    };

                                    match error.kind() {
                                        // Permanent Errors that should not be retried
                                        std::io::ErrorKind::NotFound |
                                        std::io::ErrorKind::PermissionDenied |
                                        std::io::ErrorKind::NotADirectory |
                                        std::io::ErrorKind::IsADirectory |
                                        std::io::ErrorKind::InvalidInput |
                                        std::io::ErrorKind::AddrInUse |
                                        std::io::ErrorKind::AddrNotAvailable |
                                        std::io::ErrorKind::AlreadyExists |
                                        std::io::ErrorKind::DirectoryNotEmpty |
                                        std::io::ErrorKind::ReadOnlyFilesystem |
                                        std::io::ErrorKind::StaleNetworkFileHandle |
                                        std::io::ErrorKind::InvalidData |
                                        std::io::ErrorKind::NotSeekable |
                                        std::io::ErrorKind::CrossesDevices |
                                        std::io::ErrorKind::TooManyLinks |
                                        std::io::ErrorKind::InvalidFilename |
                                        std::io::ErrorKind::ArgumentListTooLong |
                                        std::io::ErrorKind::Unsupported =>  {
                                            eprintln!("IO error: {error}");
                                            file.set_status(DownloadStatus::Failed(DownloadFailureReason::DiskError));
                                            // fail
                                        }
                                        
                                        // Storage errors
                                        std::io::ErrorKind::WriteZero |
                                        std::io::ErrorKind::StorageFull |
                                        std::io::ErrorKind::QuotaExceeded |
                                        std::io::ErrorKind::FileTooLarge |
                                        std::io::ErrorKind::OutOfMemory => {
                                            eprintln!("The system has ran out of storage: {error}");
                                            file.set_status(DownloadStatus::Failed(DownloadFailureReason::DiskError));
                                            // fail
                                        },

                                        // Retryiable errors
                                        std::io::ErrorKind::NetworkUnreachable |
                                        std::io::ErrorKind::WouldBlock |
                                        std::io::ErrorKind::ConnectionReset |
                                        std::io::ErrorKind::ConnectionAborted |
                                        std::io::ErrorKind::NotConnected |
                                        std::io::ErrorKind::NetworkDown |
                                        std::io::ErrorKind::BrokenPipe |
                                        std::io::ErrorKind::HostUnreachable |
                                        std::io::ErrorKind::TimedOut |
                                        std::io::ErrorKind::ResourceBusy |
                                        std::io::ErrorKind::ExecutableFileBusy |
                                        std::io::ErrorKind::Deadlock |
                                        std::io::ErrorKind::Interrupted => {
                                            eprintln!("Temporary OS error: {error}. Retrying...");

                                            let _ = sender.send(SupervisorMessage::RetryAfter(permit, Duration::from_secs(5), retry_kind));
                                        },
                                        
                                        _ => {
                                            eprintln!("OS error: {error}.");
                                            file.increment_retries();
                                            if file.retries() > 5 { 
                                                file.set_status(DownloadStatus::Failed(DownloadFailureReason::DiskError));
                                            } else {
                                                let _ = sender.send(SupervisorMessage::RetryAfter(permit, Duration::from_secs(5), retry_kind));
                                            }
                                        },
                                    }

                                    let _ = state.ui_sender.send(UiStateEvent::AddUpdate(DownloadUpdate::FileUpdated { id: download_id, file_update: FileUpdate::Status { id: file_id, status: file.status() } }));
                                },
                            }
                        }
                    _ = save_interval.tick() => {
                        state.db_manager.write_download(&state.download).await;
                    }
                }
        
            }
            // saves to db here for persitence and in case oneshot fails
            state.db_manager.write_download(&state.download).await;

            let _ = shutdown_sender.send(state);
        });
    }
        
    pub fn give_permit(&mut self, permit: ActiveDownloadPermit) {
        if let Some(sender) = &self.sender {
            if sender.is_closed() {
                self.sender = None;
            }
        }

        if self.sender.is_none() {
            // If the message wasn't sent correctly it might mean the thread died and we are hibernating
            let (sender, receiver) = unbounded_channel();
            self.sender = Some(sender.clone());
            self.spawn(sender, receiver);
        }

        let _ = self.sender.as_ref().unwrap().send(SupervisorMessage::ProcessPermit(permit));
    }

    pub fn is_saturated(&self) -> bool {
        self.saturated.load(Ordering::Acquire).into()
    }

    pub fn set_saturated(&mut self, saturated: bool) {
        self.saturated.store(saturated, Ordering::Relaxed); 
    }

    pub fn permit_count(&self) -> Arc<AtomicUsize> {
        self.permit_count.clone()
    }
}

async fn fetch_file_size(client: &reqwest::Client, url: &str) -> SizeResult {
    // Try a HEAD request first
    let head_result = client.head(url)
        .header("Accept-Encoding", "identity")
        .send()
        .await;

    if let Ok(response) = head_result {
        if let Some(len) = response.content_length() && response.status().is_success() {
            if len != 0 {
                return SizeResult::Known(len);
            }
        }
    }

    // If HEAD fails or returns no length, do a GET request and abort immediately to avoid downloading body
    let get_result = client.get(url)
        .header("Accept-Encoding", "identity")
        .header("Range", "bytes=0-0")
        .send()
        .await;

    if let Ok(response) = get_result {
        match response.status() {
            | StatusCode::PARTIAL_CONTENT => {
                if let Some(range_header) = response.headers().get(header::CONTENT_RANGE) {
                    if let Ok(str) = range_header.to_str() {
                        println!("parse successfuly: {}", str);
                        if let Some(total_size) = parse_content_range(str) && total_size != 0 {
                            println!("parsed correctly!");
                            return SizeResult::Known(total_size);
                        }

                        return SizeResult::Stream; 
                    }
                }
            }
            StatusCode::OK => {
                if let Some(len) = response.content_length() && len != 0 {
                    return SizeResult::Known(len)
                }

                return SizeResult::Stream;
            }
            StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE | StatusCode::GATEWAY_TIMEOUT => {
                return SizeResult::Retryable(response.status().as_u16());
            }
            _ => return SizeResult::PermanentFail,
        }
    }

    SizeResult::Retryable(0)
}

fn parse_content_range(range_header: &str) -> Option<u64> {
     // e.g. "bytes 0-0/1048576"
    range_header.rsplit('/').next()?.parse::<u64>().ok()
}

async fn download_range(client: Client, url: &str, range: (usize, usize), file_map: Arc<SharedFileMap>, expected_len: u64) -> Result<(), RangeDownloadError> {
    let chunk_size = 16384;

    let start_byte = range.0 as u64 * chunk_size;
    let end_byte = (range.1 as u64 * chunk_size) - 1; // -1 because http ranges are inclusive

    let range_header = format!("bytes={}-{}", start_byte, end_byte);

    let request = client.get(url)
        .header("Range", range_header);

    let mut response = match request.send().await {
        Ok(response) => match response.status() {
            StatusCode::TOO_MANY_REQUESTS => {
                let retry_after = response.headers().get(header::RETRY_AFTER).and_then(|header| {
                    let retry_after_str = header.to_str().ok()?;
                    
                    // Try parsing as seconds
                    if let Ok(seconds) = retry_after_str.parse::<u64>() {
                        return Some(seconds);
                    }

                        // Try parsing as HTTP-Date
                    if let Ok(date) = DateTime::parse_from_rfc2822(retry_after_str) {
                        let now = Utc::now();
                        let diff = date.with_timezone(&Utc).signed_duration_since(now);
                        return Some(diff.num_seconds().max(0) as u64);
                    }

                    None
                });

                Err(DownloadError::RateLimited(retry_after))
            },
            status if status.is_server_error() => Err(DownloadError::ServerError(status)),
            status if status.is_client_error() => Err(DownloadError::ClientError(status)),
            StatusCode::OK => {
                if start_byte != 0 {
                    return Err(RangeDownloadError::RangeNotSupported);
                }

                if let Some(content_length) = response.content_length() {
                    if content_length != end_byte + 1 {
                        return Err(RangeDownloadError::RangeNotSupported);
                    }
                };
                
                Ok(response)
            }
            StatusCode::PARTIAL_CONTENT => Ok(response),
            status => return Err(RangeDownloadError::UnexpectedStatus(status)),
        },
        Err(err) => return Err(DownloadError::Network(err).into()),
    }?;

    let mut current_offset = start_byte;
    let mut bytes_received = 0; 

    while let Some(chunk) = response.chunk().await.map_err(DownloadError::from)? {
        let chunk_len = chunk.len() as u64;
        file_map.write_chunk(current_offset as usize, &chunk);

        current_offset += chunk_len;
        bytes_received += chunk_len;
    }

    if bytes_received != expected_len {
        return Err(RangeDownloadError::UnexpectedLength(bytes_received, expected_len));
    }

    Ok(())
}

/// Downloads a file from a server that requested `Transfer-Encoding: chunked`. 
/// The server doesn't provide a `Content-Length` header for these files and thus they can't be downloaded using a multi-part strategy.
/// These downloads are non-resumable.
async fn download_stream(client: Client, path: &Path, url: &str) -> Result<usize, DownloadError> {
    let mut response = match client.get(url).send().await {
        Ok(response) => match response.status() {
            StatusCode::TOO_MANY_REQUESTS => {
                let retry_after = response.headers().get(header::RETRY_AFTER).and_then(|header| {
                    let retry_after_str = header.to_str().ok()?;
                    
                    // Try parsing as seconds
                    if let Ok(seconds) = retry_after_str.parse::<u64>() {
                        return Some(seconds);
                    }

                        // Try parsing as HTTP-Date
                    if let Ok(date) = DateTime::parse_from_rfc2822(retry_after_str) {
                        let now = Utc::now();
                        let diff = date.with_timezone(&Utc).signed_duration_since(now);
                        return Some(diff.num_seconds().max(0) as u64);
                    }

                    None
                });

                Err(DownloadError::RateLimited(retry_after))
            },
            status if status.is_server_error() => Err(DownloadError::ServerError(status)),
            status if status.is_client_error() => Err(DownloadError::ClientError(status)),
            _ => Ok(response),
        },
        Err(err) => return Err(DownloadError::Network(err)),
    }?;

    if let Some(parent_path) = path.parent() {
        create_dir_all(parent_path).await?;
    }

    let file = tokio::fs::File::create(&path).await?;

    let mut writer = BufWriter::new(file);
    let mut size = 0;

    while let Some(chunk) = response.chunk().await? {
        size += chunk.len();
        writer.write_all(&chunk).await?;
    }

    writer.flush().await?;

    Ok(size)
}