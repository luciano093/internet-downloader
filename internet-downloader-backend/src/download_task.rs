use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use reqwest::{Client, StatusCode, header};
use thiserror::Error;
use tokio::fs::create_dir_all;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::oneshot;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn};

use crate::client_state_manager::UiStateEvent;
use crate::context::AppContext;
use crate::download::{Download, DownloadFailureReason, DownloadId, DownloadItem, DownloadStatus, DownloadType, DownloadUpdate, FileSize, FileUpdate};
use crate::host_manager::{ActiveDownloadPermit, HostMessage, ValidDownloadPermit};
use crate::shared_file_map::SharedFileMap;

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

enum RetryKind {
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

enum SupervisorMessage {
    ProcessPermit(ActiveDownloadPermit),
    Pause,
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
}

#[derive(Debug)]
pub struct RangeRetryJob {
    pub range: (usize, usize),
    pub url: Arc<String>,
    pub file_map: Arc<SharedFileMap>,
    pub expected_len: u64,
}

// TODO: try to see if i can implement a get_next_chunk()

struct SupervisorState {
    download: Download,
    chunk_cursors: HashMap<usize, usize>, // used to keep track of last tracked chunk in a file to avoid looping through all the chunks every time
    uninitialized_cursor: usize, // track the last known initialized file
    streams_cursor: usize, // track the last known stream-only file
    // TODO: change this to a vec
    retry_ranges: HashMap<usize, Vec<RangeRetryJob>>, // ranges that failed but are still buffered
    retry_uninitialized: Vec<(usize, Arc<String>)>, // tracks the files that failed to get metadata
    retry_streams: Vec<(usize, Arc<String>, PathBuf)>, // tracks the files that failed to get metadata
    host_sender: UnboundedSender<HostMessage>,
    permit_count: Arc<AtomicUsize>, 
    active_downloads: usize, // tracks how many permits we are using to download files
    active_metadata_requests: usize, // tracks how many permits we are using to gather metadata
    file_maps: HashMap<usize, Arc<SharedFileMap>>, // Tracks file maps to get memory mapped files
    app_context: AppContext,
    retry_queue_count: usize, // tracks how many downloads we are trying. Useful for when there are no jobs and nothing in the retry queue due to retry timeout or delay
}

impl SupervisorState {
    fn new(app_context: AppContext, download: Download, host_sender: UnboundedSender<HostMessage>, permit_count: Arc<AtomicUsize>) -> Self {
        Self { 
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
            app_context,
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

        // we found no job
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
            if let Some(retry_range) = self.retry_ranges.get_mut(&file_id) 
                && !retry_range.is_empty()
            {
                let retry_range_job = retry_range.pop()?;

                return Some(Job::DownloadChunk {
                    file_id,
                    url: retry_range_job.url,
                    range: retry_range_job.range,
                    file_map: retry_range_job.file_map,
                    expected_len: retry_range_job.expected_len
                });
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

                let file_map = self.file_maps
                    .entry(file_id)
                    .or_insert_with(|| {
                        Arc::new(SharedFileMap::new(path.to_path_buf(), file_size))
                    })
                    .clone();

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
            if let DownloadType::File(file) = download_type
                && file.size().is_none()
            {
                return Some((index, file.url()));
            }
        }
        
        None
    }

    fn calculate_chunk_expected_len(&self, chunk_size: u64, range: (usize, usize), file_size: u64) -> u64 {
        let start_byte = range.0 as u64 * chunk_size;
        let theoretical_end = range.1 as u64 * chunk_size;

        let actual_end = std::cmp::min(theoretical_end, file_size);
        let expected_len = actual_end.saturating_sub(start_byte);
        
        expected_len.min(file_size)
    }
}

pub struct Active;
pub struct Inactive;

pub struct DownloadSupervisor {
    state: Option<SupervisorState>,
    sender: Option<UnboundedSender<SupervisorMessage>>,
    shutdown_receiver: Option<oneshot::Receiver<SupervisorState>>,
    demand: Arc<AtomicUsize>,
    permit_count: Arc<AtomicUsize>,
    handle: Option<JoinHandle<()>>,
    download_id: DownloadId,
    cancel_token: CancellationToken, // Used for killing workers forcefully if needed
}

impl DownloadSupervisor {
    pub async fn new(app_context: AppContext, download: Download, host_sender: UnboundedSender<HostMessage>) -> Self {
        debug!("Supervisor created for: {}", download.name());
        app_context.db_manager.write_download(&download).await;
        let permit_count: Arc<AtomicUsize> = Arc::new(0.into());
        let download_id = download.id();

        let initial_demand = Self::calculate_initial_demand(&download);

        Self {
            state: Some(SupervisorState::new(app_context.clone(), download, host_sender, permit_count.clone())),
            sender: None,
            shutdown_receiver: None,
            demand: Arc::new(AtomicUsize::new(initial_demand)),
            permit_count,
            handle: None,
            download_id,
            cancel_token: CancellationToken::new(),
        }
    }

    fn spawn(&mut self, sender: UnboundedSender<SupervisorMessage>, mut receiver: UnboundedReceiver<SupervisorMessage>) {
        let mut state = self.state.take();
        let mut previous_shutdown_receiver = self.shutdown_receiver.take();

        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        self.shutdown_receiver = Some(shutdown_receiver);
        let demand = self.demand.clone();
        let cancel_token = self.cancel_token.clone();

        let handle = tokio::spawn(async move {
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

            if state.download.is_completed() {
                let _ = state.host_sender.send(HostMessage::DownloadFinished(state.download.id()));
                return;
            }

            let mut save_interval = tokio::time::interval(Duration::from_millis(100));

            
                loop {
                    tokio::select! {
                        Some(message) = receiver.recv() => {
                            match message {
                                SupervisorMessage::ProcessPermit(permit) => {
                                    if let Some(permit) = permit.validate() {
                                        let _ = sender.send(SupervisorMessage::SpawnWorker(permit));
                                    }
                                }
                                SupervisorMessage::SpawnWorker(permit) => {
                                    trace!("spawning worker for download: {}, permits: {}, downloads: {}", state.download.name(), state.permit_count.load(Ordering::SeqCst), state.active_downloads);

                                    let sender = sender.clone();

                                    // no next job means either we are finished or all remaining jobs are already taken
                                    // in any case, we send the permit back to the host
                                    let job = match state.get_next_job().await {
                                        Some(job) => {
                                            trace!("found job {:?} for download: {}", job, state.download.name());
                                            job
                                        },
                                        None => {
                                            if state.retry_queue_count > 0 {
                                                trace!("there are still retries in queue");
                                                // We drop the permit so that it returns to the global pool.
                                                // When the retry timer is finished, it will increment the demand and request a new permit
                                                drop(permit);
                                                continue;
                                            }

                                            trace!("no jobs left");
                                            // We have no jobs, and no retries so our demand must be 0.
                                            demand.store(0, Ordering::SeqCst);
                        
                                            // We drop the permit so it gets sent back to the host manager
                                            drop(permit);

                                            // check if there is no more work to do
                                            if state.permit_count.load(Ordering::SeqCst) == 0 && state.active_downloads == 0 {
                                                if Self::is_download_finished(&state.download) {
                                                    debug!("All files completed for download {}. Exiting supervisor loop.", state.download.id());          
                                                    state.app_context.db_manager.write_download(&state.download).await;
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
                                        Job::GetSize { file_id, url } => {
                                            trace!("getting size for download: {}", state.download.name());
                                            state.active_metadata_requests += 1;

                                            let client = state.app_context.client.clone();

                                            let cancel_token = cancel_token.clone();

                                            tokio::spawn(async move {  
                                                tokio::select! {
                                                    _ = cancel_token.cancelled() => {
                                                        return; 
                                                    }

                                                    size_result = fetch_file_size(&client, &url) => {
                                                        let _ = sender.send(SupervisorMessage::MetadataFetched(permit.downgrade(), file_id, url, size_result));
                                                    }
                                                }
                                            });
                                        },
                                        Job::DownloadChunk { file_id, url, range, file_map, expected_len } => {
                                            state.active_downloads += 1;

                                            let client = state.app_context.client.clone();
                                            let cancel_token = cancel_token.clone(); 
          
                                            tokio::spawn(async move {
                                                tokio::select! {
                                                    _ = cancel_token.cancelled() => {
                                                        return;
                                                    }
                                                    // Do worker stuff
                                                    result = download_range(client, &url, range, file_map.clone(), expected_len) => {
                                                        match result {
                                                            Ok(_) => {
                                                                let _ = sender.send(SupervisorMessage::RangeSuccess(permit.downgrade(), file_id, range));
                                                            }
                                                            Err(download_error) => {
                                                                let _ = sender.send(SupervisorMessage::RangeFailed(permit.downgrade(), file_id, range, url, file_map, expected_len, download_error));
                                                            }
                                                        }
                                                    }
                                                }
                                                
                                            });
                                        },
                                        Job::DownloadStream(file_id, url, path) => {
                                            state.active_downloads += 1;
                                            let client = state.app_context.client.clone();
                                            let cancel_token = cancel_token.clone();
                                            
                                            tokio::spawn(async move {
                                                tokio::select! {
                                                    _ = cancel_token.cancelled() => {
                                                        return;
                                                    }
                                                    // Do worker stuff
                                                    result = download_stream(client, &path, &url) => {
                                                        match result {
                                                            Ok(size_downloaded) => {
                                                                let _ = sender.send(SupervisorMessage::StreamSuccess(permit.downgrade(), file_id, size_downloaded));
                                                            },
                                                            Err(download_error) => {
                                                                let _ = sender.send(SupervisorMessage::StreamFailed(permit.downgrade(), file_id, url, path, download_error));
                                                            },
                                                        }
                                                    }
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
                                    drop(permit);

                                    let download_id = state.download.id();
                                    let chunk_size = 16384;

                                    match state.download.files_mut().get_mut(&file_id).unwrap() {
                                        DownloadType::File(file) => {
                                            file.reset_retries();
                                            file.set_size(FileSize::Known(size as u64));
                                            file.set_bytes_downloaded(size as u64);

                                            if size > 0 {
                                                let chunk_count = size.div_ceil(chunk_size);
                                                file.chunks_mut().resize(chunk_count, true);
                                                trace!("got chunk size completed: {}/{}", file.chunks_mut().count_ones(), file.chunks().len());
                                                file.set_status(DownloadStatus::Completed); 
                                            } else {
                                                trace!("got 0 bytes: {}", file.name());
                                                // 0 Byte file
                                                file.set_status(DownloadStatus::Completed);
                                            }

                                            let _ = state.app_context.ui_sender.send(UiStateEvent::AddUpdate(DownloadUpdate::FileUpdated { id: download_id, file_update: FileUpdate::FileSize { id: file_id, len: size as u64 } }));
                                            let _ = state.app_context.ui_sender.send(UiStateEvent::AddUpdate(DownloadUpdate::FileUpdated { id: download_id, file_update: FileUpdate::BytesDownloaded { id: file_id, len: size as u64 } }));
                                            let _ = state.app_context.ui_sender.send(UiStateEvent::AddUpdate(DownloadUpdate::FileUpdated { id: download_id, file_update: FileUpdate::Status { id: file_id, status: DownloadStatus::Completed } }));
                                        },
                                        DownloadType::Folder(_) => todo!(),
                                    }

                                    if Self::is_download_finished(&state.download) {
                                        debug!("All files completed for download {}. Exiting supervisor loop.", download_id);
                                        state.app_context.db_manager.write_download(&state.download).await;
                                        state.download.set_status(DownloadStatus::Completed);
                                        let _ = state.host_sender.send(HostMessage::DownloadFinished(download_id));
                                        break;
                                    }
                                },
                                SupervisorMessage::RangeSuccess(permit, file_id, range) => {
                                    state.active_downloads -= 1;
                                    drop(permit); 

                                    let download_id = state.download.id();

                                    let chunk_size = 16384;
                                    
                                    match state.download.files_mut().get_mut(&file_id).unwrap() {
                                        crate::download::DownloadType::File(file_download) => {
                                            file_download.reset_retries();

                                            let total_size = match file_download.size() {
                                                Some(FileSize::Known(size)) => size,
                                                Some(FileSize::Unknown) => {
                                                    error!("A file with not yet unknown size has had a piece downloaded!");
                                                    continue;
                                                }
                                                None => {
                                                    error!("A file with not yet resolved size has had a piece downloaded!");
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

                                            let _ = state.app_context.ui_sender.send(UiStateEvent::AddUpdate(DownloadUpdate::FileUpdated { id: download_id, file_update: FileUpdate::BytesDownloaded { id: file_id, len: bytes_downloaded } }));


                                            file_download.chunks_mut()[range.0..range.1].fill(true);

                                            let all_chunks_done = file_download.chunks().all();

                                            if all_chunks_done {
                                                trace!("file {} finished! got {} bytes", name, bytes_downloaded);
                                                let _ = state.app_context.ui_sender.send(UiStateEvent::AddUpdate(DownloadUpdate::FileUpdated { id: download_id, file_update: FileUpdate::Status { id: file_id, status: DownloadStatus::Completed } }));
                                                file_download.set_status(DownloadStatus::Completed);
                                                state.file_maps.remove(&file_id);
                                            }
                                        },
                                        crate::download::DownloadType::Folder(_) => todo!(),
                                    }

                                    if Self::is_download_finished(&state.download) {
                                        debug!("All files completed for download {}. Exiting supervisor loop.", download_id);
                                        state.app_context.db_manager.write_download(&state.download).await;
                                        state.download.set_status(DownloadStatus::Completed);
                                        let _ = state.host_sender.send(HostMessage::DownloadFinished(download_id));
                                        break;
                                    }
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
                                            warn!("got unexpected status code length for: {} {}. received: {}", download_name, file_id, status_code);
                                            file.increment_retries();
                                            if file.retries() > 5 { 
                                                // Try to download this as chunked as fallback
                                                file.set_size(FileSize::Unknown);
                                                file.reset_retries();
                                                state.file_maps.remove(&file.id());
                                                state.retry_streams.push((file_id, url, file.relative_path().to_owned()));
                                                demand.fetch_add(1, Ordering::SeqCst); 
                                            } else {
                                                let _ = sender.send(SupervisorMessage::RetryAfter(permit, Duration::from_secs(5), retry_kind));
                                            }
                                        },
                                        RangeDownloadError::UnexpectedLength(bytes_received, bytes_expected) => {
                                            warn!("got unexpected length for: {} {}. received: {}, expected: {}", download_name, file_id, bytes_received, bytes_expected);

                                            file.increment_retries();
                                            if file.retries() > 5 { 
                                                // Try to download this as chunked as fallback
                                                file.set_size(FileSize::Unknown);
                                                file.reset_retries();
                                                state.file_maps.remove(&file.id());
                                                state.retry_streams.push((file_id, url, file.relative_path().to_owned()));
                                                demand.fetch_add(1, Ordering::SeqCst); 
                                            } else {
                                                // This error is usually from a droppped connection, so don't wait much before retrying
                                                let _ = sender.send(SupervisorMessage::RetryAfter(permit, Duration::from_millis(300), retry_kind));
                                            }
                                        },
                                        RangeDownloadError::RangeNotSupported => {
                                            warn!("got non-range response for: {} {}.", download_name, file_id);

                                            // Set this file as having an unknown length so it can be downloaded as chunked
                                            file.set_size(FileSize::Unknown);
                                            file.reset_retries();
                                            state.file_maps.remove(&file.id());
                                            state.retry_streams.push((file_id, url, file.relative_path().to_owned()));
                                            demand.fetch_add(1, Ordering::SeqCst); 
                                        },
                                    }
                                }
                                SupervisorMessage::MetadataFetched(permit, file_id, url, size_result) => {
                                    trace!("got metadata for: {} {}", state.download.name(), file_id);

                                    let download_id = state.download.id();
                                    let file = match state.download.files_mut().get_mut(&file_id) {
                                        Some(DownloadType::File(file)) => file,
                                        _ => continue,
                                    };

                                    match size_result {
                                        SizeResult::Known(size) => {
                                            trace!("Got known metadata size for {}. Got {}", file_id, size);
                                            let download_id = state.download.id();

                                            if let Some(DownloadType::File(file)) = state.download.files_mut().get_mut(&file_id) {
                                                file.reset_retries();
                                                file.set_size(FileSize::Known(size));
                                                let _ = state.app_context.ui_sender.send(UiStateEvent::AddUpdate(DownloadUpdate::FileUpdated { id: download_id, file_update: FileUpdate::FileSize { id: file_id, len: size } }));
                                                
                                                // todo, make this a global or store it somewhere
                                                let chunk_size = 16384; // 16 KB
                                                let target_range_size = 5242880 / 16384; // 320 chunks (5 MB)

                                                // Initialize chunks
                                                if size > 0 {
                                                    // Calculate how many 16KB chunks exist
                                                    let chunk_count = size.div_ceil(chunk_size);
                                                    file.chunks_mut().resize(chunk_count as usize, false);
                                                    file.set_status(DownloadStatus::InProgress); 

                                                    // Calculate how many ranges (jobs) are needed for this file
                                                    let job_count = chunk_count.div_ceil(target_range_size);
                                                    
                                                    // Add the required jobs to our demand
                                                    demand.fetch_add(job_count as usize, Ordering::SeqCst);
                                                    let _ = state.host_sender.send(HostMessage::RequestPermits(state.download.id())); 
                                                } else {
                                                    warn!("got 0 bytes: {}", file.name());
                                                    // 0 Byte file
                                                    file.set_status(DownloadStatus::Completed);
                                                }
                                            }
                                        },
                                        SizeResult::Stream => {
                                            debug!("Got no known metadata size for {}. Setting to stream.", file_id);
                                            if let Some(DownloadType::File(file)) = state.download.files_mut().get_mut(&file_id) {
                                                file.reset_retries();
                                                file.set_size(FileSize::Unknown);
                                                file.set_status(DownloadStatus::InProgress); 

                                                demand.fetch_add(1, Ordering::SeqCst);
                                                let _ = state.host_sender.send(HostMessage::RequestPermits(state.download.id())); 
                                            }
                                        },
                                        SizeResult::Retryable(_) => {
                                            file.increment_retries();
                                            if file.retries() > 5 { 
                                                warn!("Failed to get metadata size for {} after retrying.", file_id);
                                                file.set_status(DownloadStatus::Failed(DownloadFailureReason::MetadataFetchError));
                                                let _ = state.app_context.ui_sender.send(UiStateEvent::AddUpdate(DownloadUpdate::FileUpdated { id: download_id, file_update: FileUpdate::Status { id: file_id, status: file.status() } }));
                                            } else {
                                                warn!("Failed to get metadata size for {}. Retrying.", file_id);
                                                let retry_kind = RetryKind::Metadata(MetadataRetry { file_id, url });
                                                let _ = sender.send(SupervisorMessage::RetryAfter(permit, Duration::from_secs(5), retry_kind));
                                            }
                                        },
                                        SizeResult::PermanentFail => {
                                            warn!("Failed to get metadata size for {}.", file_id);
                                            file.set_status(DownloadStatus::Failed(DownloadFailureReason::MetadataFetchError));
                                            let _ = state.app_context.ui_sender.send(UiStateEvent::AddUpdate(DownloadUpdate::FileUpdated { id: download_id, file_update: FileUpdate::Status { id: file_id, status: file.status() } }));
                                        },
                                    }
                                },
                                SupervisorMessage::RetryAfter(permit, duration, retry_kind) => {
                                    let download_id = state.download.id();
                                    let file_id = retry_kind.file_id();

                                    let file = match state.download.files_mut().get_mut(&file_id).unwrap() {
                                        DownloadType::File(file) => file,
                                        DownloadType::Folder(_) => continue,
                                    };

                                    file.set_status(DownloadStatus::Retrying);

                                    let _ = state.app_context.ui_sender.send(UiStateEvent::AddUpdate(DownloadUpdate::FileUpdated { id: download_id, file_update: FileUpdate::Status { id: file_id, status: DownloadStatus::Retrying } }));

                                    drop(permit); 
                                    let sender = sender.clone();
                                    let cancel_token = cancel_token.clone();

                                    state.retry_queue_count += 1;
                                    tokio::spawn(async move {
                                        tokio::select! {
                                            _ = tokio::time::sleep(duration) => {
                                                let _ = sender.send(SupervisorMessage::RetryReady(retry_kind)); 
                                            }
                                            _ = cancel_token.cancelled() => {
                                                return;
                                            }
                                        }
                                    });
                                },
                                SupervisorMessage::RetryReady(retry_kind) => {
                                    state.retry_queue_count -= 1;
                                    demand.fetch_add(1, Ordering::SeqCst);

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
                                                .push(RangeRetryJob {
                                                    range: range_download.range,
                                                    url: range_download.url,
                                                    file_map: range_download.file_map,
                                                    expected_len: range_download.expected_len
                                                });
                                        },
                                    }

                                    let _ = state.host_sender.send(HostMessage::RequestPermits(state.download.id())); 
                                }
                                SupervisorMessage::NetworkError(permit, error, retry_kind) => {
                                    warn!("Network Error for {}: {}. Retrying...", retry_kind.file_id(), error);

                                    let _ = sender.send(SupervisorMessage::RetryAfter(permit, Duration::from_secs(5), retry_kind)); 
                                },
                                SupervisorMessage::ServerError(permit, status_code, retry_kind) => {
                                    warn!("Server error ({}). Retrying...", status_code);

                                    let download_id = state.download.id();
                                    let file_id = retry_kind.file_id();

                                    let file = match state.download.files_mut().get_mut(&file_id).unwrap() {
                                        DownloadType::File(file) => file,
                                        DownloadType::Folder(_) => continue,
                                    };

                                    file.increment_retries();
                                    if file.retries() > 5 { 
                                        file.set_status(DownloadStatus::Failed(DownloadFailureReason::ServerError));
                                        let _ = state.app_context.ui_sender.send(UiStateEvent::AddUpdate(DownloadUpdate::FileUpdated { id: download_id, file_update: FileUpdate::Status { id: file_id, status: file.status() } }));
                                    } else {
                                        let _ = sender.send(SupervisorMessage::RetryAfter(permit, Duration::from_secs(5), retry_kind));
                                    }
                                },
                                SupervisorMessage::ClientError(permit, status_code, retry_kind) => {
                                    error!("Client error ({}).", status_code);
                                    drop(permit); 

                                    let download_id = state.download.id();
                                    let file_id = retry_kind.file_id();

                                    let file = match state.download.files_mut().get_mut(&file_id).unwrap() {
                                        DownloadType::File(file) => file,
                                        DownloadType::Folder(_) => todo!(),
                                    };

                                    file.set_status(DownloadStatus::Failed(DownloadFailureReason::ClientError));

                                    let _ = state.app_context.ui_sender.send(UiStateEvent::AddUpdate(DownloadUpdate::FileUpdated { id: download_id, file_update: FileUpdate::Status { id: file_id, status: file.status() } }));
                                }
                                SupervisorMessage::RateLimited(_permit, retry_after, retry_kind) => {
                                    let file = match state.download.files_mut().get_mut(&retry_kind.file_id()).unwrap() {
                                        DownloadType::File(file) => file,
                                        DownloadType::Folder(_) => continue,
                                    };

                                    warn!("Rate limited for {}.", retry_kind.file_id());
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
                                            error!("IO error: {error}");
                                            file.set_status(DownloadStatus::Failed(DownloadFailureReason::DiskError));
                                            // fail
                                        }
                                        
                                        // Storage errors
                                        std::io::ErrorKind::WriteZero |
                                        std::io::ErrorKind::StorageFull |
                                        std::io::ErrorKind::QuotaExceeded |
                                        std::io::ErrorKind::FileTooLarge |
                                        std::io::ErrorKind::OutOfMemory => {
                                            error!("The system has ran out of storage: {error}");
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
                                            warn!("Temporary OS error: {error}. Retrying...");

                                            let _ = sender.send(SupervisorMessage::RetryAfter(permit, Duration::from_secs(5), retry_kind));
                                        },
                                        
                                        _ => {
                                            error!("OS error: {error}.");
                                            file.increment_retries();
                                            if file.retries() > 5 { 
                                                file.set_status(DownloadStatus::Failed(DownloadFailureReason::DiskError));
                                            } else {
                                                let _ = sender.send(SupervisorMessage::RetryAfter(permit, Duration::from_secs(5), retry_kind));
                                            }
                                        },
                                    }

                                    let _ = state.app_context.ui_sender.send(UiStateEvent::AddUpdate(DownloadUpdate::FileUpdated { id: download_id, file_update: FileUpdate::Status { id: file_id, status: file.status() } }));
                                },
                                SupervisorMessage::Pause => {
                                    info!("Pausing download.");

                                    // Set ui and db status to Paused
                                    state.download.set_status(DownloadStatus::Paused);
                                    let _ = state.app_context.ui_sender.send(UiStateEvent::AddUpdate(DownloadUpdate::StatusChanged { id: state.download.id(), status: DownloadStatus::Paused }));

                                    // Save the download to db
                                    state.app_context.db_manager.write_download(&state.download).await;

                                    // Break the loop to close this thread
                                    break; 
                                },
                            }
                        }
                    _ = save_interval.tick() => {
                        state.app_context.db_manager.write_download(&state.download).await;
                    }
                }
        
            }
            // saves to db here for persitence and in case oneshot fails
            state.app_context.db_manager.write_download(&state.download).await;

            let _ = shutdown_sender.send(state);
        });

        self.handle = Some(handle);
    }

    pub fn handle_mut(&mut self) -> Option<&mut JoinHandle<()>> {
        self.handle.as_mut()
    }
        
    pub fn give_permit(&mut self, permit: ActiveDownloadPermit) {
        if let Some(sender) = &self.sender && sender.is_closed() {
            self.sender = None;
        }

        if self.sender.is_none() {
            // If the message wasn't sent correctly it might mean the thread died and we are hibernating
            let (sender, receiver) = unbounded_channel();
            self.sender = Some(sender.clone());
            self.spawn(sender, receiver);
        }

        let _ = self.sender.as_ref().unwrap().send(SupervisorMessage::ProcessPermit(permit));
    }

    pub fn permit_count(&self) -> Arc<AtomicUsize> {
        self.permit_count.clone()
    }

    pub fn demand(&self) -> Arc<AtomicUsize> {
        self.demand.clone()
    }

    pub fn pause(&self) {
        if let Some(sender) = &self.sender {
            let _ = sender.send(SupervisorMessage::Pause);
        }
    }

    pub fn download_id(&self) -> DownloadId {
        self.download_id
    }

    fn calculate_initial_demand(download: &Download) -> usize {
        let mut demand = 0;

        let chunk_size = 16384; // 16 KB
        let target_range_size = 5242880 / chunk_size; // 320 chunks


        for file_type in download.files().values() {
            if let DownloadType::File(file) = file_type {
                // Skip fully downloaded files
                if file.status() == DownloadStatus::Completed {
                    continue; 
                }

                let chunks = file.chunks(); 

                if chunks.is_empty() {
                    // Uninitialized file: needs 1 permit for metadata/stream request
                    demand += 1;
                } else {
                    // Initialized file: count how many ranges have at least one chunk missing
                    let incomplete_jobs = chunks
                        .chunks(target_range_size)
                        .filter(|chunk_range| !chunk_range.all())
                        .count();
                    
                    demand += incomplete_jobs;
                }
            }
        }

        demand
    }

    fn is_download_finished(download: &Download) -> bool {
        download.files().values().all(|f| match f {
            DownloadType::File(file) => file.status() == DownloadStatus::Completed,
            _ => true,
        })
    }
}

impl Drop for DownloadSupervisor {
    fn drop(&mut self) {
        // Instantly kill every active network connection of this download
        self.cancel_token.cancel();
        info!("Dropping {}", self.download_id());
    }
}

async fn fetch_file_size(client: &reqwest::Client, url: &str) -> SizeResult {
    // Try a HEAD request first
    let head_result = client.head(url)
        .header("Accept-Encoding", "identity")
        .send()
        .await;

    if let Ok(response) = head_result
        && let Some(len) = response.content_length()
        && response.status().is_success() && len != 0
    {
        return SizeResult::Known(len);
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
                if let Some(range_header) = response.headers().get(header::CONTENT_RANGE)
                    && let Ok(str) = range_header.to_str()
                {
                    trace!("parse successfuly: {}", str);
                    if let Some(total_size) = parse_content_range(str) && total_size != 0 {
                        trace!("parsed correctly!");
                        return SizeResult::Known(total_size);
                    }

                    return SizeResult::Stream; 
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
    let end_byte = start_byte + expected_len - 1; // -1 because http ranges are inclusive

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

                if let Some(content_length) = response.content_length() 
                    && content_length != end_byte + 1
                {
                    return Err(RangeDownloadError::RangeNotSupported);
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