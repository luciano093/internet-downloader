use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::BytesMut;
use chrono::{DateTime, Utc};
use reqwest::{Client, StatusCode, header};
use thiserror::Error;

use tokio::fs::create_dir_all;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::oneshot;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn};

use crate::client_state_manager::UiStateEvent;
use crate::context::AppContext;
use crate::download::items::{ChangedItem, Download, DownloadItem, DownloadType};
use crate::download::status::{DownloadStatus, FileStatus, StateBucket};
use crate::download::{DownloadId, DownloadLimiterGroup, DownloadUpdate, FileFailureReason, FileSize, FileUpdate, FolderUpdate, ItemUpdate, ManagerCommand};
use crate::download_writer_manager::FileChunk;
use crate::host_manager::{ActiveDownloadPermit, HostMessage, ValidDownloadPermit};
use crate::shared_file_map::SharedFileMap;
use crate::utils::network_utils::{BandwidthLimiter, ThrottledStream};

const CHUNK_SIZE: usize = 16384; // 16 KB
const TARGET_RANGE_SIZE: usize = 5242880 / CHUNK_SIZE; // 320 ranges of chunks
const CHANNEL_UPDATE_THRESHOLD: u64 = 128 * 1024; // 128 KB

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
    #[error("There was an error writing to disk.")]
    DiskWriteError(#[from] std::io::Error),
    #[error("The disk pool was unexpectedly dropped.")]
    DiskPoolDropped,
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

// RAII guard in case a worker unexpectedly fails or dies
// Will automatically subtract the bytes it was downloading but never registerd
// from the total number of bytes downloaded for the file
struct RangeProgress {
    file_progress: Arc<AtomicU64>,
    local_bytes_downloaded: u64,
    completed: bool,
}

impl RangeProgress {
    fn new(file_progress: Arc<AtomicU64>) -> Self {
        Self { 
            file_progress,
            local_bytes_downloaded: 0,
            completed: false
        }
    }

    // Returns new value
    fn add(&mut self, bytes: u64) -> u64 {
        self.local_bytes_downloaded += bytes;
        let prev = self.file_progress.fetch_add(bytes, Ordering::Relaxed);

        prev + bytes 
    }

    fn complete(mut self) {
        self.completed = true;
    }
}

impl Drop for RangeProgress {
    fn drop(&mut self) {
        if !self.completed && self.local_bytes_downloaded > 0 {
            self.file_progress.fetch_sub(self.local_bytes_downloaded, Ordering::Relaxed);
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
    app_context: AppContext,
    retry_queue_count: usize, // tracks how many downloads we are trying. Useful for when there are no jobs and nothing in the retry queue due to retry timeout or delay
    file_progress: HashMap<usize, Arc<AtomicU64>>, // tracker for how many bytes we have downloaded for each file
    writer_sender: flume::Sender<FileChunk>, // direct channels to the tasks that manage file writing io
    shared_file_maps: HashMap<usize, Arc<SharedFileMap>>,
}

impl SupervisorState {
    fn new(app_context: AppContext, download: Download, host_sender: UnboundedSender<HostMessage>, permit_count: Arc<AtomicUsize>) -> Self {
        let mut file_progress = HashMap::new();
        for (id, item) in download.files() {
            if let DownloadType::File(file) = item {
                let initial_bytes = file.calculate_initial_bytes(CHUNK_SIZE as u64);
                
                file_progress.insert(*id, Arc::new(AtomicU64::new(initial_bytes)));
            }
        }

        let writer_sender = app_context.writer_handle.sender();

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
            app_context,
            retry_queue_count: 0,
            file_progress,
            writer_sender,
            shared_file_maps: HashMap::new(),
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
                DownloadType::File(file) if file.status() != FileStatus::Completed => file,
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
                
                let max_end = (start_index + TARGET_RANGE_SIZE).min(chunks.len());

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

                let expected_len = self.calculate_chunk_expected_len(CHUNK_SIZE as u64, range, file_size);

                let path = file_download.relative_path();

                if !self.shared_file_maps.contains_key(&file_id) {
                    if let Some(parent_path) = path.parent() {
                        create_dir_all(parent_path).await.unwrap();
                    }

                    let file_map = self.app_context.writer_handle.create_file(path.clone(), file_size).await.unwrap();
                    
                    self.shared_file_maps.insert(file_id, file_map);
                }

                let file_map = self.shared_file_maps.get(&file_id).unwrap().clone();

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

pub struct DownloadSupervisor {
    state: Option<SupervisorState>,
    sender: Option<UnboundedSender<SupervisorMessage>>,
    shutdown_receiver: Option<oneshot::Receiver<SupervisorState>>,
    demand: Arc<AtomicUsize>,
    permit_count: Arc<AtomicUsize>,
    handle: Option<JoinHandle<()>>,
    download_id: DownloadId,
    cancel_token: CancellationToken, // Used for killing workers forcefully if needed
    global_limit: Arc<BandwidthLimiter>,
    host_limit: Arc<BandwidthLimiter>,
    download_limits: Arc<DownloadLimiterGroup>,
}

impl DownloadSupervisor {
    pub async fn new(app_context: AppContext, download: Download, host_sender: UnboundedSender<HostMessage>, global_limit: Arc<BandwidthLimiter>, host_limit: Arc<BandwidthLimiter>, download_limits: Arc<DownloadLimiterGroup>) -> Self {
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
            global_limit,
            host_limit,
            download_limits,
        }
    }

    fn spawn(&mut self, sender: UnboundedSender<SupervisorMessage>, mut receiver: UnboundedReceiver<SupervisorMessage>) {
        let mut state = self.state.take();
        let mut previous_shutdown_receiver = self.shutdown_receiver.take();

        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        self.shutdown_receiver = Some(shutdown_receiver);
        let demand = self.demand.clone();
        let cancel_token = self.cancel_token.clone();

        let global_limit = self.global_limit.clone();
        let host_limit = self.host_limit.clone();
        let download_limit = self.download_limits.download_limiter();
        let download_group = self.download_limits.clone();

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
                Self::finish_download(&mut state).await;
                return;
            }

            let mut save_interval = tokio::time::interval(Duration::from_millis(100));

            let download_status = match state.download.files().get(&0) {
                Some(DownloadType::File(file)) => file.status().bucket(),
                Some(DownloadType::Folder(folder)) => folder.status().bucket(),
                None => StateBucket::Completed, // Download has nothing inside, so we must be completed
            };

            match download_status {
                StateBucket::InProgress | 
                StateBucket::FetchingMetadata | 
                StateBucket::Initializing | 
                StateBucket::Retrying | 
                StateBucket::Queued |
                StateBucket::Waiting => { },

                StateBucket::Paused |
                StateBucket::Error |
                StateBucket::CompletedWithErrors |
                StateBucket::Completed => {
                    warn!("A supervisor for download '{}' was spawned, but its status is {:?}. Halting.", state.download.name(), download_status);

                    // We set demand to 0 just in case to prevent host manager sending us any more permits
                    demand.store(0, Ordering::SeqCst);
                    let _ = state.host_sender.send(HostMessage::DownloadHalted(state.download.id()));

                    if download_status == StateBucket::Completed
                        || download_status == StateBucket::CompletedWithErrors
                    {
                        Self::finish_download(&mut state).await;
                    }

                    return;
                }
            }
            
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
                                                    Self::finish_download(&mut state).await;
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

                                            if let Some(changed_items) = state.download.set_file_status(file_id, FileStatus::FetchingMetadata) {
                                                Self::process_status_changes(&mut state, changed_items).await;
                                            }

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

                                            let file_limit = download_group.file_limiters()
                                                .get(&file_id)
                                                .map(|limiter| limiter.clone())
                                                .unwrap_or_else(|| {
                                                    let limiter = BandwidthLimiter::new(0);
                                                    limiter.set_unlimited(true);

                                                    Arc::new(limiter)
                                                });

                                            let limiters = vec![global_limit.clone(), host_limit.clone(), download_limit.clone(), file_limit];
                                            let ui_sender = state.app_context.ui_sender.clone();
                                            let download_id = state.download.id();

                                            let file_progress = state.file_progress.get(&file_id).unwrap().clone();
                                            let io_sender = state.writer_sender.clone();
          
                                            tokio::spawn(async move {
                                                tokio::select! {
                                                    _ = cancel_token.cancelled() => {
                                                        return;
                                                    }
                                                    // Do worker stuff
                                                    result = download_range(client, &url, range, io_sender, file_map.clone(), expected_len, limiters, ui_sender, download_id, file_id, file_progress) => {
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

                                            let file_limit = download_group.file_limiters()
                                                .get(&file_id)
                                                .map(|limiter| limiter.clone())
                                                .unwrap_or_else(|| {
                                                    let limiter = BandwidthLimiter::new(0);
                                                    limiter.set_unlimited(true);

                                                    Arc::new(limiter)
                                                });

                                            let limiters = vec![global_limit.clone(), host_limit.clone(), download_limit.clone(), file_limit];
                                            
                                            tokio::spawn(async move {
                                                tokio::select! {
                                                    _ = cancel_token.cancelled() => {
                                                        return;
                                                    }
                                                    // Do worker stuff
                                                    result = download_stream(client, &path, &url, limiters) => {
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

                                    // Store new progress and resize chunks
                                    if let Some(file) = state.download.get_file_mut(&file_id) {
                                        file.reset_retries();
                                        file.set_size(FileSize::Known(size as u64));

                                        // The file is complete, so we can just store the size in progress
                                        if let Some(progress) = state.file_progress.get(&file_id) {
                                            progress.store(size as u64, Ordering::Relaxed);
                                        }

                                        if size > 0 {
                                            let chunk_count = size.div_ceil(CHUNK_SIZE);
                                            file.chunks_mut().resize(chunk_count, true);
                                            trace!("got chunk size completed: {}/{}", file.chunks_mut().count_ones(), file.chunks().len());
                                        } else {
                                            // 0 Byte file
                                            trace!("got 0 bytes: {}", file.name());
                                        }

                                        let _ = state.app_context.ui_sender.send(UiStateEvent::AddUpdate(DownloadUpdate::ItemUpdated { id: download_id, item_update: ItemUpdate::File(FileUpdate::FileSize { id: file_id, len: size as u64 }) }));
                                    }

                                    // Update new statuses
                                    if let Some(changed_items) = state.download.set_file_status(file_id, FileStatus::Completed) {
                                        Self::process_status_changes(&mut state, changed_items).await;
                                    }

                                    if Self::is_download_finished(&state.download) {
                                        debug!("All files completed for download {}. Exiting supervisor loop.", download_id);
                                        Self::finish_download(&mut state).await;
                                        break;
                                    }
                                },
                                SupervisorMessage::RangeSuccess(permit, file_id, range) => {
                                    state.active_downloads -= 1;
                                    drop(permit); 

                                    let download_id = state.download.id();
                                    let mut all_chunks_done = false;
                                    
                                    if let Some(file) = state.download.get_file_mut(&file_id) {
                                        file.reset_retries();

                                        file.chunks_mut()[range.0..range.1].fill(true);

                                        all_chunks_done = file.chunks().all();

                                        if all_chunks_done {
                                            let bytes_downloaded = state.file_progress
                                            .get(&file_id)
                                            .map(|p| p.load(Ordering::Relaxed))
                                            .unwrap_or(0);

                                            trace!("file {} finished! got {} bytes", file.name(), bytes_downloaded);
                                        }
                                    }

                                    if all_chunks_done {
                                        if let Some(changed_items) = state.download.set_file_status(file_id, FileStatus::Completed) {
                                            Self::process_status_changes(&mut state, changed_items).await;
                                        }
                                        
                                        state.shared_file_maps.remove(&file_id);

                                        if Self::is_download_finished(&state.download) {
                                            debug!("All files completed for download {}. Exiting supervisor loop.", download_id);
                                            Self::finish_download(&mut state).await;
                                            break;
                                        }
                                    }
                                },
                                SupervisorMessage::RangeFailed(permit, file_id, range, url, file_map, expected_len, download_error) => {
                                    let retry_kind = RetryKind::RangeDownload(RangeDownload { file_id, range, url: url.clone(), file_map, expected_len });
                                    let file_id = retry_kind.file_id();
                                    let download_name = state.download.name().clone();

                                    if let Some(file) = state.download.get_file_mut(&file_id) {
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
                                                    state.shared_file_maps.remove(&file.id());
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
                                                    state.shared_file_maps.remove(&file.id());
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
                                                state.shared_file_maps.remove(&file.id());
                                                state.retry_streams.push((file_id, url, file.relative_path().to_owned()));
                                                demand.fetch_add(1, Ordering::SeqCst); 
                                            },
                                            RangeDownloadError::DiskWriteError(error) => {
                                                let _ = sender.send(SupervisorMessage::IoError(permit, error, retry_kind));
                                            },
                                            RangeDownloadError::DiskPoolDropped => {
                                                error!("App-wide disk pool dropped. App entered an invalid state and should restart. This probably happened due to an OS error or logic bug.");

                                                let _ = state.app_context.download_manager.send(ManagerCommand::Shutdown);

                                                break;
                                            },
                                        }
                                    }
                                }
                                SupervisorMessage::MetadataFetched(permit, file_id, url, size_result) => {
                                    trace!("got metadata for: {} {}", state.download.name(), file_id);

                                    let download_id = state.download.id();
                                    let mut target_status = None;
                                    let mut new_jobs = 0;

                                    if let Some(file) = state.download.get_file_mut(&file_id) {
                                        match size_result {
                                            SizeResult::Known(size) => {
                                                trace!("Got known metadata size for {}. Got {}", file_id, size);
                                                file.reset_retries();
                                                file.set_size(FileSize::Known(size));

                                                let _ = state.app_context.ui_sender.send(UiStateEvent::AddUpdate(DownloadUpdate::ItemUpdated { id: download_id, item_update: ItemUpdate::File(FileUpdate::FileSize { id: file_id, len: size }) }));

                                                // Initialize chunks
                                                if size > 0 {
                                                    // Calculate how many 16KB chunks exist
                                                    let chunk_count = size.div_ceil(CHUNK_SIZE as u64);
                                                    file.chunks_mut().resize(chunk_count as usize, false);

                                                    // Calculate how many ranges (jobs) are needed for this file
                                                    // and add the required jobs to our demand
                                                    new_jobs = chunk_count.div_ceil(TARGET_RANGE_SIZE as u64) as usize;
    
                                                    target_status = Some(FileStatus::InProgress);
                                                } else {
                                                    warn!("got 0 bytes: {}", file.name());
                                                    // 0 Byte file
                                                    target_status = Some(FileStatus::Completed);
                                                }
                                            },
                                            SizeResult::Stream => {
                                                debug!("Got no known metadata size for {}. Setting to stream.", file_id);
                                                file.reset_retries();
                                                file.set_size(FileSize::Unknown);
                                                new_jobs = 1;
                                                
                                                target_status = Some(FileStatus::InProgress);
                                            },
                                            SizeResult::Retryable(_) => {
                                                file.increment_retries();
                                                if file.retries() > 5 { 
                                                    warn!("Failed to get metadata size for {} after retrying.", file_id);
                                                    target_status = Some(FileStatus::Failed(FileFailureReason::MetadataFetchError));
                                                } else {
                                                    warn!("Failed to get metadata size for {}. Retrying.", file_id);
                                                    let retry_kind = RetryKind::Metadata(MetadataRetry { file_id, url });
                                                    let _ = sender.send(SupervisorMessage::RetryAfter(permit, Duration::from_secs(5), retry_kind));
                                                }
                                            },
                                            SizeResult::PermanentFail => {
                                                warn!("Failed to get metadata size for {}.", file_id);
                                                target_status = Some(FileStatus::Failed(FileFailureReason::MetadataFetchError));
                                            },
                                        }
                                    }

                                    if let Some(status) = target_status {
                                        if let Some(changed_items) = state.download.set_file_status(file_id, status) {
                                            Self::process_status_changes(&mut state, changed_items).await;
                                        }
                                    }

                                    if new_jobs > 0 {
                                        demand.fetch_add(new_jobs, Ordering::SeqCst);
                                        let _ = state.host_sender.send(HostMessage::RequestPermits(download_id)); 
                                    }
                                },
                                SupervisorMessage::RetryAfter(permit, duration, retry_kind) => {
                                    let download_id = state.download.id();
                                    let file_id = retry_kind.file_id();

                                    if let Some(changed_items) = state.download.set_file_status(file_id, FileStatus::Retrying) {
                                        Self::process_status_changes(&mut state, changed_items).await;
                                    }

                                    let _ = state.app_context.ui_sender.send(UiStateEvent::AddUpdate(DownloadUpdate::ItemUpdated { id: download_id, item_update: ItemUpdate::File(FileUpdate::Status { id: file_id, status: FileStatus::Retrying }) }));

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

                                    let file_id = retry_kind.file_id();
                                    let mut status_change = None;

                                    if let Some(file) = state.download.get_file_mut(&file_id) {
                                        file.increment_retries();

                                        if file.retries() > 5 { 
                                            status_change = Some(FileStatus::Failed(FileFailureReason::ServerError));
                                        } else {
                                            let _ = sender.send(SupervisorMessage::RetryAfter(permit, Duration::from_secs(5), retry_kind));
                                        }
                                    }

                                    if let Some(status_change) = status_change
                                        && let Some(changed_items) = state.download.set_file_status(file_id, status_change)
                                    {
                                        Self::process_status_changes(&mut state, changed_items).await;
                                    }
                                },
                                SupervisorMessage::ClientError(permit, status_code, retry_kind) => {
                                    error!("Client error ({}).", status_code);
                                    drop(permit); 

                                    let file_id = retry_kind.file_id();

                                    if let Some(changed_items) = state.download.set_file_status(file_id, FileStatus::Failed(FileFailureReason::ClientError)) {
                                        Self::process_status_changes(&mut state, changed_items).await;
                                    }
                                }
                                SupervisorMessage::RateLimited(_permit, retry_after, retry_kind) => {
                                    let file_id = retry_kind.file_id();

                                    warn!("Rate limited for {}.", retry_kind.file_id());

                                    if let Some(changed_items) = state.download.set_file_status(file_id, FileStatus::Waiting(retry_after)) {
                                        Self::process_status_changes(&mut state, changed_items).await;
                                    }
                                    
                                    state.retry_queue_count += 1;
                                    let _ = sender.send(SupervisorMessage::RetryReady(retry_kind));
                                    let _ = state.host_sender.send(HostMessage::RateLimited(retry_after));
                                },
                                SupervisorMessage::IoError(permit, error, retry_kind) => {
                                    let file_id = retry_kind.file_id();
                                    let mut status_change = None;

                                    if let Some(file) = state.download.get_file_mut(&retry_kind.file_id()) {
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
                                                status_change = Some(FileStatus::Failed(FileFailureReason::DiskError));
                                                // fail
                                            }
                                            
                                            // Storage errors
                                            std::io::ErrorKind::WriteZero |
                                            std::io::ErrorKind::StorageFull |
                                            std::io::ErrorKind::QuotaExceeded |
                                            std::io::ErrorKind::FileTooLarge |
                                            std::io::ErrorKind::OutOfMemory => {
                                                error!("The system has ran out of storage: {error}");
                                                status_change = Some(FileStatus::Failed(FileFailureReason::DiskError));
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
                                                    status_change = Some(FileStatus::Failed(FileFailureReason::DiskError));
                                                } else {
                                                    let _ = sender.send(SupervisorMessage::RetryAfter(permit, Duration::from_secs(5), retry_kind));
                                                }
                                            },
                                        }
                                    }

                                    if let Some(status_change) = status_change
                                        && let Some(changed_items) = state.download.set_file_status(file_id, status_change)
                                    {
                                        Self::process_status_changes(&mut state, changed_items).await;
                                    }
                                },
                                SupervisorMessage::Pause => {
                                    info!("Pausing download.");

                                    let download_id = state.download.id();
                                    let changed_items = state.download.set_paused();

                                    // Set ui status to Paused
                                    let _ = state.app_context.ui_sender.send(UiStateEvent::AddUpdate(DownloadUpdate::StatusChanged { id: state.download.id(), status: DownloadStatus::Paused }));
                                    
                                    for item in changed_items {
                                        let update = match item {
                                            ChangedItem::File { id, status } => {
                                                DownloadUpdate::ItemUpdated {
                                                    id: download_id,
                                                    item_update: ItemUpdate::File(
                                                        FileUpdate::Status { 
                                                            id: id, 
                                                            status, 
                                                        }
                                                    )
                                                }
                                            },
                                            ChangedItem::Folder { id, status } => {
                                                DownloadUpdate::ItemUpdated {
                                                    id: download_id,
                                                    item_update: ItemUpdate::Folder(
                                                        FolderUpdate::Status { 
                                                            id: id, 
                                                            status, 
                                                        }
                                                    )
                                                }
                                            },
                                            ChangedItem::Download(status) => {
                                                DownloadUpdate::StatusChanged { 
                                                    id: download_id, 
                                                    status 
                                                }
                                            },
                                        };

                                        let _ = state.app_context.ui_sender.send(UiStateEvent::AddUpdate(
                                            update
                                        ));
                                    }

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

        for file_type in download.files().values() {
            if let DownloadType::File(file) = file_type {
                // Skip fully downloaded files
                if file.status() == FileStatus::Completed {
                    continue; 
                }

                let chunks = file.chunks(); 

                if chunks.is_empty() {
                    // Uninitialized file: needs 1 permit for metadata/stream request
                    demand += 1;
                } else {
                    // Initialized file: count how many ranges have at least one chunk missing
                    let incomplete_jobs = chunks
                        .chunks(TARGET_RANGE_SIZE)
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
            DownloadType::File(file) => file.status() == FileStatus::Completed,
            _ => true,
        })
    }

    async fn finish_download(state: &mut SupervisorState) {
        let download = &mut state.download;

        info!("Supervisor finishing download '{}' with final status: {:?}", download.name(), download.status());

        state.app_context.db_manager.write_download(download).await;

        let _ = state.host_sender.send(HostMessage::DownloadFinished(state.download.id()));
    }

    async fn process_status_changes(state: &mut SupervisorState, changed_items: Vec<ChangedItem>) {
        if changed_items.is_empty() {
            return;
        }

        // Save the new statuses to db
        state.app_context.db_manager.write_download(&state.download).await;

        let download_id = state.download.id();

        // Send every change to db
        for item in changed_items {
            match item {
                ChangedItem::File { id, status } => {
                    let _ = state.app_context.ui_sender.send(UiStateEvent::AddUpdate(
                        DownloadUpdate::ItemUpdated { 
                            id: download_id, 
                            item_update: ItemUpdate::File(FileUpdate::Status { id, status }) 
                        }
                    ));
                },
                ChangedItem::Folder { id, status } => {
                    let _ = state.app_context.ui_sender.send(UiStateEvent::AddUpdate(
                        DownloadUpdate::ItemUpdated { 
                            id: download_id,
                            item_update: ItemUpdate::Folder(FolderUpdate::Status { id, status, }) 
                        }
                    ));
                }
                ChangedItem::Download(status) => {
                    let _ = state.app_context.ui_sender.send(UiStateEvent::AddUpdate(
                        DownloadUpdate::StatusChanged { 
                            id: download_id,
                            status,
                        }
                    ));
                },
            }
        }
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

async fn download_range(
    client: Client, 
    url: &str,
    range: (usize, usize),
    io_sender: flume::Sender<FileChunk>,
    file_map: Arc<SharedFileMap>,
    expected_len: u64,
    limiters: Vec<Arc<BandwidthLimiter>>,
    ui_sender: UnboundedSender<UiStateEvent>,
    download_id: DownloadId,
    file_id: usize,
    file_progress: Arc<AtomicU64>, 
)-> Result<(), RangeDownloadError> {
    let start_byte = range.0 as u64 * (CHUNK_SIZE as u64);
    let end_byte = start_byte + expected_len.saturating_sub(1); // -1 because http ranges are inclusive

    let range_header = format!("bytes={}-{}", start_byte, end_byte);

    let request = client.get(url)
        .header("Range", range_header);

    let response = match request.send().await {
        Ok(response) => match response.status() {
            StatusCode::TOO_MANY_REQUESTS => {
                let retry_after = parse_retry_after(response.headers());

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

    let raw_stream = response.bytes_stream();
    let throttled_stream = ThrottledStream::new(raw_stream, limiters);
    tokio::pin!(throttled_stream);

    let mut current_offset = start_byte;
    let mut bytes_received = 0; 

    let mut range_progress = RangeProgress::new(file_progress);
    let mut unnotified_bytes = 0; 
    let mut current_progress = 0;

    let buffer_capacity: usize = 1024 * 1024; // 1 MB
    let mut buffer = BytesMut::with_capacity(buffer_capacity);
    let mut buffer_start_offset = current_offset;

    let mut in_flight_acks: VecDeque<(u64, oneshot::Receiver<std::io::Result<()>>)> = VecDeque::new();
    const MAX_IN_FLIGHT: usize = 4; // Max 4MB in RAM per worker before we apply backpressure

    while let Some(chunk) = throttled_stream.next().await {
        let chunk = chunk.map_err(DownloadError::from)?; 
        let chunk_len = chunk.len() as u64;

        buffer.extend_from_slice(&chunk);

        while let Some((_, receiver)) = in_flight_acks.front_mut() {
            match receiver.try_recv() {
                Ok(Ok(_)) => {
                    let (bytes_written, _) = in_flight_acks.pop_front().unwrap();
                    current_progress = range_progress.add(bytes_written);
                    unnotified_bytes += bytes_written;

                    if unnotified_bytes >= CHANNEL_UPDATE_THRESHOLD {
                        let _ = ui_sender.send(UiStateEvent::AddUpdate(
                            DownloadUpdate::ItemUpdated { 
                                id: download_id,
                                item_update: ItemUpdate::File(
                                    FileUpdate::BytesDownloaded { 
                                        id: file_id,
                                        len: current_progress, 
                                    }
                                ) 
                            }
                        ));
                        unnotified_bytes = 0; 
                    }
                }
                Ok(Err(error)) => return Err(RangeDownloadError::DiskWriteError(error)), // Disk failed
                Err(oneshot::error::TryRecvError::Empty) => break,
                Err(oneshot::error::TryRecvError::Closed) => return Err(RangeDownloadError::DiskPoolDropped),
            }
        }

        current_offset += chunk_len;
        bytes_received += chunk_len;

        if buffer.len() >= buffer_capacity {
            // Swap full buffer for an empty one
            let buffer_to_write = buffer.split().freeze();
            let bytes_to_write = buffer_to_write.len() as u64;

            buffer.reserve(buffer_capacity); 

            let (ack_sender, ack_receiver) = oneshot::channel();

            let file_chunk = FileChunk {
                file_map: file_map.clone(),
                offset: buffer_start_offset,
                data: buffer_to_write,
                ack: ack_sender, 
            };

            io_sender.send_async(file_chunk).await.map_err(|_| RangeDownloadError::DiskPoolDropped)?;

            in_flight_acks.push_back((bytes_to_write, ack_receiver));
            buffer_start_offset = current_offset; 


            if in_flight_acks.len() >= MAX_IN_FLIGHT {
                let (bytes_written, receiver) = in_flight_acks.pop_front().unwrap();

                receiver.await
                    .map_err(|_| RangeDownloadError::DiskPoolDropped)? 
                    .map_err(RangeDownloadError::DiskWriteError)?; 

                current_progress = range_progress.add(bytes_written);
                unnotified_bytes += bytes_written;

                if unnotified_bytes >= CHANNEL_UPDATE_THRESHOLD {
                    let _ = ui_sender.send(UiStateEvent::AddUpdate(
                        DownloadUpdate::ItemUpdated { 
                            id: download_id,
                            item_update: ItemUpdate::File(
                                FileUpdate::BytesDownloaded { 
                                    id: file_id,
                                    len: current_progress, 
                                }
                            ) 
                        }
                    ));
                    unnotified_bytes = 0; 
                }
            }
                
        }      
    }

    if !buffer.is_empty() {
        let final_bytes_len = buffer.len() as u64;
        let (ack_sender, ack_receiver) = oneshot::channel();

        let file_chunk = FileChunk {
            file_map: file_map.clone(),
            offset: buffer_start_offset,
            data: buffer.split().freeze(),
            ack: ack_sender,
        };

        io_sender.send_async(file_chunk).await
            .map_err(|_| RangeDownloadError::DiskPoolDropped)?;

        in_flight_acks.push_back((final_bytes_len, ack_receiver));
    }

    while let Some((bytes_written, rx)) = in_flight_acks.pop_front() {
        rx.await
            .map_err(|_| RangeDownloadError::DiskPoolDropped)?
            .map_err(RangeDownloadError::DiskWriteError)?;

        current_progress = range_progress.add(bytes_written);
        unnotified_bytes += bytes_written;
    }

    // Update UI if any unnotified bytes remain
    if unnotified_bytes > 0 {
        let _ = ui_sender.send(UiStateEvent::AddUpdate(
            DownloadUpdate::ItemUpdated { 
                id: download_id,
                item_update: ItemUpdate::File(
                    FileUpdate::BytesDownloaded { 
                    id: file_id,
                    len: current_progress,
                })
            }
        ));
    }

    if bytes_received != expected_len {
        return Err(RangeDownloadError::UnexpectedLength(bytes_received, expected_len));
    }

    range_progress.complete();

    Ok(())
}

/// Downloads a file from a server that requested `Transfer-Encoding: chunked`. 
/// The server doesn't provide a `Content-Length` header for these files and thus they can't be downloaded using a multi-part strategy.
/// These downloads are non-resumable.
async fn download_stream(client: Client, path: &Path, url: &str, limiters: Vec<Arc<BandwidthLimiter>>) -> Result<usize, DownloadError> {
    let response = match client.get(url).send().await {
        Ok(response) => match response.status() {
            StatusCode::TOO_MANY_REQUESTS => {
                let retry_after = parse_retry_after(response.headers());

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

    let raw_stream = response.bytes_stream();
    let throttled_stream = ThrottledStream::new(raw_stream, limiters);
    tokio::pin!(throttled_stream);

    while let Some(chunk) = throttled_stream.next().await {
        let chunk = chunk?;

        size += chunk.len();
        writer.write_all(&chunk).await?;
    }

    writer.flush().await?;

    Ok(size)
}

fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<u64> {
    headers.get(header::RETRY_AFTER).and_then(|header| {
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
    })
}