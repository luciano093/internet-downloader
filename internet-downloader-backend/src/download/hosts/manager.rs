use std::collections::VecDeque;
use std::fmt;
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::BytesMut;
use chrono::{DateTime, Utc};
use futures_util::stream::FuturesUnordered;
use http::{StatusCode, header};
use rand::RngExt;
use rand::rng;
use reqwest::{Client, Response};
use thiserror::Error;
use tokio::fs::create_dir_all;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::Notify;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::time::Instant;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::trace;
use tracing::{debug, warn, error};
use url::Host;

use crate::app::manager::AppManagerCommand;
use crate::client_state_manager::{FileUpdate, UiManagerHandle};
use crate::download::hosts::manager::HostMessage::RetryReady;
use crate::download::items::{ActiveOperation, DownloadId, FileId, FileSize};
use crate::download::supervisor::{BLOCK_SIZE, HASH_CHUNK_SIZE};
use crate::download::writer::{DownloadWriterManager, FileChunk};
use crate::utils::network_utils::{BandwidthLimiter, ThrottledStream};
use crate::utils::shared_file_map::SharedFileMap;

const CHANNEL_UPDATE_THRESHOLD: u64 = 128 * 1024; // 128 KB
const MAX_RETRIES: usize = 5;

// Internal errors
#[derive(Debug, Error)]
enum MetadataError {
    #[error("HTTP {0}")]
    HttpStatus(StatusCode),
    #[error("Network error: {0}")]
    Network(#[from] reqwest::Error),
}

#[derive(Debug, Error)]
enum DownloadError {
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
    ServerError(StatusCode), // Error status 500-599
    #[error("Client error ({0})")]
    ClientError(StatusCode), // Error status 400-499
}

#[derive(Debug, Error)]
enum RangeDownloadError {
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

/// A permanent failure during a download operation.
/// This is sent to the supervisor, it will not be retried.
#[derive(Debug, Error)]
#[error("{operation}: {kind}")]
pub struct PermanentFailure {
    pub operation: DownloadOperation,
    pub kind: PermanentFailureKind,
    #[source]
    pub source: Box<dyn std::error::Error + Send + Sync + 'static>,
}

/// What was being attempted when the permanent failure occurred.
#[derive(Debug, Clone)]
pub enum DownloadOperation {
    /// Fetching metadata for a file
    FetchingMetadata { download_id: DownloadId, file_id: FileId },
    /// Streaming a full file
    DownloadingStream { download_id: DownloadId, file_id: FileId },
    /// Downloading a byte range
    DownloadingRange { download_id: DownloadId, file_id: FileId, range: Range<usize> },
}

impl DownloadOperation {
    pub fn file_id(&self) -> FileId {
        match self {
            DownloadOperation::FetchingMetadata { file_id, .. } => *file_id,
            DownloadOperation::DownloadingStream { file_id, .. } => *file_id,
            DownloadOperation::DownloadingRange { file_id, .. } => *file_id,
        }
    }
}

impl fmt::Display for DownloadOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FetchingMetadata { download_id, file_id } => {
                write!(f, "Failed to fetch metadata for file {file_id} of download {download_id}")
            }
            Self::DownloadingStream { download_id, file_id } => {
                write!(f, "Failed to download file {file_id} of download {download_id}")
            }
            Self::DownloadingRange { download_id, file_id, range } => {
                write!(f, "Failed to download range {}-{} for file {file_id} of download {download_id}", range.start, range.end)
            }
        }
    }
}

impl std::error::Error for DownloadOperation {}

#[derive(Debug, Clone, Error)]
pub enum PermanentFailureKind {
    #[error("Disk error")]
    Disk(io::ErrorKind),
    #[error(transparent)]
    Http(HttpFailureKind),
    #[error("Failed after {attempts} retries")]
    TooManyRetries { attempts: usize },
}

impl From<&io::Error> for PermanentFailureKind {
    fn from(value: &io::Error) -> Self {
       PermanentFailureKind::Disk(value.kind())
    }
}

#[derive(Debug, Clone, Error)]
pub enum HttpFailureKind {
    #[error("File not found (404)")]
    FileNotFound,
    #[error("Metadata fetch failed with status {status}")]
    MetadataError { status: u16 },
    #[error("Unexpected HTTP status {status} during download")]
    UnexpectedStatus { status: u16 },
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

enum FailedJob {
    Metadata(MetadataJob),
    Stream(StreamJob),
    Range(RangeJob),
}

impl FailedJob {
    fn download_id(&self) -> DownloadId {
        match self {
            FailedJob::Metadata(metadata_job) => metadata_job.download_id,
            FailedJob::Stream(stream_job) => stream_job.download_id,
            FailedJob::Range(range_job) => range_job.download_id,
        }
    }
    
    fn file_id(&self) -> FileId {
        match self {
            FailedJob::Metadata(metadata_job) => metadata_job.file_id,
            FailedJob::Stream(stream_job) => stream_job.file_id,
            FailedJob::Range(range_job) => range_job.file_id,
        }
    }
    
    fn result(&self) -> &mpsc::Sender<DownloadResult> {
        match self {
            FailedJob::Metadata(metadata_job) => &metadata_job.result,
            FailedJob::Stream(stream_job) => &stream_job.result,
            FailedJob::Range(range_job) => &range_job.result,
        }
    }
    
    fn active_operations_sender(&self) -> &UnboundedSender<(FileId, ActiveOperation)> {
        match self {
            FailedJob::Metadata(metadata_job) => &metadata_job.active_operations_sender,
            FailedJob::Stream(stream_job) => &stream_job.active_operations_sender,
            FailedJob::Range(range_job) => &range_job.active_operations_sender,
        }
    }
    
    /// Convert this failed job into a permanent failure report,
    /// given the kind of failure that occurred.
    fn into_permanent_failure(&self, kind: PermanentFailureKind, source: Box<dyn std::error::Error + Send + Sync + 'static>) -> PermanentFailure {
        let operation = match &self {
            FailedJob::Metadata(job) => DownloadOperation::FetchingMetadata {
                download_id: job.download_id,
                file_id: job.file_id,
            },
            FailedJob::Stream(job) => DownloadOperation::DownloadingStream {
                download_id: job.download_id,
                file_id: job.file_id,
            },
            FailedJob::Range(job) => DownloadOperation::DownloadingRange {
                download_id: job.download_id,
                file_id: job.file_id,
                range: job.range.clone(),
            },
        };
        
        PermanentFailure { operation, kind, source }
    }

    fn retries(&self) -> usize {
        match self {
            FailedJob::Metadata(job) => job.retries,
            FailedJob::Stream(job) => job.retries,
            FailedJob::Range(job) => job.retries,
        }
    }
        
    fn increment_retries(&mut self) {
        match self {
            FailedJob::Metadata(job) => job.retries += 1,
            FailedJob::Stream(job) => job.retries += 1,
            FailedJob::Range(job) => job.retries += 1,
        }
    }

    fn cancel_token(&self) -> &CancellationToken {
        match self {
            FailedJob::Metadata(metadata_job) => &metadata_job.cancel_token,
            FailedJob::Stream(stream_job) => &stream_job.cancel_token,
            FailedJob::Range(range_job) => &range_job.cancel_token,
        }
    }
}

pub struct MetadataJob {
    pub download_id: DownloadId,
    pub file_id: FileId,
    pub url: Arc<String>,
    pub result: mpsc::Sender<DownloadResult>,
    pub cancel_token: CancellationToken,
    pub active_operations_sender: UnboundedSender<(FileId, ActiveOperation)>,
    pub retries: usize,
}

pub struct StreamJob {
    pub download_id: DownloadId,
    pub file_id: FileId,
    pub url: Arc<String>,
    pub path: PathBuf,
    pub download_limiter: Arc<BandwidthLimiter>,
    pub file_limiter: Arc<BandwidthLimiter>,
    pub result: mpsc::Sender<DownloadResult>,
    pub cancel_token: CancellationToken,
    pub active_operations_sender: UnboundedSender<(FileId, ActiveOperation)>,
    pub retries: usize,
}

impl TryFrom<RangeJob> for StreamJob {
    type Error = ();
    
    fn try_from(range_job: RangeJob) -> Result<Self, Self::Error> {
        let won = range_job.is_converting_to_stream.compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed).is_ok();

        if !won {
            return Err(());
        }
        
        let path = range_job.file_map.path().to_path_buf();
        
        Ok(Self {
            download_id: range_job.download_id,
            file_id: range_job.file_id,
            url: range_job.url,
            path,
            download_limiter: range_job.download_limiter,
            file_limiter: range_job.file_limiter,
            result: range_job.result,
            cancel_token: range_job.cancel_token,
            active_operations_sender: range_job.active_operations_sender,
            retries: 0, // we reset retries
        })
    }
}

pub struct RangeJob {
    pub download_id: DownloadId,
    pub file_id: FileId,
    pub url: Arc<String>,
    pub range: Range<usize>,
    pub expected_bytes: u64,
    pub previously_downloaded: u64,
    pub file_map: Arc<SharedFileMap>,
    pub progress: Arc<AtomicU64>,
    pub download_limiter: Arc<BandwidthLimiter>,
    pub file_limiter: Arc<BandwidthLimiter>,
    pub result: mpsc::Sender<DownloadResult>,
    pub cancel_token: CancellationToken,
    pub active_operations_sender: UnboundedSender<(FileId, ActiveOperation)>,
    // If this is true, it means that this entire fail failed to be downloaded as a range
    // so we are falling back to a stream. Only one of the range jobs will be converted to a stream
    // all the others will be dropped.
    pub is_converting_to_stream: Arc<AtomicBool>,
    pub retries: usize,
}

#[derive(Debug)]
pub enum MetadataResult {
    Stream { 
        file_size: FileSize, // can be either a known size, or unknown altogether
        file_name: String,
    }, 
    Chunked { 
        file_size: u64, // always has to be a known size
        file_name: String,
    }, 
}

#[derive(Debug)]
pub enum DownloadResult {
    Metadata {
        file_id: FileId,
        metadata: MetadataResult,
    },
    Stream {
        file_id: FileId,
        bytes_downloaded: u64,
    },
    Range {
        file_id: FileId,
        range: Range<usize>,
        hashes: Vec<[u8; 16]>,
    },
    Failed(PermanentFailure),
}

enum HostMessage {
    QueueMetadata(MetadataJob),
    QueueStream(StreamJob),
    QueueRanges(Vec<RangeJob>),
    RetryReady(FailedJob),
    RateLimited(Duration),
    RateLimitExpired,
}

#[derive(Debug, Clone)]
pub enum SchedulingStrategy {
    /// Default: first-in, first-out.
    Fifo,
    /// Pick the largest remaining work first (by expected bytes).
    BiggestFirst,
    /// Prioritize a specific file before any other work in its category.
    PrioritizeFile(FileId),
    /// Prioritize all files belonging to a specific download.
    PrioritizeDownload(DownloadId),
}

impl SchedulingStrategy {
    fn select_metadata(&self, queue: &VecDeque<MetadataJob>) -> Option<usize> {
        match self {
            Self::Fifo => {
                (!queue.is_empty()).then_some(0)
            }
            Self::BiggestFirst => {
                // Metadata jobs are all equal weight, we fall back to FIFO
                (!queue.is_empty()).then_some(0)
            }
            Self::PrioritizeFile(file_id) => {
                // If we don't find a file with the file_id provided
                // we fall back to FIFO
                queue.iter()
                    .position(|job| job.file_id == *file_id)
                    .or_else(|| (!queue.is_empty()).then_some(0))
            }
            Self::PrioritizeDownload(download_id) => {
                // If we don't find a file with the download_id provided
                // we fall back to FIFO
                queue.iter()
                    .position(|job| job.download_id == *download_id)
                    .or_else(|| (!queue.is_empty()).then_some(0))
            }
        }
    }

    fn select_stream(&self, queue: &VecDeque<StreamJob>) -> Option<usize> {
        match self {
            Self::Fifo => {
                (!queue.is_empty()).then_some(0)
            }
            Self::BiggestFirst => {
                // StreamJob doesn't know their size, so we use FIFO
                (!queue.is_empty()).then_some(0)
            }
            Self::PrioritizeFile(file_id) => {
                // If we don't find a file with the file_id provided
                // we fall back to FIFO
                queue.iter()
                    .position(|job| job.file_id == *file_id)
                    .or_else(|| (!queue.is_empty()).then_some(0))
            }
            Self::PrioritizeDownload(download_id) => {
                // If we don't find a file with the download_id provided
                // we fall back to FIFO
                queue.iter()
                    .position(|job| job.download_id == *download_id)
                    .or_else(|| (!queue.is_empty()).then_some(0))
            }
        }
    }
    
    fn select_range(&self, queue: &VecDeque<RangeJob>) -> Option<usize> {
        match self {
            Self::Fifo => {
                (!queue.is_empty()).then_some(0)
            }
            Self::BiggestFirst => {
                queue.iter()
                    .enumerate()
                    .max_by_key(|(_index, job)| job.expected_bytes - job.previously_downloaded)
                    .map(|(index, _job)| index)
            }
            Self::PrioritizeFile(file_id) => {
                // If we don't find a file with the file_id provided
                // we fall back to FIFO
                queue.iter()
                    .position(|job| job.file_id == *file_id)
                    .or_else(|| (!queue.is_empty()).then_some(0))
            }
            Self::PrioritizeDownload(download_id) => {
                // If we don't find a file with the download_id provided
                // we fall back to FIFO
                queue.iter()
                    .position(|job| job.download_id == *download_id)
                    .or_else(|| (!queue.is_empty()).then_some(0))
            }
        }
    }
}

pub struct HostScheduler {
    strategy: SchedulingStrategy,

    // Retry queues (priority: metadata > stream > range)
    metadata_retry: VecDeque<MetadataJob>,
    stream_retry: VecDeque<StreamJob>,
    range_retry: VecDeque<RangeJob>,

    // Main queues
    metadata: VecDeque<MetadataJob>,
    stream: VecDeque<StreamJob>,
    range: VecDeque<RangeJob>,

    notify: Arc<Notify>,
}

pub enum NextJob {
    Metadata(MetadataJob),
    Stream(StreamJob),
    Range(RangeJob),
}

impl HostScheduler {
    pub fn new(strategy: SchedulingStrategy) -> Self {    
        Self {
            strategy,
            metadata_retry: VecDeque::new(),
            stream_retry: VecDeque::new(),
            range_retry: VecDeque::new(),
            metadata: VecDeque::new(),
            stream: VecDeque::new(),
            range: VecDeque::new(),
            notify: Arc::new(Notify::const_new()),
        }
    }

    pub fn set_strategy(&mut self, strategy: SchedulingStrategy) {
        self.strategy = strategy;
    }
    
    pub fn push_metadata(&mut self, job: MetadataJob) {
        self.metadata.push_back(job);
        self.notify.notify_waiters();
    }
    
    pub fn push_stream(&mut self, job: StreamJob) {
        self.stream.push_back(job);
        self.notify.notify_waiters();
    }
    
    pub fn push_range(&mut self, job: RangeJob) {
        self.range.push_back(job);
        self.notify.notify_waiters();
    }

    fn retry(&mut self, failed_job: FailedJob) {
        match failed_job {
            FailedJob::Metadata(metadata_job) => self.metadata_retry.push_back(metadata_job),
            FailedJob::Stream(stream_job) => self.stream_retry.push_back(stream_job),
            FailedJob::Range(range_job) => self.range_retry.push_back(range_job),
        }
        self.notify.notify_waiters();
    }

    pub async fn next(&mut self, permits_available: usize, permits_total: usize) -> NextJob {
        loop {
            if !self.has_work() {
                let notified = self.notify.notified();
                tokio::pin!(notified);
                
                if !self.has_work() {
                    notified.as_mut().enable();
                    (&mut notified).await;
                }
            }
            
            // Retries first (metadata > stream > range)
            if let Some(job) = self.take_from_retry_metadata() {
                return NextJob::Metadata(job);
            }
            if let Some(job) = self.take_from_retry_stream() {
                return NextJob::Stream(job);
            }
            if let Some(job) = self.take_from_retry_range() {
                return NextJob::Range(job);
            }
    
            // If we only have one permit left, prioritize metadata
            if permits_available == 1 {
                if let Some(job) = self.take_metadata() {
                    return NextJob::Metadata(job);
                }
            }
    
            // 1.0 means no permits are free (saturated)
            // 0.0 means all permits are free (idle)
            let busy_ratio = 1.0 - (permits_available as f64 / permits_total.max(1) as f64);
    
            // If more than half of our permits are taken, prefer metadata
            if busy_ratio > 0.5 {
                if let Some(job) = self.take_metadata() {
                    return NextJob::Metadata(job);
                }
            }
            
            if let Some(job) = self.take_stream() {
                return NextJob::Stream(job);
            }
            if let Some(job) = self.take_range() {
                return NextJob::Range(job);
            }
            if let Some(job) = self.take_metadata() {
                return NextJob::Metadata(job);
            }
        }
    }

    // Retry helpers are always FIFO
    fn take_from_retry_metadata(&mut self) -> Option<MetadataJob> {
        self.metadata_retry.pop_front()
    }
    
    fn take_from_retry_stream(&mut self) -> Option<StreamJob> {
        self.stream_retry.pop_front()
    }
    
    fn take_from_retry_range(&mut self) -> Option<RangeJob> {
        self.range_retry.pop_front()
    }

    fn take_metadata(&mut self) -> Option<MetadataJob> {
        let index = self.strategy.select_metadata(&self.metadata)?;
        self.metadata.remove(index)
    }
    
    fn take_stream(&mut self) -> Option<StreamJob> {
        let index = self.strategy.select_stream(&self.stream)?;
        self.stream.remove(index)
    }
    
    fn take_range(&mut self) -> Option<RangeJob> {
        let index = self.strategy.select_range(&self.range)?;
        self.range.remove(index)
    }

    pub fn has_work(&self) -> bool {
        !self.metadata_retry.is_empty()
            || !self.stream_retry.is_empty()
            || !self.range_retry.is_empty()
            || !self.metadata.is_empty()
            || !self.stream.is_empty()
            || !self.range.is_empty()
    }
}

pub struct HostManager {
    host: Host,
    client: Client,
    writer: DownloadWriterManager,
    ui_handle: UiManagerHandle,
    app_manager: mpsc::Sender<AppManagerCommand>,
    receiver: mpsc::Receiver<HostMessage>,
    sender: mpsc::Sender<HostMessage>,
    host_limiter: Arc<BandwidthLimiter>,
    global_limiter: Arc<BandwidthLimiter>,

    scheduler: HostScheduler,
    permits: Arc<Semaphore>,
    max_permits: usize,
    rate_limit_sender: Option<mpsc::UnboundedSender<Duration>>,
}

impl HostManager {
    fn new(
        host: Host, 
        client: Client,
        writer: DownloadWriterManager,
        ui_handle: UiManagerHandle,
        app_manager: mpsc::Sender<AppManagerCommand>,
        receiver: mpsc::Receiver<HostMessage>, 
        sender: mpsc::Sender<HostMessage>, 
        host_limiter: Arc<BandwidthLimiter>, 
        global_limiter: Arc<BandwidthLimiter>
    ) -> Self {
        Self {
            host,
            client,
            writer,
            ui_handle,
            app_manager,
            receiver,
            sender,
            host_limiter,
            global_limiter,
            scheduler: HostScheduler::new(SchedulingStrategy::Fifo),
            permits: Arc::new(Semaphore::const_new(5)),
            max_permits: 5,
            rate_limit_sender: None,
        }
    }

    pub async fn run(mut self) {  
        loop {
            tokio::select! {
                result = async {          
                    // Ask the scheduler if we have a next job
                    let job = self.scheduler.next(
                        self.permits.available_permits(),
                        self.max_permits,
                    ).await;
                    
                    let _permit = self.permits.clone().acquire_owned().await.unwrap();
                    
                    (job, _permit)
                } => {
                    let (next, _permit) = result;
                    
                    match next {
                        NextJob::Metadata(job) => self.dispatch_metadata(job, _permit),
                        NextJob::Stream(job) => self.dispatch_stream(job, _permit).await,
                        NextJob::Range(job) => self.dispatch_range(job, _permit),
                    }
                }
                Some(message) = self.receiver.recv() => {
                    match message {
                        HostMessage::QueueMetadata(metadata_job) => {
                            self.scheduler.push_metadata(metadata_job);
                        },
                        HostMessage::QueueStream(stream_job) => {
                            self.scheduler.push_stream(stream_job);
                        },
                        HostMessage::QueueRanges(range_job) => {
                            if let Some(job) = range_job.get(0) {
                                debug!("Host manager received request to queue {} range jobs for file {} from download {}", range_job.len(), job.file_id, job.download_id);
                            } else {
                                debug!("Host manager received request to queue {} range jobs", range_job.len());
                            }
                            
                            for job in range_job {
                                self.scheduler.push_range(job);
                            }
                        },
                        HostMessage::RetryReady(failed_job) => {
                            self.scheduler.retry(failed_job);
                        }
                        HostMessage::RateLimited(duration) => {
                            // There is already a task handling rate limiting, let's send the new duration we got
                            if let Some(sender) = &self.rate_limit_sender {
                                let _ = sender.send(duration);
                            } else {
                                // There is still no task handling rate limiting, let's create one
                                let (rate_limit_sender, mut rate_limit_receiver) = mpsc::unbounded_channel();
                                self.rate_limit_sender = Some(rate_limit_sender);

                                let permits = self.permits.clone();
                                let max_permits = self.max_permits;
                                let sender = self.sender.clone();

                                tokio::spawn(async move {
                                    // Hold all permits until rate limit expires
                                    let held = (0..max_permits)
                                        .map(|_| permits.clone().acquire_owned())
                                        .collect::<FuturesUnordered<_>>()
                                        .collect::<Vec<_>>()
                                        .await;
                                    
                                    let mut deadline = Instant::now() + duration;
                                    let mut sleep = Box::pin(tokio::time::sleep_until(deadline));
                                    
                                    loop {
                                        tokio::select! {
                                            _ = &mut sleep => {
                                                // We drop all the permits so they can be used once again
                                                drop(held);
                                                let _ = sender.send(HostMessage::RateLimitExpired).await;
                                                break;
                                            }
                                            new_duration = rate_limit_receiver.recv() => {
                                                match new_duration {
                                                    Some(new_duration) => {
                                                        let new_deadline = Instant::now() + new_duration;
                                                            
                                                        if new_deadline > deadline {
                                                            deadline = new_deadline;
                                                            sleep.as_mut().reset(deadline);
                                                        }
                                                    },
                                                    None => break,
                                                }
                                            }
                                        }
                                    }
                                });
                            }
                        },
                        HostMessage::RateLimitExpired => {
                            self.rate_limit_sender = None;
                        }
                    }
                }
            }
        }
    }

    fn dispatch_metadata(&self, metadata_job: MetadataJob, _permit: OwnedSemaphorePermit) {
        let client = self.client.clone();
        let sender = self.sender.clone();
        
        tokio::spawn(async move {
            let _permit = _permit;
            
            match Self::fetch_metadata(client, &metadata_job.url).await {
                Ok(metadata) => {
                    let _ = metadata_job.result.send(DownloadResult::Metadata { file_id: metadata_job.file_id, metadata }).await;
                },
                Err(metadata_error) => {
                    Self::handle_metdata_failed(sender, metadata_job, metadata_error, MAX_RETRIES).await;
                },
            }
        });
    }

    async fn dispatch_stream(&self, stream_job: StreamJob, _permit: OwnedSemaphorePermit) {
        let client = self.client.clone();
        let limiters = vec![self.global_limiter.clone(), self.host_limiter.clone(), stream_job.download_limiter.clone(), stream_job.file_limiter.clone()];
        let sender = self.sender.clone();

        let _ = stream_job.active_operations_sender.send((stream_job.file_id, ActiveOperation::Downloading));
        
        tokio::spawn(async move {
            let _permit = _permit;
            
            tokio::select! {
                result = download_stream(client.clone(), &stream_job.path, &stream_job.url, limiters) => {
                    match result {
                        Ok(bytes_downloaded) => {
                            let _ = stream_job.result.send(DownloadResult::Stream { file_id: stream_job.file_id, bytes_downloaded }).await;
                        },
                        Err(download_error) => {
                            Self::handle_stream_failed(sender, stream_job, download_error, MAX_RETRIES).await;
                        },
                    }
                }
                _ = stream_job.cancel_token.cancelled() => {
                    return;
                }
            }
        });
    }

    fn dispatch_range(&self, range_job: RangeJob, _permit: OwnedSemaphorePermit) {
        let client = self.client.clone();

        let limiters = vec![self.global_limiter.clone(), self.host_limiter.clone(), range_job.download_limiter.clone(), range_job.file_limiter.clone()];
        let writer = self.writer.sender();
        let ui_handle = self.ui_handle.clone();
        let sender = self.sender.clone();
        let app_manager = self.app_manager.clone();

        let _ = range_job.active_operations_sender.send((range_job.file_id, ActiveOperation::Downloading));

        trace!("Spawning range job [{}..{}] for file {} from download {}", range_job.range.start, range_job.range.end, range_job.file_id, range_job.download_id);

        tokio::spawn(async move {
            let _permit = _permit;
            
            // Do worker stuff
            tokio::select! {
                result = download_range(client, &range_job, limiters, writer, ui_handle) => {
                    match result {
                        Ok(chunk_hashes) => {
                            let _ = range_job.result.send(DownloadResult::Range { file_id: range_job.file_id, range: range_job.range, hashes: chunk_hashes }).await;
                        }
                        Err(download_error) => {
                            Self::handle_range_failed(sender, app_manager, range_job, download_error, MAX_RETRIES).await;
                        }
                    }
                }
                _ = range_job.cancel_token.cancelled() => {
                    return;
                }
            }
        });
    }

    async fn handle_metdata_failed(sender: mpsc::Sender<HostMessage>, metadata_job: MetadataJob, metadata_error: MetadataError, max_retries: usize) {
        let mut failed_job = FailedJob::Metadata(metadata_job);
        
        match metadata_error {
            MetadataError::HttpStatus(status_code) => {
                if status_code.is_client_error() {
                    Self::handle_client_error(sender, failed_job, status_code, max_retries).await;
                } else if status_code.is_server_error() {
                    Self::handle_server_error(sender, failed_job, status_code, max_retries).await;
                } else {
                    failed_job.increment_retries();
                    
                    if failed_job.retries() >= max_retries {
                        let failure = failed_job.into_permanent_failure(
                            PermanentFailureKind::Http(HttpFailureKind::UnexpectedStatus { status: status_code.as_u16() }),
                            Box::new(std::io::Error::new(std::io::ErrorKind::Other, format!("Unexpected status: {status_code}"))),
                        );
                        
                        let _ = failed_job.result().send(DownloadResult::Failed(failure)).await;
                    } else {
                        warn!("Got unexpected status code {} when fetching metadata for file {} in download {}. Retrying ({}/{})...", 
                            status_code,
                            failed_job.file_id(), failed_job.download_id(), failed_job.retries(), max_retries);
                        
                        let jitter = Duration::from_millis(rng().random_range(0..500));
                        let delay = Duration::from_secs(1) * 2u32.pow(failed_job.retries() as u32) + jitter;
                        Self::retry_after(sender, failed_job, delay).await;
                    }
                }
            },
            MetadataError::Network(error) => {
                Self::handle_network_error(sender, failed_job, error, max_retries).await;
            },
        }
    }

    async fn handle_stream_failed(sender: mpsc::Sender<HostMessage>, stream_job: StreamJob, download_error: DownloadError, max_retries: usize) {
        let failed_job = FailedJob::Stream(stream_job);

        match download_error {
            DownloadError::Io(error) =>  {
                Self::handle_io_error(sender, failed_job, error, max_retries).await;
            },
            DownloadError::Network(error) => {
                Self::handle_network_error(sender, failed_job, error, max_retries).await;
            },
            DownloadError::RateLimited(retry_after) => {
                Self::handle_rate_limited(sender, failed_job, retry_after).await;
            },
            DownloadError::ServerError(status_code) => {
                Self::handle_server_error(sender, failed_job, status_code, max_retries).await;
            },
            DownloadError::ClientError(status_code) => {
                Self::handle_client_error(sender, failed_job, status_code, max_retries).await;
            },
        }
    }

    async fn handle_range_failed(sender: mpsc::Sender<HostMessage>, app_manager: mpsc::Sender<AppManagerCommand>, range_job: RangeJob, download_error: RangeDownloadError, max_retries: usize) {
        match download_error {
            RangeDownloadError::Download(download_error) => {
                let failed_job = FailedJob::Range(range_job);
                
                match download_error {
                    DownloadError::Io(error) =>  {
                        Self::handle_io_error(sender, failed_job, error, max_retries).await;
                    },
                    DownloadError::Network(error) => {
                        Self::handle_network_error(sender, failed_job, error, max_retries).await;
                    },
                    DownloadError::RateLimited(retry_after) => {
                        Self::handle_rate_limited(sender, failed_job, retry_after).await;
                    },
                    DownloadError::ServerError(status_code) => {
                        Self::handle_server_error(sender, failed_job, status_code, max_retries).await;
                    },
                    DownloadError::ClientError(status_code) => {
                        Self::handle_client_error(sender, failed_job, status_code, max_retries).await;
                    },
                }
            },
            RangeDownloadError::UnexpectedStatus(status_code) => {
                let range = range_job.range.clone();
                let mut failed_job = FailedJob::Range(range_job);
                failed_job.increment_retries();
                
                if failed_job.retries() >= max_retries {
                    let failure = failed_job.into_permanent_failure(
                        PermanentFailureKind::Http(HttpFailureKind::UnexpectedStatus { status: status_code.as_u16() }),
                        Box::new(std::io::Error::new(std::io::ErrorKind::Other, format!("Unexpected status: {status_code}"))),
                    );
                    
                    let _ = failed_job.result().send(DownloadResult::Failed(failure)).await;
                } else {
                    warn!("Got unexpected status code {} for range {}..{} in file {} download {}. Retrying ({}/{})...", 
                        status_code, range.start, range.end,
                        failed_job.file_id(), failed_job.download_id(), failed_job.retries(), max_retries);
                    
                    let jitter = Duration::from_millis(rng().random_range(0..500));
                    let delay = Duration::from_secs(1) * 2u32.pow(failed_job.retries() as u32) + jitter;
                    Self::retry_after(sender, failed_job, delay).await;
                }
            },
            RangeDownloadError::UnexpectedLength(bytes_received, bytes_expected) => {
                let range = range_job.range.clone();
                let mut failed_job = FailedJob::Range(range_job);
                failed_job.increment_retries();
                
                if failed_job.retries() >= max_retries {
                    let failure = failed_job.into_permanent_failure(
                        PermanentFailureKind::TooManyRetries { attempts: failed_job.retries() },
                        Box::new(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, 
                            format!("Expected {bytes_expected} bytes, received {bytes_received}"))),
                    );
                    
                    let _ = failed_job.result().send(DownloadResult::Failed(failure)).await;
                } else {
                    warn!("Got unexpected length for range {}..{} in file {} download {}. Received: {}, expected: {}. Retrying ({}/{})...", 
                        range.start, range.end,
                        failed_job.file_id(), failed_job.download_id(), bytes_received, bytes_expected, failed_job.retries(), max_retries);
                    
                    // This error is usually from a droppped connection, so don't wait much before retrying
                    Self::retry_after(sender, failed_job, Duration::from_millis(300)).await;
                }
            },
            RangeDownloadError::RangeNotSupported => {
                warn!("Got non-range response for file {} in download {}. Falling back to stream.", range_job.file_id, range_job.download_id);

                // Convert to stream job
                if let Ok(stream_job) = StreamJob::try_from(range_job) {
                    // We send a retry ready instead of a normal queue command
                    // to avoid sending this message to the end of the normal queue
                    let failed_job = FailedJob::Stream(stream_job);
                    let _ = sender.send(RetryReady(failed_job)).await;
                }
            },
            RangeDownloadError::DiskWriteError(error) => {
                let failed_job = FailedJob::Range(range_job);
                Self::handle_io_error(sender, failed_job, error, max_retries).await;
            },
            RangeDownloadError::DiskPoolDropped => {
                error!("App-wide disk pool dropped. App entered an invalid state and should restart. This probably happened due to an OS error or logic bug.");

                let _ = app_manager.send(AppManagerCommand::Shutdown).await;
            },
        }
    }

    async fn handle_io_error(sender: mpsc::Sender<HostMessage>, mut failed_job: FailedJob, error: io::Error, max_retries: usize) {
        match error.kind() {
            // Permanent Errors that should not be retried
            io::ErrorKind::NotFound |
            io::ErrorKind::PermissionDenied |
            io::ErrorKind::NotADirectory |
            io::ErrorKind::IsADirectory |
            io::ErrorKind::InvalidInput |
            io::ErrorKind::AddrInUse |
            io::ErrorKind::AddrNotAvailable |
            io::ErrorKind::AlreadyExists |
            io::ErrorKind::DirectoryNotEmpty |
            io::ErrorKind::ReadOnlyFilesystem |
            io::ErrorKind::StaleNetworkFileHandle |
            io::ErrorKind::InvalidData |
            io::ErrorKind::NotSeekable |
            io::ErrorKind::CrossesDevices |
            io::ErrorKind::TooManyLinks |
            io::ErrorKind::InvalidFilename |
            io::ErrorKind::ArgumentListTooLong |
            io::ErrorKind::Unsupported =>  {
                error!("IO error: {error}");

                let failure = failed_job.into_permanent_failure(PermanentFailureKind::from(&error), Box::new(error));
                
                let _ = failed_job.result().send(DownloadResult::Failed(failure)).await;
            }
            
            // Storage errors
            io::ErrorKind::WriteZero |
            io::ErrorKind::StorageFull |
            io::ErrorKind::QuotaExceeded |
            io::ErrorKind::FileTooLarge |
            io::ErrorKind::OutOfMemory => {
                error!("The system has ran out of storage: {error}");
                
                let failure = failed_job.into_permanent_failure(PermanentFailureKind::from(&error), Box::new(error));
                
                let _ = failed_job.result().send(DownloadResult::Failed(failure)).await;
            },

            // Retryiable errors
            io::ErrorKind::NetworkUnreachable |
            io::ErrorKind::WouldBlock |
            io::ErrorKind::ConnectionReset |
            io::ErrorKind::ConnectionAborted |
            io::ErrorKind::NotConnected |
            io::ErrorKind::NetworkDown |
            io::ErrorKind::BrokenPipe |
            io::ErrorKind::HostUnreachable |
            io::ErrorKind::TimedOut |
            io::ErrorKind::ResourceBusy |
            io::ErrorKind::ExecutableFileBusy |
            io::ErrorKind::Deadlock |
            io::ErrorKind::Interrupted => {
                failed_job.increment_retries();
                
                if failed_job.retries() >= max_retries {
                    let failure = failed_job.into_permanent_failure(
                        PermanentFailureKind::TooManyRetries { attempts: failed_job.retries() },
                        Box::new(error),
                    );
                    
                    let _ = failed_job.result().send(DownloadResult::Failed(failure)).await;
                } else {
                    warn!("Temporary OS error: {error}. Retrying ({}/{})...", failed_job.retries(), max_retries);
                    let jitter = Duration::from_millis(rng().random_range(0..500));
                    let delay = Duration::from_secs(1) * 2u32.pow(failed_job.retries() as u32) + jitter;
                    Self::retry_after(sender, failed_job, delay).await;
                }
            },
            // We retry unknown errors
            _ => {
                failed_job.increment_retries();
                
                if failed_job.retries() >= max_retries {
                    let failure = failed_job.into_permanent_failure(
                        PermanentFailureKind::TooManyRetries { attempts: failed_job.retries() },
                        Box::new(error),
                    );
                    
                    let _ = failed_job.result().send(DownloadResult::Failed(failure)).await;
                } else {
                    warn!("Uncategorized OS error: {error}. Retrying ({}/{})...", failed_job.retries(), max_retries);
                    let jitter = Duration::from_millis(rng().random_range(0..500));
                    let delay = Duration::from_secs(1) * 2u32.pow(failed_job.retries() as u32) + jitter;
                    Self::retry_after(sender, failed_job, delay).await;
                }
            },
        }
    }

    async fn handle_network_error(sender: mpsc::Sender<HostMessage>, mut failed_job: FailedJob, error: reqwest::Error, max_retries: usize) {
        failed_job.increment_retries();
        
        if failed_job.retries() >= max_retries {
            let failure = failed_job.into_permanent_failure(
                PermanentFailureKind::TooManyRetries { attempts: failed_job.retries() },
                Box::new(error),
            );
            
            let _ = failed_job.result().send(DownloadResult::Failed(failure)).await;
        } else {
            match &failed_job {
                FailedJob::Metadata(metadata_job) => warn!("Network error for file {} from download {}: {} (cause: {:?}). Retrying ({}/{})...", metadata_job.file_id, metadata_job.download_id, error, error.status(), metadata_job.retries, max_retries),
                FailedJob::Stream(stream_job) => warn!("Network error for file {} from download {}: {} (cause: {:?}). Retrying ({}/{})...", stream_job.file_id, stream_job.download_id, error, error.status(), stream_job.retries, max_retries),
                FailedJob::Range(range_job) => {
                    warn!("Network error for range {}..{} in file {} from download {}: {} (cause: {:?}). Retrying ({}/{})...",
                        range_job.range.start, range_job.range.end,
                        range_job.file_id,
                        range_job.download_id,
                        error,
                        error.status(),
                        range_job.retries,
                        max_retries
                    );
                },
            }
            
            let jitter = Duration::from_millis(rng().random_range(0..500));
            let delay = Duration::from_secs(1) * 2u32.pow(failed_job.retries() as u32) + jitter;
            Self::retry_after(sender, failed_job, delay).await;
        }
    }

    async fn handle_rate_limited(sender: mpsc::Sender<HostMessage>, failed_job: FailedJob, retry_after: Option<u64>) {
        warn!("Rate limited for file {} from download {}.", failed_job.file_id(), failed_job.download_id());

        let duration = retry_after.map(Duration::from_secs).unwrap_or(Duration::from_secs(5));
        
        let _ = failed_job.active_operations_sender().send((failed_job.file_id(), ActiveOperation::Waiting(duration)));
        
        let _ = sender.send(HostMessage::RateLimited(duration)).await;
        let _ = sender.send(HostMessage::RetryReady(failed_job)).await;
    }

    async fn handle_server_error(sender: mpsc::Sender<HostMessage>, mut failed_job: FailedJob, status_code: StatusCode, max_retries: usize) {
        failed_job.increment_retries();
        if failed_job.retries() >= max_retries {
            let failure = failed_job.into_permanent_failure(
                PermanentFailureKind::TooManyRetries { attempts: failed_job.retries() },
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, format!("Server error: {status_code}"))),
            );
            let _ = failed_job.result().send(DownloadResult::Failed(failure)).await;
        } else {
            warn!("Server error for file {} from download {}: Status Code {}. Retrying ({}/{})...", status_code, failed_job.file_id(), failed_job.download_id(), failed_job.retries(), max_retries);
            let jitter = Duration::from_millis(rng().random_range(0..500));
            let delay = Duration::from_secs(1) * 2u32.pow(failed_job.retries() as u32) + jitter;
            Self::retry_after(sender, failed_job, delay).await;
        }
    }

    async fn handle_client_error(sender: mpsc::Sender<HostMessage>, mut failed_job: FailedJob, status_code: StatusCode, max_retries: usize) { 
        if status_code == StatusCode::NOT_FOUND {
            error!("File not found (404) for file {} in download {}.", failed_job.file_id(), failed_job.download_id());
            
            let failure = failed_job.into_permanent_failure(
                PermanentFailureKind::Http(HttpFailureKind::FileNotFound),
                Box::new(std::io::Error::new(std::io::ErrorKind::NotFound, "HTTP 404")),
            );
            
            let _ = failed_job.result().send(DownloadResult::Failed(failure)).await;
            
            return;
        }
        
        failed_job.increment_retries();
        
        if failed_job.retries() >= max_retries {
            let failure = failed_job.into_permanent_failure(
                PermanentFailureKind::TooManyRetries { attempts: failed_job.retries() },
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, format!("Client error: {status_code}"))),
            );
            
            let _ = failed_job.result().send(DownloadResult::Failed(failure)).await;
        } else {
            error!("Client error for file {} from download {}: Status Code {}. Retrying ({}/{})...", failed_job.file_id(), failed_job.download_id(), status_code, failed_job.retries(), max_retries);
            let jitter = Duration::from_millis(rng().random_range(0..500));
            let delay = Duration::from_secs(1) * 2u32.pow(failed_job.retries() as u32) + jitter;
            Self::retry_after(sender, failed_job, delay).await;
        }
    }

    async fn retry_after(sender: mpsc::Sender<HostMessage>, failed_job: FailedJob, duration: Duration) {
        let _ = failed_job.active_operations_sender().send((failed_job.file_id(), ActiveOperation::Waiting(duration)));

        tokio::spawn(async move {
            tokio::select! {
                _ = tokio::time::sleep(duration) => {
                    let _ = sender.send(HostMessage::RetryReady(failed_job)).await;
                }
                _ = failed_job.cancel_token().cancelled() => {
                    return;
                }
            }
        });
    }

    async fn fetch_metadata(client: Client, url: &str) -> Result<MetadataResult, MetadataError> {
        // Try a HEAD request first
        let head_result = client.head(url)
            .header("Accept-Encoding", "identity")
            .send()
            .await;

        // If we don't head a response back (i.e HEAD fails) 
        // we continue to try other ways of getting metadata
        if let Ok(response) = head_result  {
            // HEAD succeeded, but we got an error status
            if !response.status().is_success() {
                return Err(MetadataError::HttpStatus(response.status()));
            }

            let file_name = Self::extract_filename(&response, url);

            let accepts_ranges = response.headers()
                .get(header::ACCEPT_RANGES)
                .map(|value| value.as_bytes() != b"none")
                .unwrap_or(false);

            // If the host accepts ranges for this file, it means we have a file that can be chunked
            match response.content_length() {
                Some(length) if length == 0 => { 
                    // Content-Length 0 on HEAD is suspicious, we fall through to GET
                }
                Some(length) => {
                    if accepts_ranges {
                        return Ok(MetadataResult::Chunked { file_size: length, file_name });
                    } else {
                        // Has length but doesn't accept ranges
                        return Ok(MetadataResult::Stream { file_size: FileSize::Known(length), file_name });
                    }
                },
                None => {
                    // HEAD succeeded, but we got no Content-Length back, the file size is unknown
                    return Ok(MetadataResult::Stream { file_size: FileSize::Unknown, file_name });
                },
            }
        }
    
        // If HEAD fails or returns no length, do a GET request and abort immediately to avoid downloading body
        let get_result = client.get(url)
            .header("Accept-Encoding", "identity")
            .header("Range", "bytes=0-0")
            .send()
            .await;
    
        match get_result {
            Ok(response) => match response.status() {
                StatusCode::PARTIAL_CONTENT => {
                    // Server supports ranges, the download should be chunked
                    let file_name = Self::extract_filename(&response, url);
                    
                    if let Some(range_header) = response.headers().get(header::CONTENT_RANGE)
                        && let Ok(str) = range_header.to_str()
                    {
                        // If we don't get a content rage, this will fall back to a stream
                        if let Some(length) = Self::parse_content_range(str) {
                            return Ok(MetadataResult::Chunked { file_size: length, file_name });
                        }
                    }

                    // The download doesn't support ranges, it should be streamed
                    Ok(MetadataResult::Stream { file_size: FileSize::Unknown, file_name })
                }
                StatusCode::OK => {
                    // Server doesn't support ranges and instead returned the full file
                    let file_name = Self::extract_filename(&response, url);
                    
                    let file_size = response.content_length()
                        .map(FileSize::Known)
                        .unwrap_or(FileSize::Unknown);
                    
                    Ok(MetadataResult::Stream { file_size, file_name })
                }
                status => Err(MetadataError::HttpStatus(status)),
            }
            Err(err) => Err(MetadataError::Network(err)),
        }
    }

    fn extract_filename(response: &Response, url: &str) -> String {
        // We first try to extract the file name from the content disposition header
        if let Some(name) = response.headers()
            .get(header::CONTENT_DISPOSITION)
            .and_then(|value| value.to_str().ok())
            .and_then(Self::parse_content_disposition_filename) 
        {
            return name;
        }
        
        // If getting the file name from content disposition fails, we fallback to
        // extracting it from the url
        Self::filename_from_url(url)
    }
    
    fn parse_content_disposition_filename(header: &str) -> Option<String> {
        header.split(';')
            .find(|part| part.trim().starts_with("filename"))
            .and_then(|part| {
                let value = part.splitn(2, '=').nth(1)?;
                Some(value.trim().trim_matches('"').to_string())
            })
    }

    fn filename_from_url(url: &str) -> String {
        url.rsplit('/')
            .next()
            .unwrap_or("unknown")
            .split('?')
            .next()
            .unwrap_or("unknown")
            .to_string()
    }

    fn parse_content_range(range_header: &str) -> Option<u64> {
         // e.g. "bytes 0-0/1048576"
        range_header.rsplit('/').next()?.parse::<u64>().ok()
    }
}

#[derive(Clone)]
pub struct HostHandle {
    sender: mpsc::Sender<HostMessage>,
}

impl HostHandle {
    pub fn spawn(host: Host, client: Client, writer: DownloadWriterManager, ui_handle: UiManagerHandle, app_manager: mpsc::Sender<AppManagerCommand>, host_limiter: Arc<BandwidthLimiter>, global_limiter: Arc<BandwidthLimiter>) -> Self {
        let (sender, receiver) = mpsc::channel(1000);

        let host_manager = HostManager::new(host, client, writer, ui_handle, app_manager, receiver, sender.clone(), host_limiter, global_limiter);

        tokio::spawn(async move {
            host_manager.run().await;
        });

        Self { 
            sender,
        }
    }

    pub async fn queue_metadata(&self, metadata_job: MetadataJob) {
        let _ = self.sender.send(HostMessage::QueueMetadata(metadata_job)).await;
    }
    
    pub async fn queue_stream(&self, stream_job: StreamJob) {
        let _ = self.sender.send(HostMessage::QueueStream(stream_job)).await;
    }
    
    pub async fn queue_ranges(&self, range_jobs: Vec<RangeJob>) {
        let _ = self.sender.send(HostMessage::QueueRanges(range_jobs)).await;
    }
}

async fn download_range(
    client: Client, 
    range_job: &RangeJob,
    limiters: Vec<Arc<BandwidthLimiter>>,
    io_sender: flume::Sender<FileChunk>,
    ui_handle: UiManagerHandle,
)-> Result<Vec<[u8; 16]>, RangeDownloadError> {
    let start_byte = range_job.range.start as u64 * (BLOCK_SIZE as u64);
    let end_byte = start_byte + range_job.expected_bytes.saturating_sub(1); // -1 because http ranges are inclusive

    let range_header = format!("bytes={}-{}", start_byte, end_byte);

    let request = client.get(range_job.url.as_str())
        .header("Range", range_header);

    let response = match request.send().await {
        Ok(response) => {
            let status = response.status();
            
            if !status.is_success() {
                warn!("Range failed: {}..{} got status {}. Headers: {:?}",
                    range_job.range.start, range_job.range.end,
                    status,
                    response.headers()
                );
            }
            
            match status {
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
            }
        }
        Err(err) => return Err(DownloadError::Network(err).into()),
    }?;

    let raw_stream = response.bytes_stream();
    let throttled_stream = ThrottledStream::new(raw_stream, limiters);
    tokio::pin!(throttled_stream);

    let mut current_offset = start_byte;
    let mut bytes_received = 0; 

    let mut range_progress = RangeProgress::new(range_job.progress.clone());
    let mut unnotified_bytes = 0; 
    let mut current_progress = 0;

    let mut bytes_to_skip_reporting = range_job.previously_downloaded;

    // Disk IO variables
    let buffer_capacity: usize = 1024 * 1024; // 1 MB
    let mut buffer = BytesMut::with_capacity(buffer_capacity);
    let mut buffer_start_offset = current_offset;

    let mut in_flight_acks: VecDeque<(u64, oneshot::Receiver<std::io::Result<()>>)> = VecDeque::new();
    const MAX_IN_FLIGHT: usize = 4; // Max 4MB in RAM per worker before we apply backpressure

    // Chunk hashing variables
    let mut hashes = Vec::new();
    let mut hasher = blake3::Hasher::new();
    let mut chunk_bytes_hashed = 0;

    while let Some(chunk) = throttled_stream.next().await {
        let chunk = chunk.map_err(DownloadError::from)?; 
        let chunk_len = chunk.len() as u64;

        buffer.extend_from_slice(&chunk);

        while let Some((_, receiver)) = in_flight_acks.front_mut() {
            match receiver.try_recv() {
                Ok(Ok(_)) => {
                    let (bytes_written, _) = in_flight_acks.pop_front().unwrap();
                    let mut reportable_bytes = bytes_written;

                    // If there are still some bytes that we have to skip due to them already being downloaded for this range
                    // we subtract them from both the bytes we report now and from the bytes to skip
                    if bytes_to_skip_reporting > 0 {
                        let skip = reportable_bytes.min(bytes_to_skip_reporting);
                        bytes_to_skip_reporting -= skip;
                        reportable_bytes -= skip;
                    }

                    if reportable_bytes > 0 {
                        current_progress = range_progress.add(reportable_bytes);
                        unnotified_bytes += reportable_bytes;

                        if unnotified_bytes >= CHANNEL_UPDATE_THRESHOLD {
                            let update = FileUpdate::BytesDownloaded { 
                                id: range_job.file_id,
                                len: current_progress, 
                            };

                            ui_handle.update_file(range_job.download_id, update);
                            unnotified_bytes = 0; 
                        }
                    }
                }
                Ok(Err(error)) => return Err(RangeDownloadError::DiskWriteError(error)), // Disk failed
                Err(oneshot::error::TryRecvError::Empty) => break,
                Err(oneshot::error::TryRecvError::Closed) => return Err(RangeDownloadError::DiskPoolDropped),
            }
        }

        current_offset += chunk_len;
        bytes_received += chunk_len;

        // Disk IO sending logic
        if buffer.len() >= buffer_capacity {
            // Swap full buffer for an empty one
            let buffer_to_write = buffer.split().freeze();
            let bytes_to_write = buffer_to_write.len() as u64;

            buffer.reserve(buffer_capacity); 

            let (ack_sender, ack_receiver) = oneshot::channel();

            let file_chunk = FileChunk {
                file_map: range_job.file_map.clone(),
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
                
                let mut reportable_bytes = bytes_written;

                if bytes_to_skip_reporting > 0 {
                    let skip = reportable_bytes.min(bytes_to_skip_reporting);
                    bytes_to_skip_reporting -= skip;
                    reportable_bytes -= skip;
                }

                if reportable_bytes > 0 {
                    current_progress = range_progress.add(reportable_bytes);
                    unnotified_bytes += reportable_bytes;

                    if unnotified_bytes >= CHANNEL_UPDATE_THRESHOLD {
                        let update = FileUpdate::BytesDownloaded { 
                            id: range_job.file_id,
                            len: current_progress, 
                        };

                        ui_handle.update_file(range_job.download_id, update);
                        unnotified_bytes = 0; 
                    }
                }
            }
                
        }

        // Chunk hashing logic
        let mut remaining_chunk = chunk.as_ref();

        // We ue the reference to a slice of the hash to only calculate the hash when the hasher
        // receives `HASH_CHUNK_SIZE_BYTES`. If for some reason we receive less than expected
        // we store the remaining bytes in the hasher but don't calculate the hash, instead leaving
        // the calcualtion for next iteration when `chunk_bytes_hashed` can reach the size we expect.
        // This works due to chunk jobs being aligned to `HASH_CHUNK_SIZE_BYTES` so there will be no
        // situation where we hash anything less than expected unless we are in the very final
        // chunk of the whole file.
        while !remaining_chunk.is_empty() {
            // This substraction should use saturate_sub, if HASH_CHUNK_SIZE_BYTES - chunk_bytes_hashed < 0
            // is true, then that's a logic bug. saturate_sub might hide this logic bug and create an infinite loop here
            // the program crashing is better to catch that bug if it ever happens.
            assert!(chunk_bytes_hashed <= HASH_CHUNK_SIZE);

            // Check just in case this happens in release mode
            if chunk_bytes_hashed > HASH_CHUNK_SIZE {
                warn!("chunk_bytes_hashed is greater HASH_CHUNK_SIZE_BYTES. This is a massive bug that invalidates chunk hashes and makes verification after a download impossible.");
            }

            let bytes_needed_for_hash = HASH_CHUNK_SIZE - chunk_bytes_hashed;

            let take_len = remaining_chunk.len().min(bytes_needed_for_hash);

            let (to_hash, remainder) = remaining_chunk.split_at(take_len);

            hasher.update(to_hash);
            chunk_bytes_hashed += to_hash.len();

            if chunk_bytes_hashed == HASH_CHUNK_SIZE {
                let full_hash = hasher.finalize();
                let mut hash_16 = [0u8; 16];
                hash_16.copy_from_slice(&full_hash.as_bytes()[..16]);
                hashes.push(hash_16);
                
                hasher.reset();
                chunk_bytes_hashed = 0;
            }

            remaining_chunk = remainder;
        }

    }

    // Disk IO buffer has some remaining bytes
    if !buffer.is_empty() {
        let final_bytes_len = buffer.len() as u64;
        let (ack_sender, ack_receiver) = oneshot::channel();

        let file_chunk = FileChunk {
            file_map: range_job.file_map.clone(),
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

        let mut reportable_bytes = bytes_written;

        if bytes_to_skip_reporting > 0 {
            let skip = reportable_bytes.min(bytes_to_skip_reporting);
            bytes_to_skip_reporting -= skip;
            reportable_bytes -= skip;
        }

        if reportable_bytes > 0 {
            current_progress = range_progress.add(reportable_bytes);
            unnotified_bytes += reportable_bytes;
        }
    }

    // This can happen only in the final chunk of the whole file, as this is the
    // only chunk that isn't necessarily aligned to `HASH_CHUNK_SIZE_BYTES`.
    // So we just calculate the hash with anything that is left in the hasher.
    if chunk_bytes_hashed > 0 {
        let full_hash = hasher.finalize();
        let mut hash_16 = [0u8; 16];
        hash_16.copy_from_slice(&full_hash.as_bytes()[..16]);
        hashes.push(hash_16);
    }

    // Update UI if any unnotified bytes remain
    if unnotified_bytes > 0 {
        let file_update = FileUpdate::BytesDownloaded { 
            id: range_job.file_id,
            len: current_progress,
        };
        
        ui_handle.update_file(range_job.download_id, file_update);
    }

    if bytes_received != range_job.expected_bytes {
        return Err(RangeDownloadError::UnexpectedLength(bytes_received, range_job.expected_bytes));
    }

    range_progress.complete();

    trace!("Worker [{}, {}) finished", range_job.range.start, range_job.range.end);

    Ok(hashes)
}

/// Downloads a file from a server that requested `Transfer-Encoding: chunked`. 
/// The server doesn't provide a `Content-Length` header for these files and thus they can't be downloaded using a multi-part strategy.
/// These downloads are non-resumable.
async fn download_stream(client: Client, path: &Path, url: &str, limiters: Vec<Arc<BandwidthLimiter>>) -> Result<u64, DownloadError> {
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

    // TODO: change bytes writing from in place to sending them to writer
    let file = tokio::fs::File::create(&path).await?;

    let mut writer = BufWriter::new(file);
    let mut size = 0;

    let raw_stream = response.bytes_stream();
    let throttled_stream = ThrottledStream::new(raw_stream, limiters);
    tokio::pin!(throttled_stream);

    while let Some(chunk) = throttled_stream.next().await {
        let chunk = chunk?;

        size += chunk.len() as u64;
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
