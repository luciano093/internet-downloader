use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use reqwest::{Client, StatusCode, header};
use tokio::fs::create_dir_all;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::oneshot;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use crate::client_state_manager::UiStateEvent;
use crate::download::hosts::Host;
use crate::download::{Download, DownloadItem, DownloadStatus, DownloadType, DownloadUpdate, FileSize, FileUpdate};
use crate::host_manager::{ActiveDownloadPermit, HostMessage};
use crate::shared_file_map::SharedFileMap;
use crate::state_manager::StateManager;

pub enum SizeResult {
    Known(u64),
    Stream,
    Retryable(u16),
    PermanentFail,
}

pub enum SupervisorMessage {
    SpawnWorker(ActiveDownloadPermit),
    WorkerFinished(ActiveDownloadPermit, usize, (usize, usize), bool), // permit, id, range, success (false if failed)
    StreamFinished(ActiveDownloadPermit, usize, Result<usize, ()>), // permit, id, result containing size
    MetadataFetched(ActiveDownloadPermit, usize, SizeResult), 
}

#[derive(Debug)]
pub enum Job {
    GetSize(usize),
    DownloadChunk(usize, (usize, usize)),
    DownloadStream(usize), // file id
}

// TODO: try to see if i can implement a get_next_chunk()

struct SupervisorState {
    client: Client,
    download: Download,
    chunk_cursors: HashMap<usize, usize>, // used to keep track of last tracked chunk in a file to avoid looping through all the chunks every time
    uninitialized_cursor: usize, // track the last known initialized file
    streams_cursor: usize, // track the last known stream-only file
    retry_ranges: HashMap<usize, Vec<(usize, usize)>>, // ranges that failed but are still buffered
    retry_uninitialized: Vec<usize>, // tracks the files that failed to get metadata
    retry_streams: Vec<usize>, // tracks the files that failed to get metadata
    host_sender: UnboundedSender<HostMessage>,
    active_permits: usize, 
    active_downloads: usize, // tracks how many permits we are using to download files
    active_metadata_requests: usize, // tracks how many permits we are using to gather metadata
    file_maps: HashMap<usize, Arc<SharedFileMap>>, // Tracks file maps to get memory mapped files
    ui_sender: UnboundedSender<UiStateEvent>,
    db_manager: StateManager,
    permits_needed: Arc<AtomicUsize>,
}

impl SupervisorState {
    fn new(client: Client, download: Download, host_sender: UnboundedSender<HostMessage>, ui_sender: UnboundedSender<UiStateEvent>, db_manager: StateManager) -> Self {
        let files = download.files.iter().filter(|(_, file)| match file {
            DownloadType::File(_) => true,
            DownloadType::Folder(_) => false,
        }).count();

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
            active_permits: 0,
            active_downloads: 0,
            active_metadata_requests: 0,
            file_maps: HashMap::new(),
            ui_sender,
            db_manager,
            permits_needed: Arc::new(files.into()),
        }
    }

    // Gets the next job the supervisor should perform. It can either be a file download, 
    // or gathering the metadata from a file whose size is still unknown
    fn get_next_job(&mut self) -> Option<Job> {
        // Check for files that need sizes
        let next_metadata_id = self.get_next_uninitialized_file();

        let next_range = self.get_next_range();

        let next_stream = self.get_next_stream();

        let can_download_files = next_range.is_some() || next_stream.is_some();

        if let Some(next_metadata_id) = next_metadata_id {
            // if we hold only one permit, prioritize downloading metadata 
            // likewise if we still have unintialized file, initialize them by getting their metadata
            if self.active_permits == 1 || !can_download_files {
                return Some(self.take_metadata_job(next_metadata_id));
            }
        }

        // we prefer to file downloads over metadata if less than half of the active permits are being used
        // this way, we can gather the metadata with the permits left
        let prefer_downloads = self.active_downloads < (self.active_permits / 2);

        if prefer_downloads {
            if can_download_files {
                match self.take_download_job(next_range, next_stream) {
                    Some(job) => return Some(job),
                    None => (),
                }
            } else if let Some(next_metadata_id) = next_metadata_id {
                return Some(self.take_metadata_job(next_metadata_id));
            }
        } else {
            if let Some(next_metadata_id) = next_metadata_id {
                return Some(self.take_metadata_job(next_metadata_id));
            } else if can_download_files {
                match self.take_download_job(next_range, next_stream) {
                    Some(job) => return Some(job),
                    None => (),
                }
            } 
        }

        None
    }

    /// Gets a metadata job and automatically updates its cursor as needed
    fn take_metadata_job(&mut self, id: usize) -> Job {
        if id >= self.uninitialized_cursor {
            self.uninitialized_cursor = id + 1;
        }
        Job::GetSize(id)
    }

    // Gets a download job and automatically updates the appropriate cursor as needed
    // Prioritizes range downloads as these can be parallelized
    fn take_download_job(&mut self, range: Option<(usize, (usize, usize))>, stream: Option<usize>) -> Option<Job> {
        match (range, stream) {
            (Some((file_id, range)), _) => {
                self.chunk_cursors.insert(file_id, range.1);
                Some(Job::DownloadChunk(file_id, range))
            }
            (None, Some(stream_id)) => {
                self.streams_cursor += 1;
                Some(Job::DownloadStream(stream_id))
            }
            _ => None,
        }
    }

    fn get_next_stream(&mut self) -> Option<usize> {
        if let Some(file_id) = self.retry_streams.pop() {
            return Some(file_id);
        }

        // leverage the fact that ids are continuous
        let last_id = *self.download.files().keys().max().unwrap_or(&0);

        let cursor = self.streams_cursor;

        for file_id in cursor..=last_id {
            if let Some(DownloadType::File(file)) = self.download.files().get(&file_id) 
                && file.size() == Some(FileSize::Unknown) {
                    return Some(file_id);
            }
        }

        None
    }

    fn get_next_uninitialized_file(&mut self) -> Option<usize> {
        if let Some(file_id) = self.retry_uninitialized.pop() {
            return Some(file_id);
        }

        if self.download.files().is_empty() {
            return None;
        }

        // get the cursor of the file, if no cursor exists in the map, then insert one and return 0
        let cursor = self.uninitialized_cursor;

        // leverages the fact that ids are continuous and always start from 0
        let last_id = *self.download.files().keys().max().unwrap_or(&0);

        for index in cursor..=last_id {
            if let Some(DownloadType::File(file)) = self.download.files().get(&index) {
                if file.size() == None {
                    return Some(index);
                }
            }
        }
        
        None
    }

    fn get_next_range(&mut self) -> Option<(usize, (usize, usize))> {
        // leverage the fact that ids are continuous
        let last_id = *self.download.files().keys().max().unwrap_or(&0);

        for file_id in 0..=last_id {
            // skip files that are already completed 
            if let Some(crate::download::DownloadType::File(f)) = self.download.files().get(&file_id) {
                if f.status() == DownloadStatus::Completed {
                    continue;
                }
            }

            // Check for retries first on this file
            if let Some(retry_range) = self.retry_ranges.get_mut(&file_id) {
                if !retry_range.is_empty() {
                    return retry_range.pop().map(|range| (file_id, range));
                }
            }

            // get cursor for this particular file
            let cursor = *self.chunk_cursors.entry(file_id).or_insert(0);

            let chunks = match self.download.files().get(&file_id).unwrap() {
                crate::download::DownloadType::File(file_download) => file_download.chunks(),
                crate::download::DownloadType::Folder(_) => continue,
            };

            // This means that the metadata is still not fetched, so we can skip it
            if chunks.is_empty() {
                continue;
            }

            // Try to find an undownloaded chunk
            if let Some(relative_start) = chunks[cursor..].first_zero() {
                let start_index = relative_start + cursor;
                
                let target_chunk_size = 5242880 / 16384; // 5 MB

                let mut index = 0;
                for bit in &chunks[start_index..] {
                    if *bit == true || index >= target_chunk_size {
                        break;
                    }
                    index += 1;
                }

                let end_index = start_index + index;

                return Some((file_id, (start_index, end_index)));
            }
        }

        None
    }
}

pub struct DownloadSupervisor {
    state: Option<SupervisorState>,
    sender: Option<UnboundedSender<SupervisorMessage>>,
    shutdown_receiver: Option<oneshot::Receiver<SupervisorState>>,
    saturated: Arc<AtomicBool>,
}

impl DownloadSupervisor {
    pub fn new(client: Client, download: Download, host_sender: UnboundedSender<HostMessage>, ui_sender: UnboundedSender<UiStateEvent>, db_manager: StateManager) -> Self {
        println!("Supervisor created for: {}", download.name());

        Self {
            state: Some(SupervisorState::new(client, download, host_sender, ui_sender, db_manager)),
            sender: None,
            shutdown_receiver: None,
            saturated: Arc::new(false.into()),
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
                                SupervisorMessage::SpawnWorker(permit) => {
                                    println!("spawning worker for download: {}", state.download.name());
                                    state.active_permits += 1;

                                    let sender = sender.clone();

                                    // no next job means either we are finished or all remaining jobs are already taken
                                    // in any case, we send the permit back to the host
                                    let job = match state.get_next_job() {
                                        Some(job) => {
                                            println!("found job {:?} for download: {}", job, state.download.name());
                                            job
                                        },
                                        None => {
                                            println!("no jobs left");
                                            state.active_permits -= 1;

                                            // We tell the host manager that we are saturated so it doesn't try to send more permits
                                            saturated.store(true, Ordering::Relaxed);
                        
                                            // We drop the permit so it gets sent back to the host manager
                                            drop(permit);

                                            // check if there is no more work to do
                                            if state.active_permits == 0 && state.active_downloads == 0 {
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
                                        Job::GetSize(file_id) => {
                                            println!("getting size for download: {}", state.download.name());
                                            state.active_metadata_requests += 1;
                                            let url = match state.download.files().get(&file_id).unwrap() {
                                                DownloadType::File(file_download) => file_download.url().clone(),
                                                DownloadType::Folder(_) => todo!(),
                                            };
                                            let host = state.download.host();
                                            let client = state.client.clone();

                                            tokio::spawn(async move {  
                                                let size_result = fetch_file_size(host, &client, &url).await;

                                                let _ = sender.send(SupervisorMessage::MetadataFetched(permit, file_id, size_result));
                                            });
                                        },
                                        Job::DownloadChunk(file_id, range) => {
                                            state.active_downloads += 1;

                                            let file_download = match state.download.files().get(&file_id).unwrap() {
                                                DownloadType::File(file_download) => file_download,
                                                DownloadType::Folder(_) => todo!(),
                                            };

                                            let client = state.client.clone();
                                            let host = state.download.host();
                                            let url = file_download.url().clone();
                                            let path = file_download.relative_path().clone();


                                            if !file_download.relative_path().exists() {
                                                if let Some(parent_path) = path.parent() {
                                                    create_dir_all(parent_path).await.unwrap();
                                                }

                                                let file = tokio::fs::File::create(&path).await.unwrap();

                                                let size = match file_download.size() {
                                                    Some(FileSize::Known(size)) => size,
                                                    Some(FileSize::Unknown) => {
                                                        eprintln!("Tried to download file with unknown size by chunks!");
                                                        continue;
                                                    }
                                                    None => {
                                                        eprintln!("Tried to download file with no size!");
                                                        continue;
                                                    },
                                                };

                                                file.set_len(size).await.unwrap(); 
                                            }
                                            
                                            let file_map = if state.file_maps.contains_key(&file_id) {
                                                state.file_maps.get(&file_id).unwrap().clone()
                                            } else {
                                                let size = match file_download.size() {
                                                    Some(FileSize::Known(size)) => size,
                                                    Some(FileSize::Unknown) => {
                                                        eprintln!("Tried to download a chunk of a file with unknown size");
                                                        continue;
                                                    },
                                                    None => {
                                                        eprintln!("Tried to download a chunk of a file with unresolved size");
                                                        continue;
                                                    },
                                                };

                                                state.file_maps.insert(file_id, Arc::new(SharedFileMap::new(&path, size)));
                                                state.file_maps.get(&file_id).unwrap().clone()
                                            };

                                            let chunk_size = 16384u64;
                                            let start_byte = range.0 as u64 * chunk_size;
                                            let theoretical_end = range.1 as u64 * chunk_size;

                                            let total_size = match state.download.files().get(&file_id).unwrap() {
                                                DownloadType::File(file) => match file.size() {
                                                    Some(FileSize::Known(size)) => size,
                                                    Some(FileSize::Unknown) => {
                                                        eprintln!("Tried to download file with unknown size by chunks!");
                                                        continue;
                                                    }
                                                    None => {
                                                        eprintln!("Tried to download file with no size!");
                                                        continue;
                                                    },
                                                },
                                                _ => 0,
                                            };

                                            println!("id: {} total size: {}", file_id, total_size);

                                            let actual_end = std::cmp::min(theoretical_end, total_size);
                                            let expected_len = actual_end.saturating_sub(start_byte);

                                            println!("id: {} expected_len {}", file_id, expected_len);

                                            tokio::spawn(async move {
                                                // Do worker stuff
                                                let success = download_range(client, host, &url, range, file_map, expected_len.min(total_size)).await;

                                                let _ = sender.send(SupervisorMessage::WorkerFinished(permit, file_id, range, success));
                                            });
                                        },
                                        Job::DownloadStream(file_id) => {
                                            state.active_downloads += 1;
                                            let client = state.client.clone();

                                            let file_download = match state.download.files.get(&file_id).unwrap() {
                                                DownloadType::File(file_download) => {
                                                    file_download
                                                },
                                                DownloadType::Folder(_) => todo!(),
                                            };

                                            let url = file_download.url().clone();
                                            let path = file_download.relative_path().clone();
                                            
                                            tokio::spawn(async move {
                                                // Do worker stuff
                                                let result = download_stream(client, path, &url).await;

                                                let _ = sender.send(SupervisorMessage::StreamFinished(permit, file_id, result));
                                            });
                                        }
                                    }
                                }
                                SupervisorMessage::StreamFinished(permit, file_id, result) => {
                                    state.active_downloads -= 1;

                                    if let Ok(size) = result {
                                        let download_id = state.download.id();
                                        let chunk_size = 16384;

                                        match state.download.files_mut().get_mut(&file_id).unwrap() {
                                            DownloadType::File(file) => {
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
                                    } else {
                                        state.retry_streams.push(file_id);
                                    }

                                    state.active_permits -= 1;

                                    // try to spawn another worked
                                    let _ = sender.send(SupervisorMessage::SpawnWorker(permit));

                                },
                                SupervisorMessage::WorkerFinished(permit, file_id, range, success) => {
                                    state.active_downloads -= 1;

                                    if success {
                                        let download_id = state.download.id();

                                        let chunk_size = 16384;
                                        
                                        match state.download.files_mut().get_mut(&file_id).unwrap() {
                                            crate::download::DownloadType::File(file_download) => {
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

                                                // assume all chunks are done, if a chunk is false, mark this to false too
                                                let mut all_chunks_done = true; 

                                                // TODO: change this logic to something that doesn't iterate over every single chunk
                                                for (i, mut chunk) in file_download.chunks_mut().iter_mut().enumerate() {
                                                    // mark all chunks of the range as finished
                                                    if i >= range.0 && i < range.1 {
                                                        *chunk = true;
                                                    }
                                                    if !*chunk {
                                                        all_chunks_done = false;
                                                    }
                                                }

                                                if all_chunks_done {
                                                    println!("file {} finished! got {} bytes", name, bytes_downloaded);
                                                    let _ = state.ui_sender.send(UiStateEvent::AddUpdate(DownloadUpdate::FileUpdated { id: download_id, file_update: FileUpdate::Status { id: file_id, status: DownloadStatus::Completed } }));
                                                    file_download.set_status(DownloadStatus::Completed);
                                                    state.file_maps.remove(&file_id);
                                                }
                                            },
                                            crate::download::DownloadType::Folder(_) => todo!(),
                                        } 
                                    } else {
                                        state.retry_ranges.entry(file_id).or_default().push(range);
                                    }


                                    // mark the permit as inactive, but still owned
                                    // this has to happen after marking all chunks as finished to avoid a race condition
                                    // otherwise spawn_worker might kill the loop before this finishes
                                    state.active_permits -= 1;

                                    // try to spawn another worked
                                    let _ = sender.send(SupervisorMessage::SpawnWorker(permit));
                                },
                                SupervisorMessage::MetadataFetched(permit, file_id, size_result) => {
                                    println!("got metadata for: {} {}", state.download.name(), file_id);
                                    state.active_metadata_requests -= 1;

                                    match size_result {
                                        SizeResult::Known(size) => {
                                            let download_id = state.download.id();

                                            if let Some(DownloadType::File(file)) = state.download.files_mut().get_mut(&file_id) {
                                                file.set_size(FileSize::Known(size));
                                                let _ = state.ui_sender.send(UiStateEvent::AddUpdate(DownloadUpdate::FileUpdated { id: download_id, file_update: FileUpdate::FileSize { id: file_id, len: size } }));
                                                
                                                // todo, make this a global or store it somewhere
                                                let chunk_size = 16384; // 16 KB
                                                let range_size = 5242880 / 16384; // 5 MB

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
                                            if let Some(DownloadType::File(file)) = state.download.files_mut().get_mut(&file_id) {
                                                file.set_size(FileSize::Unknown);
                                            }
                                        },
                                        SizeResult::Retryable(_) => {
                                            state.retry_uninitialized.push(file_id);
                                        },
                                        SizeResult::PermanentFail => todo!(),
                                    }

                                    state.active_permits -= 1; 
                                    let _ = sender.send(SupervisorMessage::SpawnWorker(permit));
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

        let _ = self.sender.as_ref().unwrap().send(SupervisorMessage::SpawnWorker(permit));
    }

    pub fn is_saturated(&self) -> bool {
        self.saturated.load(Ordering::Acquire).into()
    }

    pub fn set_saturated(&mut self, saturated: bool) {
        self.saturated.store(saturated, Ordering::Relaxed); 
    }
}

async fn fetch_file_size(host: Host, client: &reqwest::Client, url: &str) -> SizeResult {
    // Try a HEAD request first
    let head_result = client.head(url)
        .headers(host.headers())
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
        .headers(host.headers())
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
                        if let Some(total_size) = parse_content_range(str) {
                            println!("parsed correctly!");
                            return SizeResult::Known(total_size);
                        }
                    }
                }
            }
            StatusCode::OK => {
                if let Some(len) = response.content_length() {
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

async fn download_range(client: Client, host: Host, url: &str, range: (usize, usize), file_map: Arc<SharedFileMap>, expected_len: u64) -> bool {
    let chunk_size = 16384;

    let start_byte = range.0 as u64 * chunk_size;
    let end_byte = (range.1 as u64 * chunk_size) - 1; // -1 because http ranges are inclusive

    let range_header = format!("bytes={}-{}", start_byte, end_byte);

    let request = client.get(url)
        .headers(host.headers())
        .header("Range", range_header);

    let result = async {
        let mut response = request.send().await.unwrap();

        if response.status() != StatusCode::PARTIAL_CONTENT && response.status() != StatusCode::OK {
            return Err(format!("Server wrong returned status: {}", response.status()));
        }

        let mut current_offset = start_byte;
        let mut bytes_received = 0; 

        while let Some(chunk) = response.chunk().await.unwrap() {
            let chunk_len = chunk.len() as u64;
            file_map.write_chunk(current_offset as usize, &chunk);

            current_offset += chunk_len;
            bytes_received += chunk_len;
        }

        if bytes_received != expected_len {
            return Err(format!("Incomplete download: expected {} bytes, got {}", expected_len, bytes_received));
        }
            
        Ok(())
    }.await;

    let success = result.is_ok();

    if let Err(error) = result {
        println!("Download worker failed: {}", error);
    }

    success
}

async fn download_stream(client: Client, path: PathBuf, url: &str) -> Result<usize, ()> {
    println!("downloading stream: {}", url);
    let mut response = client.get(url)
        .send()
        .await
        .unwrap();

    if let Some(parent_path) = path.parent() {
        create_dir_all(parent_path).await.unwrap();
    }

    let file = tokio::fs::File::create(&path).await.unwrap();

    let mut writer = BufWriter::new(file);
    let mut size = 0;

    while let Some(chunk) = response.chunk().await.unwrap() {
        size += chunk.len();
        writer.write_all(&chunk).await.unwrap();
    }

    writer.flush().await.unwrap();

    Ok(size)
}