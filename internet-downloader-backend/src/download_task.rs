use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use reqwest::{Client, StatusCode, header};
use tokio::fs::create_dir_all;
use tokio::sync::oneshot;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use crate::client_state_manager::UiStateEvent;
use crate::download::hosts::Host;
use crate::download::{Download, DownloadItem, DownloadStatus, DownloadType, DownloadUpdate, FileSize, FileUpdate};
use crate::host_manager::{ActiveDownloadPermit, HostMessage};
use crate::shared_file_map::SharedFileMap;
use crate::state_manager::StateManager;

pub enum SupervisorMessage {
    SpawnWorker(ActiveDownloadPermit),
    WorkerFinished(ActiveDownloadPermit, usize, (usize, usize), bool), // permit, id, range, success (false if failed)
    MetadataFetched(ActiveDownloadPermit, usize, Option<u64>), 
}

#[derive(Debug)]
pub enum Job {
    GetSize(usize),
    DownloadChunk(usize, (usize, usize)),
}

struct SupervisorState {
    client: Client,
    download: Download,
    chunk_cursors: HashMap<usize, usize>, // used to keep track of last tracked chunk in a file to avoid looping through all the chunks every time
    uninitialized_cursor: usize, // track the last known initialized file
    retry_ranges: HashMap<usize, Vec<(usize, usize)>>, // ranges that failed but are still buffered
    retry_uninitialized: Vec<usize>, // tracks the files that failed to get metadata
    host_sender: UnboundedSender<HostMessage>,
    active_permits: usize, 
    max_permits: usize, 
    active_downloads: usize, // tracks how many permits we are using to download files
    active_metadata_requests: usize, // tracks how many permits we are using to gather metadata
    file_maps: HashMap<usize, Arc<SharedFileMap>>, // Tracks file maps to get memory mapped files
    ui_sender: UnboundedSender<UiStateEvent>,
    db_manager: StateManager,
}

impl SupervisorState {
    fn new(client: Client, download: Download, host_sender: UnboundedSender<HostMessage>, ui_sender: UnboundedSender<UiStateEvent>, db_manager: StateManager) -> Self {
        Self { 
            client,
            download,
            chunk_cursors: HashMap::new(),
            uninitialized_cursor: 0,
            retry_ranges: HashMap::new(),
            retry_uninitialized: Vec::new(),
            host_sender,
            active_permits: 0,
            max_permits: 4,
            active_downloads: 0,
            active_metadata_requests: 0,
            file_maps: HashMap::new(),
            ui_sender,
            db_manager,
        }
    }

    // Gets the next job the supervisor should perform. It can either be a file download, 
    // or gathering the metadata from a file whose size is still unknown
    fn get_next_job(&mut self) -> Option<Job> {
        // Check for files that need sizes
        let next_metadata_id = self.get_next_uninitialized_file();

        // Check for a chunk range that can already be downloaded
        let next_range = self.get_next_range();

        match (next_metadata_id, next_range) {
            // We have both a file that needs metadata resolution and a file that can already start to be downloaded
            (Some(next_metadata_id), Some((file_id, range))) => {
                // TODO: This implementation should probably be changed in the future to optimize for concurrent downloads

                // Prioritize downloads
                if self.active_downloads == 0 {
                    self.chunk_cursors.insert(file_id, range.1);
                    return Some(Job::DownloadChunk(file_id, range));
                }

                if next_metadata_id >= self.uninitialized_cursor {
                    self.uninitialized_cursor = next_metadata_id + 1;
                }

                Some(Job::GetSize(next_metadata_id))
            },
            // We only have files that needs metadta resolution
            (Some(next_metadata_id), None) => {
                if next_metadata_id >= self.uninitialized_cursor {
                    self.uninitialized_cursor = next_metadata_id + 1;
                }

                Some(Job::GetSize(next_metadata_id))
            },
            // All metadata is gathered, so we only have files to download
            (None, Some((file_id, range))) => {

                self.chunk_cursors.insert(file_id, range.1);
                Some(Job::DownloadChunk(file_id, range))
            },
            (None, None) => None,
        }
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
                if file.size() == FileSize::Unknown {
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
                
                // Update cursor for next time so we don't scan the start of this file again
                self.chunk_cursors.insert(file_id, end_index);

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
    saturated: bool,
}

impl DownloadSupervisor {
    pub fn new(client: Client, download: Download, host_sender: UnboundedSender<HostMessage>, ui_sender: UnboundedSender<UiStateEvent>, db_manager: StateManager) -> Self {
        println!("Supervisor created for: {}", download.name());

        Self {
            state: Some(SupervisorState::new(client, download, host_sender, ui_sender, db_manager)),
            sender: None,
            shutdown_receiver: None,
            saturated: false,
        }
    }

    fn spawn(&mut self, sender: UnboundedSender<SupervisorMessage>, mut receiver: UnboundedReceiver<SupervisorMessage>) {
        let mut state = self.state.take();
        let mut previous_shutdown_receiver = self.shutdown_receiver.take();

        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        self.shutdown_receiver = Some(shutdown_receiver);

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
                                    // If we are already at max permits, don't take the permit 
                                    if state.active_permits >= state.max_permits {
                                        HostMessage::SupervisorSaturated(state.download.id());
                                        drop(permit);
                                        continue;
                                    }

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
                                            HostMessage::SupervisorSaturated(state.download.id());
                        
                                            // We drop the permit so it gets sent back to the host manager
                                            drop(permit);

                                            // no more work to do
                                            if state.active_permits == 0 && state.active_downloads == 0 {
                                                state.download.set_status(DownloadStatus::Completed);
                                                let _ = state.host_sender.send(HostMessage::DownloadFinished(state.download.id()));
                                                break;
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
                                                let size = fetch_file_size(host, &client, &url).await;
                                                
                                                let _ = sender.send(SupervisorMessage::MetadataFetched(permit, file_id, size));
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
                                                    FileSize::Unknown => todo!(),
                                                    FileSize::Known(size) => size,
                                                };

                                                file.set_len(size).await.unwrap(); 
                                            }
                                            
                                            let file_map = if state.file_maps.contains_key(&file_id) {
                                                state.file_maps.get(&file_id).unwrap().clone()
                                            } else {
                                                let size = match file_download.size() {
                                                    FileSize::Unknown => todo!(),
                                                    FileSize::Known(size) => size,
                                                };

                                                state.file_maps.insert(file_id, Arc::new(SharedFileMap::new(&path, size)));
                                                state.file_maps.get(&file_id).unwrap().clone()
                                            };


                                            tokio::spawn(async move {
                                                // Do worker stuff
                                                download_range(client, host, &url, range, file_map).await;

                                                let _ = sender.send(SupervisorMessage::WorkerFinished(permit, file_id, range, true));
                                            });
                                        },
                                    }
                                }
                                SupervisorMessage::WorkerFinished(permit, file_id, range, success) => {
                                    state.active_downloads -= 1;

                                    if success {
                                        let download_id = state.download.id();

                                        let chunk_size = 16384;

                                        let start_byte = range.0 * chunk_size;
                                        let end_byte = (range.1 * chunk_size) - 1; // -1 because http ranges are inclusive
                                        
                                        match state.download.files_mut().get_mut(&file_id).unwrap() {
                                            crate::download::DownloadType::File(file_download) => {
                                                let bytes_downloaded = file_download.bytes_downloaded() + (end_byte - start_byte) as u64;
                                                file_download.set_bytes_downloaded(bytes_downloaded);

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
                                SupervisorMessage::MetadataFetched(permit, file_id, size) => {
                                    println!("got metadata for: {}", state.download.name());
                                    state.active_metadata_requests -= 1;

                                    if let Some(size) = size {
                                        let download_id = state.download.id();

                                        if let Some(DownloadType::File(file)) = state.download.files_mut().get_mut(&file_id) {
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
                                    } else {
                                        state.retry_uninitialized.push(file_id);
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

    pub const fn is_saturated(&self) -> bool {
        self.saturated
    }

    pub fn set_saturated(&mut self, saturated: bool) {
        self.saturated = saturated
    }
}

async fn fetch_file_size(host: Host, client: &reqwest::Client, url: &str) -> Option<u64> {
    // Try a HEAD request first
    let head_result = client.head(url)
        .headers(host.headers())
        .header("Accept-Encoding", "identity")
        .send()
        .await;

    if let Ok(response) = head_result {
        if let Some(len) = response.content_length() && response.status().is_success() {
            if len != 0 {
                return Some(len);
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
            if response.status() == StatusCode::PARTIAL_CONTENT {
                if let Some(range_header) = response.headers().get(header::CONTENT_RANGE) {
                    // Helper to parse "bytes 0-0/12345" -> 12345
                    if let Ok(str) = range_header.to_str() {
                        if let Some(total_size) = parse_content_range(str) {
                            return Some(total_size);
                        }
                    }
                }
            }
        }

    None
}

fn parse_content_range(range_header: &str) -> Option<u64> {
     // e.g. "bytes 0-0/1048576"
    range_header.rsplit('/').next()?.parse::<u64>().ok()
}

async fn download_range(client: Client, host: Host, url: &str, range: (usize, usize), file_map: Arc<SharedFileMap>) -> bool {
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
            return Err(format!("Server returned status: {}", response.status()));
        }

        let mut current_offset = start_byte;

        while let Some(chunk) = response.chunk().await.unwrap() {
            file_map.write_chunk(current_offset as usize, &chunk);

            current_offset += chunk.len() as u64;
        }
            
        Ok(())
    }.await;

    let success = result.is_ok();

    if let Err(error) = result {
        eprintln!("Download worker failed: {}", error);
    }

    success
}