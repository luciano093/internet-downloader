use serde::{Deserialize, Serialize};
use strum::EnumCount;
use strum_macros::EnumCount;

use crate::download::{DownloadFailureReason, FileFailureReason};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "state", content = "value")]
pub enum DownloadStatus {
    Queued,
    Initializing,
    FetchingMetadata,
    InProgress,
    Completed,
    CompletedWithErrors,
    Paused,
    Failed(DownloadFailureReason),
    NotFound,
    Retrying,
    Waiting
}


impl DownloadStatus {
    /// Returns true if download is actively downloading or waiting to be downloaded.
    pub fn is_active(&self) -> bool {
        match self {
            Self::Queued | 
            Self::Initializing | 
            Self::FetchingMetadata | 
            Self::InProgress | 
            Self::Retrying | 
            Self::Waiting => true,

            Self::Completed | 
            Self::CompletedWithErrors | 
            Self::Failed(_) | 
            Self::NotFound | 
            Self::Paused => false,
        }
    }

    /// Returns true if the download is in a final state and should not be modified.
    pub fn is_inactive(&self) -> bool {
        !self.is_active()
    }

    /// This function exists because certain states like completed shouldn't be able to transition to queued automatically
    pub fn can_set_to_queue(&self) -> bool {
        match self {
            Self::Completed | 
            Self::CompletedWithErrors |
            Self::NotFound | 
            Self::Queued => false,

            Self::Paused | 
            Self::Failed(_) | 
            Self::Initializing | 
            Self::FetchingMetadata | 
            Self::InProgress | 
            Self::Retrying | 
            Self::Waiting => true,
        }
    }

    pub fn bucket(&self) -> StatusBucket {
        match self {
            Self::Queued => StatusBucket::Queued,
            Self::Initializing => StatusBucket::Initializing,
            Self::FetchingMetadata => StatusBucket::FetchingMetadata,
            Self::InProgress => StatusBucket::InProgress,
            Self::Retrying => StatusBucket::Retrying,
            Self::Waiting => StatusBucket::Waiting,

            Self::Completed => StatusBucket::Completed,

            Self::CompletedWithErrors => StatusBucket::CompletedWithErrors,

            Self::Failed(_) | 
            Self::NotFound => StatusBucket::Error,
            
            Self::Paused => StatusBucket::Paused,
        }
    }
}

// EnumCount can be changed to std::mem::variant_count whenever it stabilizes its const api
#[derive(Debug, Clone, Copy, EnumCount, PartialEq)]
#[repr(usize)] // This allows us to use each enum as an index in an array
pub enum StatusBucket {
    Queued,
    Initializing,
    FetchingMetadata,
    InProgress,
    Retrying,
    Waiting,
    Completed,
    CompletedWithErrors,
    Error,
    Paused,
}

const BUCKET_COUNT: usize = StatusBucket::COUNT;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateBucketCounters {
    data: [usize; BUCKET_COUNT],
}

impl StateBucketCounters {
    pub fn new() -> Self {
        Self {
            data: [0; BUCKET_COUNT],
        }
    }

    pub fn increment(&mut self, bucket: StatusBucket) {
        self.data[bucket as usize] += 1;
    }

    pub fn decrement(&mut self, bucket: StatusBucket) {
        let _ = self.data[bucket as usize].saturating_sub(1);
    }

    pub fn get(&self, bucket: StatusBucket) -> usize {
        self.data[bucket as usize]
    }
}

impl Default for StateBucketCounters {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "state", content = "value")]
pub enum FileStatus {
    Queued,
    Initializing,
    FetchingMetadata,
    InProgress,
    Completed,
    Paused,
    Failed(FileFailureReason),
    NotFound,
    Retrying,
    Waiting(Option<u64>)
}

impl FileStatus {
    /// Returns true if the file is actively downloading or waiting to download.
    pub fn is_active(&self) -> bool {
        match self {
            // Active states
            Self::Queued | 
            Self::Initializing | 
            Self::FetchingMetadata | 
            Self::InProgress | 
            Self::Retrying | 
            Self::Waiting(_) => true,

            // Inactive states
            Self::Completed | 
            Self::Failed(_) | 
            Self::NotFound | 
            Self::Paused => false,
        }
    }

    /// Returns true if the file is in a final state and should not be modified.
    pub fn is_inactive(&self) -> bool {
        !self.is_active()
    }

    /// This function exists because certain states like completed shouldn't be able to transition to queued automatically
    pub fn can_set_to_queue(&self) -> bool {
        match self {
            Self::Completed | 
            Self::NotFound | 
            Self::Queued => false,

            Self::Paused | 
            Self::Failed(_) | 
            Self::Initializing | 
            Self::FetchingMetadata | 
            Self::InProgress | 
            Self::Retrying | 
            Self::Waiting(_) => true,
        }
    }

    pub fn bucket(&self) -> StatusBucket {
        match self {
            Self::Queued => StatusBucket::Queued,
            Self::Initializing => StatusBucket::Initializing,
            Self::FetchingMetadata => StatusBucket::FetchingMetadata,
            Self::InProgress => StatusBucket::InProgress,
            Self::Retrying => StatusBucket::Retrying,
            Self::Waiting(_) => StatusBucket::Waiting,

            Self::Completed => StatusBucket::Completed,

            Self::Failed(_) | 
            Self::NotFound => StatusBucket::Error,
            
            Self::Paused => StatusBucket::Paused,
        }
    }
}