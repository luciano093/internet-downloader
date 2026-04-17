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

    pub fn bucket(&self) -> StateBucket {
        match self {
            Self::Queued => StateBucket::Queued,
            Self::Initializing => StateBucket::Initializing,
            Self::FetchingMetadata => StateBucket::FetchingMetadata,
            Self::InProgress => StateBucket::InProgress,
            Self::Retrying => StateBucket::Retrying,
            Self::Waiting => StateBucket::Waiting,

            Self::Completed => StateBucket::Completed,

            Self::CompletedWithErrors => StateBucket::CompletedWithErrors,

            Self::Failed(_) | 
            Self::NotFound => StateBucket::Error,
            
            Self::Paused => StateBucket::Paused,
        }
    }
}

// EnumCount can be changed to std::mem::variant_count whenever it stabilizes its const api
#[derive(Debug, Clone, Copy, EnumCount, PartialEq)]
#[repr(usize)] // This allows us to use each enum as an index in an array
pub enum StateBucket {
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

const BUCKET_COUNT: usize = StateBucket::COUNT;

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

    pub fn increment(&mut self, bucket: StateBucket) {
        self.data[bucket as usize] += 1;
    }

    pub fn decrement(&mut self, bucket: StateBucket) {
        let _ = self.data[bucket as usize].saturating_sub(1);
    }

    pub fn get(&self, bucket: StateBucket) -> usize {
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

    pub fn bucket(&self) -> StateBucket {
        match self {
            Self::Queued => StateBucket::Queued,
            Self::Initializing => StateBucket::Initializing,
            Self::FetchingMetadata => StateBucket::FetchingMetadata,
            Self::InProgress => StateBucket::InProgress,
            Self::Retrying => StateBucket::Retrying,
            Self::Waiting(_) => StateBucket::Waiting,

            Self::Completed => StateBucket::Completed,

            Self::Failed(_) | 
            Self::NotFound => StateBucket::Error,
            
            Self::Paused => StateBucket::Paused,
        }
    }
}