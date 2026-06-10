use std::str::FromStr;

use serde::{Deserialize, Serialize};
use strum::EnumCount;
use strum_macros::{EnumCount, EnumDiscriminants, EnumString, IntoStaticStr};

use crate::download::error::{DownloadFailureReason, FileFailureReason};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, IntoStaticStr, EnumDiscriminants, Default)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "state", content = "value")]
#[strum(serialize_all = "snake_case")]
#[strum_discriminants(derive(EnumString, IntoStaticStr))]
#[strum_discriminants(name(DownloadStatusParse))] 
#[strum_discriminants(strum(serialize_all = "snake_case"))]
pub enum DownloadStatus {
    #[default]
    Uninitialized,
    MetadataFetched,
    Partial,
    NotFound,
    Completed,
    Failed(DownloadFailureReason),
    CompletedWithErrors,
}

impl DownloadStatus {
    pub fn from_db_columns(status: &str, failure_reason: Option<&str>) -> Option<Self> {
        if let Some(reason_str) = failure_reason {
            let reason = DownloadFailureReason::from_db_string(reason_str).unwrap_or_default();
            return Some(Self::Failed(reason));
        }

        // If we fail to deserialize, we fallback to Queued
        let parsed_state = DownloadStatusParse::from_str(status).ok()?;

        Some(match parsed_state {
            DownloadStatusParse::Uninitialized => DownloadStatus::Uninitialized,
            DownloadStatusParse::MetadataFetched => DownloadStatus::MetadataFetched,
            DownloadStatusParse::Partial => DownloadStatus::Partial,
            DownloadStatusParse::NotFound => DownloadStatus::NotFound,
            DownloadStatusParse::Completed => DownloadStatus::Completed,
            DownloadStatusParse::CompletedWithErrors => DownloadStatus::CompletedWithErrors,
            
            // Fallback if for some reason we still get Failed here
            DownloadStatusParse::Failed => return None,
        })
    }

    pub fn bucket(&self) -> StatusBucket {
        match self {
            DownloadStatus::Uninitialized => StatusBucket::Uninitialized,
            DownloadStatus::MetadataFetched => StatusBucket::MetadataFetched,
            DownloadStatus::Partial => StatusBucket::Partial,
            DownloadStatus::Completed => StatusBucket::Completed,
            DownloadStatus::NotFound |
            DownloadStatus::Failed(_) => StatusBucket::Error,
            DownloadStatus::CompletedWithErrors => StatusBucket::CompletedWithErrors,
        }
    }

        pub fn to_db_columns(&self) -> (&'static str, Option<&'static str>) {
        let status_str: &'static str = self.into(); 

        // Enum variants that contain extra information need to be extracted
        match self {
            DownloadStatus::Failed(reason) => {
                let reason_str: &'static str = reason.into();

                (status_str, Some(reason_str))
            }
            DownloadStatus::Uninitialized |
            DownloadStatus::MetadataFetched |
            DownloadStatus::Partial |
            DownloadStatus::NotFound |
            DownloadStatus::Completed |
            DownloadStatus::CompletedWithErrors => (status_str, None),
        }
    }
}

// EnumCount can be changed to std::mem::variant_count whenever it stabilizes its const api
#[derive(Debug, Clone, Copy, EnumCount, PartialEq)]
#[repr(usize)] // This allows us to use each enum as an index in an array
pub enum StatusBucket {
    Uninitialized,
    MetadataFetched,
    Partial,
    Completed,
    Error,
    CompletedWithErrors,
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
        self.data[bucket as usize] = self.data[bucket as usize].saturating_sub(1);
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, IntoStaticStr, EnumDiscriminants, Default)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "state", content = "value")]
#[strum(serialize_all = "snake_case")]
#[strum_discriminants(derive(EnumString, IntoStaticStr))]
#[strum_discriminants(name(FileStatusParse))] 
#[strum_discriminants(strum(serialize_all = "snake_case"))]
// These variants are mutually exclusive, because of this, states like
// paused shouldn't live in here as they will overwrite the current status of the file.
pub enum FileStatus {
    #[default]
    Uninitialized,
    MetadataFetched,
    Partial,
    Completed,
    Failed(FileFailureReason),
    NotFound,
}

impl FileStatus {
    /// This function exists because certain states like completed shouldn't be able to transition to queued automatically
    pub fn can_set_to_queued(&self) -> bool {
        match self {
            Self::Completed | 
            Self::NotFound  => false,

            Self::Uninitialized |
            Self::MetadataFetched |
            Self::Partial |
            Self::Failed(_)  => true,
        }
    }

    pub fn can_be_failed(&self) -> bool {
        match self {
            Self::Uninitialized |
            Self::Completed | 
            Self::NotFound | 
            Self::Failed(_) => false,

            Self::MetadataFetched |
            Self::Partial => true,
        }
    }

    pub fn bucket(&self) -> StatusBucket {
        match self {
            Self::Uninitialized => StatusBucket::Uninitialized,
            Self::MetadataFetched => StatusBucket::MetadataFetched,
            Self::Partial => StatusBucket::Partial,
            Self::Completed => StatusBucket::Completed,
            Self::NotFound |
            Self::Failed(_) => StatusBucket::Error,
        }
    }

    pub fn is_terminal(&self) -> bool {
        match self {
            FileStatus::Uninitialized |
            FileStatus::MetadataFetched |
            FileStatus::Partial => false,
            FileStatus::Completed |
            FileStatus::NotFound |
            FileStatus::Failed(_) => true,
        }
    }

    pub fn from_db_columns(status: &str, file_failure_reason: Option<&str>) -> Option<Self> {
        if let Some(file_failure_reason) = file_failure_reason {
            let inner_reason = FileFailureReason::from_str(file_failure_reason).unwrap_or_default();
            return Some(Self::Failed(inner_reason));
        }

        let parsed_reason = FileStatusParse::from_str(status).ok()?;

        Some(match parsed_reason {
            FileStatusParse::Uninitialized => Self::Uninitialized,
            FileStatusParse::MetadataFetched => Self::MetadataFetched,
            FileStatusParse::Partial => Self::Partial,
            FileStatusParse::Completed => Self::Completed,
            FileStatusParse::NotFound => Self::NotFound,
            
            // Fallback if for some reason we still get here
            FileStatusParse::Failed  => return None,
        })
    }

    pub fn to_db_columns(&self) -> (&'static str, Option<&'static str>) {
        let status_str: &'static str = self.into(); 

        // Enum variants that contain extra information need to be extracted
        match self {
            FileStatus::Failed(reason) => {
                let reason_str: &'static str = reason.into();

                (status_str, Some(reason_str))
            }

            FileStatus::Uninitialized |
            FileStatus::MetadataFetched |
            FileStatus::Partial |
            FileStatus::NotFound |
            FileStatus::Completed => (status_str, None),
        }
    }

    pub fn as_download_status(&self) -> DownloadStatus {
        match self {
            FileStatus::Uninitialized => DownloadStatus::Uninitialized,
            FileStatus::MetadataFetched => DownloadStatus::MetadataFetched,
            FileStatus::Partial => DownloadStatus::Partial,
            FileStatus::Completed => DownloadStatus::Completed,
            FileStatus::NotFound => DownloadStatus::NotFound,

            FileStatus::Failed(reason) => {
                DownloadStatus::Failed(DownloadFailureReason::AllFilesFailed(*reason))
            }
        }
    }
}
