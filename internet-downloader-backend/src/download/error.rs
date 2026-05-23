use std::str::FromStr;

use bitvec::{order::Msb0, vec::BitVec};
use serde::{Deserialize, Serialize, Serializer};
use strum_macros::{EnumDiscriminants, EnumString, IntoStaticStr};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, IntoStaticStr, EnumString, Default)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "state", content = "value")]
#[strum(serialize_all = "snake_case")]
pub enum FileFailureReason {
    HashMismatch,
    DiskError,
    ClientError,
    ServerError,
    MetadataFetchError,
    BadPath,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, IntoStaticStr, EnumDiscriminants, Default)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "state", content = "value")]
#[strum(serialize_all = "snake_case")]
#[strum_discriminants(derive(EnumString, IntoStaticStr))]
#[strum_discriminants(name(DownloadFailureReasonParse))] 
#[strum_discriminants(strum(serialize_all = "snake_case"))]
pub enum DownloadFailureReason {
    HashMismatch,
    DiskError,
    ClientError,
    ServerError,
    MetadataFetchError,
    MultipleErrors,
    AllFilesFailed(FileFailureReason),
    FilesMissingFromDisk,
    StateDesynchronized,
    BadPath,
    #[default]
    Unknown,
}

impl DownloadFailureReason {
    pub fn from_db_string(reason_str: &str) -> Option<Self> {
        if let Some((_prefix, inner_str)) = reason_str.split_once(':') {
            let inner_reason = FileFailureReason::from_str(inner_str).ok()?;
            return Some(Self::AllFilesFailed(inner_reason));
        }
        
        let parsed_reason = DownloadFailureReasonParse::from_str(reason_str).ok()?;

        let reason = Some(match parsed_reason {
            DownloadFailureReasonParse::HashMismatch => Self::HashMismatch,
            DownloadFailureReasonParse::DiskError => Self::DiskError,
            DownloadFailureReasonParse::ClientError => Self::ClientError,
            DownloadFailureReasonParse::ServerError => Self::ServerError,
            DownloadFailureReasonParse::MetadataFetchError => Self::MetadataFetchError,
            DownloadFailureReasonParse::MultipleErrors => Self::MultipleErrors,
            DownloadFailureReasonParse::FilesMissingFromDisk => Self::FilesMissingFromDisk,
            DownloadFailureReasonParse::StateDesynchronized => Self::StateDesynchronized,
            DownloadFailureReasonParse::Unknown => Self::Unknown,
            DownloadFailureReasonParse::BadPath => Self::BadPath,
            
            // Fallback if for some reason we still get here
            DownloadFailureReasonParse::AllFilesFailed => return None,
        });

        reason
    }
}

pub fn serialize_hash<S>(hash: &Option<u128>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if serializer.is_human_readable() {
        match hash {
            Some(v) => serializer.collect_str(v),
            None => serializer.serialize_none(),
        }
    } else {
        hash.serialize(serializer)
    }
}

pub fn serialize_chunks<S>(chunks: &BitVec<u8, Msb0>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if serializer.is_human_readable() {
        serializer.serialize_none()
    } else {
        chunks.serialize(serializer)
    }
}
