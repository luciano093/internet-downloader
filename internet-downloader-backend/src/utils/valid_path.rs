use std::ops::Deref;
use std::path::{Component, Path, PathBuf};

use os_str_bytes::OsStrBytesExt;
use thiserror::Error;

use crate::utils::file_utils::{InvalidFilename, is_valid_file_name};

#[derive(Debug, Error, Clone)]
pub enum InvalidPath {
    #[error("The path can not be empty")]
    Empty,
    #[error("The filename can not contain a null byte")]
    ContainsNullByte,
    #[error(transparent)]
    InvalidFilename(#[from] InvalidFilename),
}

pub struct ValidPath(PathBuf);

impl ValidPath {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, InvalidPath> {
        let path = path.as_ref();
        
        if path.as_os_str().is_empty() {
            return Err(InvalidPath::Empty);
        }

        // Null byte check
        if path.as_os_str().contains('\0') {
            return Err(InvalidPath::ContainsNullByte);
        }

        for component in path.components() {
            match component {
                // This catches drive prefixes ("C:"), UNC shares ("\\server\share"), root slashes ("/").
                // and relative path navigators ("." / "..") all or which are valid in a folder path
                Component::Prefix(_) | Component::RootDir | Component::CurDir | Component::ParentDir => continue,
                Component::Normal(os_str) => {
                    is_valid_file_name(os_str)?;
                }
            }
        }
        
        Ok(Self(path.to_path_buf()))
    }
}

impl Deref for ValidPath {
    type Target = Path;
    fn deref(&self) -> &Self::Target {
        self.0.as_path()
    }
}

impl AsRef<Path> for ValidPath {
    fn as_ref(&self) -> &Path {
        self.0.as_path()
    }
}
