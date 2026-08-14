use std::ops::Deref;
use std::path::{Component, Path, PathBuf};

use os_str_bytes::OsStrBytesExt;
use serde::Serialize;
use thiserror::Error;

use crate::utils::file_utils::{InvalidFilename, is_valid_file_name};

#[derive(Debug, Error, Clone, Serialize)]
#[serde(tag = "state", content = "value", rename_all = "snake_case")]
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

#[derive(Debug, Error, Clone, Serialize)]
#[serde(tag = "state", content = "value", rename_all = "snake_case")]
pub enum SavePathError {
    #[error(transparent)]
    Invalid(#[from] InvalidPath),
    #[error("This drive does not exist")]
    RootDoesNotExist,
    #[error("This path exists but it's not a directory")]
    NotADirectory,
    #[error("You don't have write permissions for this path")]
    PermissionDenied,
}

pub async fn validate_save_path(path: impl AsRef<Path>) -> Result<(), SavePathError> {
    // First, we do a syntatic check to discard plainly invalid paths
    let path = match ValidPath::new(path) {
        Ok(path) => path,
        Err(err) => return Err(SavePathError::Invalid(err)),
    };

    // Then if the path already exists, we can say it's valid
    // otherwise, we continue 
    match tokio::fs::metadata(path.as_ref()).await {
        Ok(meta) if meta.is_dir() => return Ok(()),
        Ok(_) => return Err(SavePathError::NotADirectory),
        Err(err) => match err.kind() {
            std::io::ErrorKind::NotFound => {},
            _ => return Err(SavePathError::PermissionDenied),
        }
    }
    
    // Walk parents until we find one that exists
    let mut current_path: &Path = match path.as_ref().parent() {
        Some(parent) => parent,
        None => return Err(SavePathError::RootDoesNotExist),
    };
    
    loop {
        match tokio::fs::metadata(current_path).await {
            Ok(metadata) => {
                // We found an existing directory
                if metadata.is_dir()  {
                    if check_writable(current_path).await {
                        return Ok(());
                    } else {
                        return Err(SavePathError::PermissionDenied);
                    }
                } else {
                    return Err(SavePathError::NotADirectory);
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                // Doesn't exist, go to parent
                match current_path.parent() {
                    Some(parent) => current_path = parent,
                    None => return Err(SavePathError::RootDoesNotExist),
                }
            }
            Err(_) => {
                // PermissionDenied or other IO error
                return Err(SavePathError::PermissionDenied);
            }
        }
    }
}

/// Check if we can write to a directory by quickly creating and deleting a temp file
async fn check_writable(directory: impl AsRef<Path>) -> bool {
    let directory = directory.as_ref();
    let temp_path = directory.join(format!("{}.tmp", rand::random::<u64>()));

    match tokio::fs::File::create(&temp_path).await {
        Ok(_) => {
            let _ = tokio::fs::remove_file(&temp_path).await;
            true
        }
        Err(_) => false,
    }
}
