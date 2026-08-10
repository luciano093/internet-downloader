use std::{collections::HashSet, ffi::OsStr, fs::File, io::{Read, Seek, SeekFrom}, path::Path, sync::{Arc, atomic::{AtomicBool, Ordering}}};

use memmap2::MmapOptions;
use os_str_bytes::OsStrBytesExt;
use thiserror::Error;

pub fn force_delete_file(path: &std::path::Path) {
    // Windows
    #[cfg(target_os = "windows")]
    {
        use std::os::windows::{fs::OpenOptionsExt, io::AsRawHandle};
        use windows_sys::Win32::Storage::FileSystem::{
            SetFileInformationByHandle, 
            FileDispositionInfoEx, 
            FILE_DISPOSITION_INFO_EX, 
            FILE_DISPOSITION_FLAG_DELETE, 
            FILE_DISPOSITION_FLAG_POSIX_SEMANTICS,
            FileDispositionInfo,
            FILE_DISPOSITION_INFO
        };

        let file_opts = std::fs::OpenOptions::new()
            .access_mode(0x00010000 | 0x80000000 | 0x40000000) // DELETE | READ | WRITE
            .share_mode(7) // SHARE_ALL
            .open(path);

        if let Ok(file) = file_opts {
            let handle = file.as_raw_handle() as isize;

            // Try  Windows 10+ POSIX semantics first.
            // This flag forcefully overrides Windows Defender / Antivirus memory-map locks
            // allowing the file to be unlinked immediately even if a background process is scanning it.
            let mut fdi_ex = FILE_DISPOSITION_INFO_EX { 
                Flags: FILE_DISPOSITION_FLAG_DELETE | FILE_DISPOSITION_FLAG_POSIX_SEMANTICS 
            };

            let mut success = unsafe {
                SetFileInformationByHandle(
                    handle as _,
                    FileDispositionInfoEx,
                    &mut fdi_ex as *mut _ as *mut std::ffi::c_void,
                    std::mem::size_of::<FILE_DISPOSITION_INFO_EX>() as u32,
                )
            };

            // Fallback for older Windows versions (or FAT32 drives)
            if success == 0 {
                let err = std::io::Error::last_os_error();
                // ERROR_INVALID_PARAMETER (87) means POSIX semantics aren't supported on this OS/Drive
                if err.raw_os_error() == Some(87) { 
                    
                    let mut fdi = FILE_DISPOSITION_INFO { DeleteFile: true }; 
                    
                    success = unsafe {
                        SetFileInformationByHandle(
                            handle as _,
                            FileDispositionInfo,
                            &mut fdi as *mut _ as *mut std::ffi::c_void,
                            std::mem::size_of::<FILE_DISPOSITION_INFO>() as u32,
                        )
                    };
                }
            }

            if success == 0 {
                tracing::error!("Failed to force delete file {:?}! OS Error: {}", path, std::io::Error::last_os_error());
            } else {
                tracing::info!("Successfully force deleted file from disk: {:?}", path);
            }
        }
    }

    #[cfg(not(target_os = "windows"))]
    {
        if let Err(e) = std::fs::remove_file(path) {
        tracing::error!("Failed to delete file {:?}: {}", path, e);
        } else {
            tracing::info!("Successfully deleted file from disk: {:?}", path);
        }
    }
}

pub fn hash_file(path: &Path, cancel_flag: Option<Arc<AtomicBool>>) -> std::io::Result<u128> {
    let file = File::open(path)?;
    let mmap = unsafe { MmapOptions::new().map(&file)? };
    let mut hasher = blake3::Hasher::new();

    // According to blake: `update_rayon` is
    // _slower_ than `update` for inputs under 128 KiB.
    let chunk_size = 16 * 1024 * 1024; 
    
    for chunk in mmap.chunks(chunk_size) {

        // If the cancel_flag is true, we return instantly
        if cancel_flag.as_ref().is_some_and(|flag| flag.load(Ordering::Relaxed)) {
            return Err(std::io::Error::new(std::io::ErrorKind::Interrupted, "Cancelled"));
        }
        hasher.update_rayon(chunk);
    }

    let mut output = [0u8; 16];

    hasher.finalize_xof().fill(&mut output);

    Ok(u128::from_le_bytes(output))
}

pub fn hash_file_chunk(path: &Path, start: u64, length: usize) -> std::io::Result<[u8; 16]> {
    let mut file = File::open(&path)?;
    let mut hasher = blake3::Hasher::new();

    // If the chunk is tiny, skip the mmap overhead and just read it.
    if length < 16 * 1024 {
        let mut buffer = vec![0u8; length];
        file.seek(SeekFrom::Start(start))?;
        file.read_exact(&mut buffer)?;
        
        hasher.update(&buffer);
    } else {
        let mmap = unsafe { 
            MmapOptions::new()
                .offset(start)
                .len(length)
                .map(&file)?
        };
        
        hasher.update_rayon(&mmap);
    }

    let mut output = [0u8; 16];
    hasher.finalize_xof().fill(&mut output);

    Ok(output)
}

#[derive(Debug, Error, Clone)]
pub enum InvalidFilename {
    #[error("The filename can not be empty")]
    Empty,
    #[error("The filename can not contain a null byte")]
    ContainsNullByte,
    #[error("The filename can not be a relative marker (e.g. '.' or '..')")]
    RelativeMarkers,
    #[error("The filename can not contain a slash '/'")]
    ContainsForwardSlash,
    #[error("The filename can not be longer than 255-bytes")]
    TooLong, // there is a 255-byte limit in windows/unix
    #[error(transparent)]
    Window(#[from] InvalidWindowsFilename),
}

#[derive(Debug, Error, Clone)]
pub enum InvalidWindowsFilename {
    #[error("The filename can not contain a backward slash '\\'")]
    ContainsBackwardSlash,
    #[error("The filename contains the following invalid characters: ({0:?})")]
    InvalidCharacters(HashSet<char>),
    #[error("The filename can not end with a space")]
    EndsWithSpace,
    #[error("The filename can not end with a dot")]
    EndsWithDot,
    #[error("The filename can not be the reserved name: {0}")]
    ReservedName(String),
}

/// Validates that a file or folder name is valid. For example "report.pdf" or "my_folder".
/// Returns false if it contains path separators, invalid chars, or DOS reserved names.
pub fn is_valid_file_name(name: impl AsRef<OsStr>) -> Result<(), InvalidFilename> {
    let name = name.as_ref();
    
    // Cannot be empty, null-terminated, or relative markers
    if name.is_empty() {
        return Err(InvalidFilename::Empty);
    }

    if name.contains('\0') {
        return Err(InvalidFilename::ContainsNullByte);
    }
    
    if name == "." || name == ".." {
        return Err(InvalidFilename::RelativeMarkers);
    }

    // Neither windows nor unix allow '/' in names
    if name.contains('/') {
        return Err(InvalidFilename::ContainsForwardSlash);
    }

    // Length limit (windows and unix have a 255-byte limit per component)
    if name.len() > 255 {
        return Err(InvalidFilename::TooLong);
    }

    #[cfg(windows)]
    {
        let name = name.to_string_lossy();
        
        // '\' is a path separator in windows
        if name.contains('\\') {
            return Err(InvalidWindowsFilename::ContainsBackwardSlash.into());
        }
        
        // On windows, all the following are illegal in filenames
        let invalid_chars = ['<', '>', ':', '"', '|', '?', '*'];

        // Windows also rejects newlines \n and tabs \t in filenames
        let found_invalid: HashSet<char> = name
            .chars()
            .filter(|char| char.is_ascii_control() || invalid_chars.contains(char))
            .collect();

        if !found_invalid.is_empty() {
            return Err(InvalidWindowsFilename::InvalidCharacters(found_invalid).into());
        }

        // Names can't end with space or dots
        if name.ends_with(' ') {
            return Err(InvalidWindowsFilename::EndsWithSpace.into());
        }

        if name.ends_with('.') {
            return Err(InvalidWindowsFilename::EndsWithDot.into());
        }

        // Reserved DOS device names
        if is_reserved_dos_name(&name) {
            return Err(InvalidWindowsFilename::ReservedName(name.to_string()).into());
        }
    }

    Ok(())
}

/// Normalizes the filename string to make it valid for the target OS \
/// An empty filename will return an empty String.
pub fn normalize_filename(filename: &str) -> String {
    // Replace invalid chars with '_'
    let mut filename: String = filename
        .chars()
        .map(|char| if is_invalid_filename_char(char) { '_' } else { char })
        .collect();

    // Truncate to 255 bytes
    if filename.len() > 255 {
        let mut end = 255;
        while !filename.is_char_boundary(end) {
            end -= 1;
        }
        filename.truncate(end);
    }

    #[cfg(windows)]
    {
        // Strip trailing dots/spaces
        let filename = filename.trim_end_matches(|char| char == '.' || char == ' ');

        // If any name is a reserved DOS name, append a '_' at the beginning (e.g. "CON" -> "_CON")
        let mut filename = filename.to_string();
        if is_reserved_dos_name(&filename) {
            filename.insert(0, '_');
        }
    }

    filename
}

pub fn is_reserved_dos_name(name: &str) -> bool {
    let name = name.split('.').next().unwrap_or(&name).to_ascii_uppercase();
    
    matches!(
        name.as_str(),
        "CON" | "PRN" | "AUX" | "NUL"
        | "COM1" | "COM2" | "COM3" | "COM4" | "COM5" | "COM6" | "COM7" | "COM8" | "COM9"
        | "LPT1" | "LPT2" | "LPT3" | "LPT4" | "LPT5" | "LPT6" | "LPT7" | "LPT8" | "LPT9"
    )
}

#[cfg(windows)]
fn is_invalid_filename_char(char: char) -> bool {
    char == '/' || char == '\\' || char.is_ascii_control()
        || matches!(char, '<' | '>' | ':' | '"' | '|' | '?' | '*')
}

#[cfg(not(windows))]
fn is_invalid_filename_char(char: char) -> bool {
    char == '/' || char == '\0'
}
