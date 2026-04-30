use std::{fs::File, io::{Read, Seek, SeekFrom}, path::Path, sync::{Arc, atomic::{AtomicBool, Ordering}}};

use memmap2::MmapOptions;

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