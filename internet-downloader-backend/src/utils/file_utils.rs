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