use std::{fs::File, path::PathBuf};

#[derive(Debug)]
pub struct SharedFileMap {
    file: File,
    size: u64,
    path: PathBuf,
}

impl SharedFileMap {
    pub fn new(path: PathBuf, size: u64) -> Self {
        let mut file_options = std::fs::OpenOptions::new();

        file_options.read(true)
            .write(true)
            .create(true);

        #[cfg(target_os = "windows")]
        {
            use std::os::windows::fs::OpenOptionsExt;

            // FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE
            // Allows us to get permission to delete a file while it's being modified
            const SHARE_ALL: u32 = 7;
            file_options.share_mode(SHARE_ALL);

            const ACCESS_DELETE: u32 = 0x00010000;
            const GENERIC_READ: u32 = 0x80000000;
            const GENERIC_WRITE: u32 = 0x40000000;
            
            // Request delete access to the handle
            // Otherwise SetFileInformationByHandle will fail later
            file_options.access_mode(GENERIC_READ | GENERIC_WRITE | ACCESS_DELETE);
        }

        let file = file_options.open(&path).unwrap();

        file.set_len(size).unwrap();
        
        Self { file, size, path }
    }

    pub fn write_chunk(&self, offset: usize, data: &[u8]) {
        let end = offset + data.len();

        if end as u64 > self.size {
            panic!("Out of bounds write! File size: {}, End: {}", self.size, end);
        }

        // Random-Access write directly to the file offset
        #[cfg(target_os = "windows")]
        {
            use std::os::windows::fs::FileExt;
            self.file.seek_write(data, offset as u64).unwrap();
        }

        #[cfg(not(target_os = "windows"))]
        {
            use std::os::unix::fs::FileExt;
            self.file.write_at(data, offset as u64).unwrap();
        }
    }

    pub fn delete(self) {
        // Windows
        #[cfg(target_os = "windows")]
        {
            use std::os::windows::io::AsRawHandle;
            use windows_sys::Win32::Storage::FileSystem::{
                SetFileInformationByHandle, 
                FileDispositionInfoEx, 
                FILE_DISPOSITION_INFO_EX, 
                FILE_DISPOSITION_FLAG_DELETE, 
                FILE_DISPOSITION_FLAG_POSIX_SEMANTICS,
                FileDispositionInfo,
                FILE_DISPOSITION_INFO
            };

            let handle = self.file.as_raw_handle() as isize;

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
                use tracing::error;
                let err = std::io::Error::last_os_error();
                error!("Failed to mark file for deletion! OS Error: {}", err);
            }
        }

        #[cfg(not(target_os = "windows"))]
        {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}