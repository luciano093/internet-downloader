use std::{fs::File, path::PathBuf};
use fs4::fs_std::FileExt;

use tracing::warn;

#[derive(Debug)]
pub struct SharedFileMap {
    file: File,
    size: u64,
}

impl SharedFileMap {
    pub fn new(path: PathBuf, size: u64) -> std::io::Result<Self> {
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

        let file = file_options.open(&path)?;

        file.set_len(size)?;

        if let Err(error) = file.allocate(size) {
            warn!("Could not physically pre-allocate space. OS will fallback to sparse. Error: {}", error);
        }
        
        Ok(Self { file, size })
    }

    pub fn write_chunk(&self, offset: u64, data: &[u8]) -> std::io::Result<()> {
        let end = match offset.checked_add(data.len() as u64) {
            Some(val) => val,
            None => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Offset and data length overflowed u64",
                ));
            }
        };

        if end > self.size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Out of bounds write! File size: {}, End: {}", self.size, end)
            ));
        }

        // Random-Access write directly to the file offset
        #[cfg(target_os = "windows")]
        {
            use std::os::windows::fs::FileExt;
            
            let mut written = 0;

            // Manual loop to simulate write_all
            while written < data.len() {
                match self.file.seek_write(&data[written..], offset + written as u64) {
                    Ok(0) => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::WriteZero,
                            "Failed to write: 0 bytes written (Disk full?)",
                        ));
                    }
                    Ok(size_written) => written += size_written,
                    Err(ref error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(error) => return Err(error),
                }
            }

            Ok(())
        }

        #[cfg(not(target_os = "windows"))]
        {
            use std::os::unix::fs::FileExt;
            self.file.write_all_at(data, offset)
        }
    }
}