use std::{fs::File, io, path::PathBuf};

use memmap2::MmapMut;

pub struct SharedFileMap {
    _mmap: MmapMut,
    ptr: *mut u8,
    size: u64,
}

// This promises to not write to overlapping offsets
unsafe impl Send for SharedFileMap {}
unsafe impl Sync for SharedFileMap {}

impl SharedFileMap {
    pub fn new(path: &PathBuf, size: u64) -> Self {
        let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)
        .unwrap();

        file.set_len(size).unwrap();
        let mut mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
        let ptr = mmap.as_mut_ptr();
        Self { _mmap: mmap, ptr, size }
    }

    pub fn write_chunk(&self, offset: usize, data: &[u8]) {
        let end = offset + data.len();


        if end as u64 > self.size {
            panic!("Out of bounds write! File size: {}, End: {}", self.size, end);
        }

        unsafe {
            let dst_ptr = self.ptr.add(offset);
            
            // "Zero-copy" write: essentially a memcpy directly into the OS file cache
            std::ptr::copy_nonoverlapping(data.as_ptr(), dst_ptr, data.len());
        }
    }
}