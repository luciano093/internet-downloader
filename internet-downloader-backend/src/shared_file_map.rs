use std::path::PathBuf;

use memmap2::MmapMut;

pub struct SharedFileMap {
    mmap: MmapMut,
    ptr: *mut u8,
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
        Self { mmap, ptr }
    }

    pub fn write_chunk(&self, offset: usize, data: &[u8]) {
        unsafe {
            let dst_ptr = self.ptr.add(offset);
            
            // "Zero-copy" write: essentially a memcpy directly into the OS file cache
            std::ptr::copy_nonoverlapping(data.as_ptr(), dst_ptr, data.len());
        }
    }
}