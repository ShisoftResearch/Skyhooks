use super::*;
use libc::*;
use core::ptr;

pub fn mmap_without_fd(size: usize) -> Ptr {
    unsafe {
        mmap(
            ptr::null_mut(),
            size as size_t,
            PROT_READ | PROT_READ,
            MAP_ANONYMOUS,
            -1,
            0
        )
    }
}

pub fn munmap_memory(address: Ptr, size: usize) {
    unsafe {
        munmap(address, size as usize);
    }
}

pub fn dealloc_regional(addr: Ptr, size: usize) -> usize {
    unsafe {
        madvise(addr, size, MADV_DONTNEED) as usize
    }
}