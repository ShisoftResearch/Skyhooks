use super::*;
use libc::*;
use std::ptr;

pub unsafe fn mmap_without_fd(size: usize) -> Ptr {
    mmap(
        ptr::null_mut(),
        size as size_t,
        PROT_READ | PROT_READ,
        MAP_ANONYMOUS,
        -1,
        0
    )
}