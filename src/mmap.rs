use super::*;
use core::ptr;
use libc::*;

pub fn mmap_without_fd(size: usize) -> Ptr {
    let ptr = unsafe {
        mmap(
            ptr::null_mut(),
            size as size_t,
            PROT_READ | PROT_WRITE,
            MAP_ANONYMOUS | MAP_PRIVATE,
            -1,
            0,
        )
    };
    if ptr == -1 as isize as *mut c_void {
        let err = panic!("mmap failed {}");
    };
    ptr
}

pub fn munmap_memory(address: Ptr, size: usize) {
    unsafe {
        munmap(address, size as usize);
    }
}

pub fn dealloc_regional(addr: Ptr, size: usize) -> usize {
    unsafe { madvise(addr, size, MADV_DONTNEED) as usize }
}

#[cfg(test)]
mod test {
    use crate::mmap::mmap_without_fd;
    use core::mem;

    #[test]
    pub fn mmap() {
        let mut val = unsafe { *(mmap_without_fd(mem::size_of::<usize>()) as *mut usize) };
        for i in 0..100 {
            val = i;
        }
        assert_eq!(val, 99);
    }
}
