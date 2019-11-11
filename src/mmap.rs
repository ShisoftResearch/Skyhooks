use super::*;
use core::ptr;
use errno::errno;
use libc::*;

const MADV_NOHUGEPAGE: c_int = 14;

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
        let err = errno();
        panic!("mmap failed: [{}] {}", err.0, err);
    };
    no_huge_page(ptr, size);
    ptr
}

pub fn munmap_memory(address: Ptr, size: usize) {
    unsafe {
        munmap(address, size as usize);
    }
}

#[cfg(target_os = "linux")]
#[inline]
pub fn no_huge_page(ptr: Ptr, size: usize) {
    unsafe {
        madvise(ptr, size, MADV_NOHUGEPAGE);
    }
}

#[cfg(not(target_os = "linux"))]
#[inline]
pub fn no_huge_page(ptr: Ptr, size: usize) {}

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
