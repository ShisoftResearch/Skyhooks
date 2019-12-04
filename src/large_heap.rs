// Heap for large objects exceeds maximum tier of pages
// Use bump heap

use crate::mmap_heap::MmapAllocator;
use crate::utils::align_padding;
use crate::utils::SYS_PAGE_SIZE;
use crate::Ptr;
use core::alloc::{Alloc, Layout};

pub unsafe fn allocate(size: usize) -> Ptr {
    if size < crate::bump_heap::HEAP_VIRT_SIZE {
        crate::bump_heap::malloc(size)
    } else {
        let mut ma = MmapAllocator;
        ma.alloc(Layout::from_size_align(size, 1).unwrap())
            .unwrap()
            .as_ptr() as Ptr
    }
}
pub unsafe fn free(ptr: Ptr) -> bool {
    crate::bump_heap::free(ptr)
}
pub fn size_of(ptr: Ptr) -> Option<usize> {
    crate::bump_heap::size_of(ptr)
}
