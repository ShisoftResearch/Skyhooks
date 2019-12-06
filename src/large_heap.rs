// Heap for large objects exceeds maximum tier of pages
// Use bump heap

use crate::mmap_heap::MmapAllocator;
use crate::utils::align_padding;
use crate::utils::SYS_PAGE_SIZE;
use crate::Ptr;
use core::alloc::{Alloc, Layout};

pub unsafe fn allocate(size: usize) -> Ptr {
    let page_size = *SYS_PAGE_SIZE;
    let padding = align_padding(size, page_size);
    let total_size = size + padding;
    if total_size < crate::bump_heap::HEAP_VIRT_SIZE {
        crate::bump_heap::malloc(total_size)
    } else {
        unimplemented!();
        let mut ma = MmapAllocator;
        ma.alloc(Layout::from_size_align(size, 1).unwrap())
            .unwrap()
            .as_ptr() as Ptr
    }
}
pub unsafe fn free(ptr: Ptr, bookmark: usize) {
    crate::bump_heap::free(ptr, bookmark)
}
pub fn size_of(ptr: Ptr, bookmark: usize) -> usize {
    crate::bump_heap::size_of(ptr, bookmark)
}
