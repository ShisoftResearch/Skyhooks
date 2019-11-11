// Heap for large objects exceeds maximum tier of pages
// Use bump heap

use crate::utils::align_padding;
use crate::utils::SYS_PAGE_SIZE;
use crate::Ptr;

pub unsafe fn allocate(size: usize) -> Ptr {
    let page_size = *SYS_PAGE_SIZE;
    let padding = align_padding(size, page_size);
    let total_size = size + padding;
    if total_size < crate::bump_heap::HEAP_VIRT_SIZE {
        crate::bump_heap::malloc(total_size)
    } else {
        unimplemented!()
    }
}
pub unsafe fn free(ptr: Ptr) -> bool {
    crate::bump_heap::free(ptr)
}
pub fn size_of(ptr: Ptr) -> Option<usize> {
    crate::bump_heap::size_of(ptr)
}
