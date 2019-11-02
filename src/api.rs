use crate::{generic_heap, Ptr, Size, NULL_PTR};
use core::alloc::{GlobalAlloc, Layout};
use libc::*;

pub unsafe fn nu_malloc(size: Size) -> Ptr {
    generic_heap::malloc(size)
}

pub unsafe fn nu_free(ptr: Ptr) {
    generic_heap::free(ptr)
}

pub unsafe fn nu_calloc(nmemb: Size, size: Size) -> Ptr {
    let total_size = nmemb * size;
    let ptr = nu_malloc(total_size);
    if ptr == NULL_PTR {
        memset(ptr, 0, total_size);
    }
    ptr
}

pub unsafe fn nu_realloc(ptr: Ptr, size: Size) -> Ptr {
    generic_heap::realloc(ptr, size)
}

// Allocator for rust itself for internal heaps
pub struct NullocAllocator;

unsafe impl GlobalAlloc for NullocAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let size = layout.size();
        let align = layout.align();
        let actual_size = layout.padding_needed_for(align) + size;
        nu_malloc(actual_size) as *mut u8
    }
    unsafe fn dealloc(&self, ptr: *mut u8, _layout: Layout) {
        nu_free(ptr as Ptr)
    }
}
