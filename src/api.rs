use crate::{generic_heap, Ptr, Size, NULL_PTR, bump_heap};
use core::alloc::{GlobalAlloc, Layout};
use libc::*;
use lfmap::{Map, WordMap};
use crate::utils::align_padding;
use crate::mmap_heap::*;
use std::ptr::null_mut;
use core::cell::Cell;

thread_local! {
    pub static INNER_CALL: Cell<bool> = Cell::new(false);
}
lazy_static! {
    static ref RUST_ADDR_MAPPING: lfmap::WordMap<MmapAllocator> = lfmap::WordMap::<MmapAllocator>::with_capacity(2048);
}

pub unsafe fn nu_malloc(size: Size) -> Ptr {
    if size == 0 { return null_mut(); } // The C standard (C17 7.22.3/1)
    INNER_CALL.with(|is_inner| {
        if !is_inner.get() {
            is_inner.set(true);
            let res = generic_heap::malloc(size);
            is_inner.set(false);
            res
        } else {
            bump_heap::malloc(size)
        }
    })

}
pub unsafe fn nu_free(ptr: Ptr) {
    if ptr == null_mut() { return; }
    INNER_CALL.with(|is_inner| {
        if !is_inner.get() {
            is_inner.set(true);
            generic_heap::free(ptr);
            is_inner.set(false);
        } else {
            bump_heap::free(ptr);
        }
    })
}

pub unsafe fn nu_calloc(nmemb: Size, size: Size) -> Ptr {
    let total_size = nmemb * size;
    let ptr = nu_malloc(total_size);
    if ptr != NULL_PTR {
        memset(ptr, 0, total_size);
    }
    ptr
}

pub unsafe fn nu_realloc(ptr: Ptr, size: Size) -> Ptr {
    INNER_CALL.with(|is_inner| {
        if !is_inner.get() {
            is_inner.set(true);
            let res = generic_heap::realloc(ptr, size);
            is_inner.set(false);
            res
        } else {
            bump_heap::realloc(ptr, size)
        }
    })
}

// Allocator for rust itself for internal heaps
pub struct NullocAllocator;

unsafe impl GlobalAlloc for NullocAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let size = layout.size();
        let align = layout.align();
        let actual_size = size + align - 1;
        let base_addr = nu_malloc(actual_size) as usize;
        let align_padding = align_padding(base_addr, align);
        let rust_addr = base_addr + align_padding;
        RUST_ADDR_MAPPING.insert(rust_addr, base_addr);
        rust_addr as *mut u8
    }
    unsafe fn dealloc(&self, ptr: *mut u8, _layout: Layout) {
        let addr = ptr as usize;
        if let Some(base_addr) = RUST_ADDR_MAPPING.remove(addr) {
            nu_free(base_addr as Ptr)
        }
    }
}
