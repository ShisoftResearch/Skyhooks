#![feature(alloc_layout_extra)]

#[macro_use]
extern crate lazy_static;

use std::alloc::{GlobalAlloc, Layout, alloc};
use std::ptr::null_mut;
use libc::*;

mod generic_heap;
mod bibop_heap;
mod large_heap;

pub type Ptr = *mut c_void;
pub type Size = usize;
pub type Void = libc::c_void;
pub const NULL: usize = 0;
pub const NULL_PTR: *mut c_void = NULL as *mut c_void;

lazy_static! {
    static ref BIBOP_HEAP: bibop_heap::Heap = bibop_heap::Heap::new();
    static ref LARGE_HEAP: large_heap::Heap = large_heap::Heap::new();
}

pub unsafe fn nu_malloc(size: Size) -> Ptr {
    unimplemented!()
}

pub unsafe fn nu_free(ptr: Ptr) {
    unimplemented!()
}

pub unsafe fn nu_calloc(nmemb: Size, size: Size) -> Ptr {
    let total_size = nmemb * size;
    let mut ptr = nu_malloc(total_size);
    if ptr == NULL_PTR {
        memset(ptr, 0, total_size);
    }
    ptr
}

pub fn nu_realloc(ptr: Ptr, size: Size) -> Ptr {
    unimplemented!()
}

// Allocator for rust itself for internal heaps
struct SelfAllocator;

unsafe impl GlobalAlloc for SelfAllocator {
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

use std::alloc::System;
#[global_allocator]
// static INTERNAL_ALLOCATOR: SelfAllocator = SelfAllocator;
static INTERNAL_ALLOCATOR: System = System;

