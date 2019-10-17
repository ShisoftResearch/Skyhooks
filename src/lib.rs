#![cfg(feature="spin_no_std")] // for lazy_static
#![no_std]
extern crate alloc;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate libc;

use libc::*;

pub mod bump_heap;
mod generic_heap;
mod bibop_heap;
mod large_heap;
mod mmap;
mod hashmap;
mod utils;

pub type Ptr = *mut c_void;
pub type Size = usize;
pub type Void = libc::c_void;
pub const NULL: usize = 0;
pub const NULL_PTR: *mut c_void = NULL as *mut c_void;

pub unsafe fn nu_malloc(size: Size) -> Ptr {
    generic_heap::malloc(size)
}

pub unsafe fn nu_free(ptr: Ptr) {
    generic_heap::free(ptr)
}

pub unsafe fn nu_calloc(nmemb: Size, size: Size) -> Ptr {
    let total_size = nmemb * size;
    let mut ptr = nu_malloc(total_size);
    if ptr == NULL_PTR {
        memset(ptr, 0, total_size);
    }
    ptr
}

pub unsafe fn nu_realloc(ptr: Ptr, size: Size) -> Ptr {
    generic_heap::realloc(ptr, size)
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

use core::alloc::{GlobalAlloc, Layout};
use alloc::vec::Vec;

#[global_allocator]
static INTERNAL_ALLOCATOR: bump_heap::BumpAllocator = bump_heap::BumpAllocator;

