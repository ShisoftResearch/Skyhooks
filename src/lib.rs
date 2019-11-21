// Gave up on no_std for filesystem is required for this allocator to get CPU related information

#![feature(alloc_layout_extra)]
#![feature(alloc_error_handler)]
#![feature(core_intrinsics)]
#![feature(allocator_api)]
#![feature(test)]

extern crate alloc;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate crossbeam;
extern crate libc;
extern crate test;

pub mod api;
mod bump_heap;
mod generic_heap;
mod large_heap;
mod mmap;
mod mmap_heap;
mod rand;
mod small_heap;
mod utils;

mod collections;

pub type Ptr = *mut c_void;
pub type Size = usize;
pub type Void = libc::c_void;
pub const NULL: usize = 0;
pub const NULL_PTR: *mut c_void = NULL as *mut c_void;

use crate::api::NullocAllocator;
use crate::bump_heap::BumpAllocator;
use core::ffi::c_void;

#[no_mangle]
pub unsafe fn malloc(size: Size) -> Ptr {
    api::nu_malloc(size)
}

#[no_mangle]
pub unsafe fn free(ptr: Ptr) {
    api::nu_free(ptr)
}

#[no_mangle]
pub unsafe fn calloc(nmemb: Size, size: Size) -> Ptr {
    api::nu_calloc(nmemb, size)
}

#[no_mangle]
pub unsafe fn realloc(ptr: Ptr, size: Size) -> Ptr {
    api::nu_realloc(ptr, size)
}

#[global_allocator]
#[cfg(not(feature = "bump_heap_only"))]
static INNER_ALLOCATOR: NullocAllocator = NullocAllocator;
//
//#[global_allocator]
//#[cfg(feature = "bump_heap_only")]
//static INNER_ALLOCATOR: BumpAllocator = BumpAllocator;
