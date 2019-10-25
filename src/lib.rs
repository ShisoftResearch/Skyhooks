// Gave up on no_std for filesystem is required for this allocator to get CPU related information

#![feature(alloc_layout_extra)]
#![feature(alloc_error_handler)]
#![feature(core_intrinsics)]
extern crate alloc;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate libc;

mod bump_heap;
mod api;
mod generic_heap;
mod bibop_heap;
mod large_heap;
mod mmap;
mod utils;
mod lfmap;

pub type Ptr = *mut c_void;
pub type Size = usize;
pub type Void = libc::c_void;
pub const NULL: usize = 0;
pub const NULL_PTR: *mut c_void = NULL as *mut c_void;

use core::alloc::{GlobalAlloc, Layout};
use core::ffi::c_void;

