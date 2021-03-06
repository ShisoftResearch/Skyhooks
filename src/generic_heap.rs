use super::*;
use crate::utils::is_power_of_2;
use core::mem;
use libc::*;
use std::ptr::null_mut;

pub const NUM_SIZE_CLASS: usize = 16;

#[derive(Clone)]
pub struct ObjectMeta {
    pub size: usize,
    pub tid: usize,
}

#[cfg(not(feature = "bump_heap_only"))]
pub unsafe fn malloc(size: Size) -> Ptr {
    let max_small_size = *small_heap::MAXIMUM_SIZE;
    if size > max_small_size {
        utils::log("LARGE MALLOC", size);
        large_heap::allocate(size)
    } else {
        utils::log("SMALL MALLOC", size);
        small_heap::allocate(size)
    }
}

#[cfg(feature = "bump_heap_only")]
pub unsafe fn malloc(size: Size) -> Ptr {
    bump_heap::malloc(size)
}

#[cfg(not(feature = "bump_heap_only"))]
pub unsafe fn free(ptr: Ptr) {
    if small_heap::free(ptr) {
        utils::log("SMALL FREE", ptr as usize);
    } else if large_heap::free(ptr) {
        utils::log("LARGE FREE", ptr as usize);
    } else {
        warn!("Cannot find object to free at {:x?}", ptr as usize);
    }
}

#[cfg(feature = "bump_heap_only")]
pub unsafe fn free(ptr: Ptr) {
    bump_heap::free(ptr);
}

pub unsafe fn realloc(ptr: Ptr, size: Size) -> Ptr {
    if ptr == NULL_PTR {
        return malloc(size);
    }
    if size == 0 {
        free(ptr);
        return NULL_PTR;
    }
    let old_size = if let Some(size) = small_heap::size_of(ptr) {
        size
    } else if let Some(_) = large_heap::size_of(ptr) {
        size
    } else {
        panic!("Cannot determinate old object");
        return NULL_PTR;
    };
    if old_size >= size {
        info!("old size is larger than requesting size, untouched");
        return ptr;
    }
    let new_ptr = malloc(size);
    memcpy(new_ptr, ptr, old_size);
    free(ptr);
    new_ptr
}

#[inline]
pub fn size_class_index_from_size(size: usize) -> usize {
    debug_assert!(size > 0);
    if size < 2 {
        return 0;
    }
    let log = log_2_of(size);
    if is_power_of_2(size) && log > 0 {
        log - 1
    } else {
        log
    }
}

#[inline]
pub fn log_2_of(num: usize) -> usize {
    mem::size_of::<usize>() * 8 - num.leading_zeros() as usize - 1
}
