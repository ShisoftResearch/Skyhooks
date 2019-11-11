use super::*;
use libc::*;
use std::ptr::null_mut;
use crate::utils::is_power_of_2;
use core::mem;

pub const NUM_SIZE_CLASS: usize = 16;

#[derive(Copy, Clone)]
pub struct ObjectMeta {
    pub size: usize,
    pub tid: usize,
}

pub unsafe fn malloc(size: Size) -> Ptr {
    let max_small_size = *small_heap::MAXIMUM_SIZE;
    let res = if size > max_small_size {
        large_heap::allocate(size)
    } else {
        small_heap::allocate(size)
    };
    if res == null_mut() {
        panic!();
    }
    res
}

pub unsafe fn free(ptr: Ptr) {
    if !small_heap::free(ptr) {
    } else if !large_heap::free(ptr) {
    } else {
        warn!("Cannot find object to free at {:x?}", ptr as usize);
    }
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
        warn!("Cannot determinate old object");
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

#[inline(always)]
pub fn size_class_index_from_size(size: usize) -> usize {
    debug_assert!(size > 0);
    let log = log_2_of(size);
    if is_power_of_2(size) && log > 0 {
        log - 1
    } else {
        log
    }
}

#[inline(always)]
pub fn log_2_of(num: usize) -> usize {
    mem::size_of::<usize>() * 8 - num.leading_zeros() as usize - 1
}