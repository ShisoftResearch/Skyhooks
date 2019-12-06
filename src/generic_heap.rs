use super::*;
use crate::utils::{is_power_of_2, CACHE_LINE_SIZE};
use core::{mem, ptr};
use libc::*;
use std::ptr::null_mut;
use std::cmp::{min, max};

pub const NUM_SIZE_CLASS: usize = 16;

// because the allocated object address are cache aligned and starts from 2,
// there will be a zero in the LSB of the address, use this bit as the flag
// indicate if the object is big object or small object
pub const BOOKMARK_TYPE_FLAG_MASK: usize = 1;

#[derive(Clone)]
pub struct ObjectMeta {
    pub size: usize,
    pub tid: usize,
}

#[cfg(not(feature = "bump_heap_only"))]
pub unsafe fn malloc(size: Size) -> Ptr {
    let max_small_size = *small_heap::MAXIMUM_SIZE;
    if size > max_small_size {
        large_heap::allocate(size)
    } else {
        small_heap::allocate(size)
    }
}

#[cfg(feature = "bump_heap_only")]
pub unsafe fn malloc(size: Size) -> Ptr {
    bump_heap::malloc(size)
}

#[cfg(not(feature = "bump_heap_only"))]
pub unsafe fn free(ptr: Ptr) {
    let (bookmark, is_bump) = object_bookmark(ptr as usize);
    if !is_bump {
        small_heap::free(ptr, bookmark);
    } else {
        large_heap::free(ptr, bookmark)
    }
}

#[cfg(feature = "bump_heap_only")]
pub unsafe fn free(ptr: Ptr) {
    let (bookmark, is_bump) = object_bookmark(ptr as usize);
    bump_heap::free(ptr, bookmark);
}

pub unsafe fn realloc(ptr: Ptr, size: Size) -> Ptr {
    if ptr == NULL_PTR {
        return malloc(size);
    }
    if size == 0 {
        free(ptr);
        return NULL_PTR;
    }
    let (bookmark, is_bump) = object_bookmark(ptr as usize);
    let old_size = if is_bump {
        large_heap::size_of(ptr, bookmark)
    } else {
        small_heap::size_of(ptr, bookmark)
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

pub unsafe fn object_bookmark(obj_addr: usize) -> (usize, bool) {
    let bookmark_ptr = (obj_addr - size_of_bookmark_word::<usize>()) as *const usize;
    let bookmark = ptr::read(bookmark_ptr);
    (bookmark & (!BOOKMARK_TYPE_FLAG_MASK), bookmark & BOOKMARK_TYPE_FLAG_MASK == 0)
}

pub unsafe fn full_object_bookmark<T>(obj_addr: usize) -> T {
    let bookmark_ptr = (obj_addr - size_of_bookmark_word::<T>()) as *const T;
    ptr::read(bookmark_ptr)
}

pub fn bookmark_size<T>(obj_size: usize) -> usize {
    min(CACHE_LINE_SIZE, max(obj_size, size_of_bookmark_word::<T>() ))
}

#[inline(always)]
pub fn size_of_bookmark_word<T>() -> usize {
    mem::size_of::<T>()
}

// return the address for the object
pub fn make_bookmark<T>(tuple_addr: usize, bookmark_size: usize, bookmark: usize, is_bump: bool) -> (usize, usize) {
    debug_assert_eq!(bookmark & BOOKMARK_TYPE_FLAG_MASK, 0);
    let size_of_bookmark_word = size_of_bookmark_word::<T>();
    let bookmark_addr = tuple_addr + (bookmark_size - size_of_bookmark_word);
    let object_addr = tuple_addr + bookmark_size;
    let bookmark_with_type = if is_bump { bookmark + 1 } else { bookmark };
    unsafe {
        ptr::write(bookmark_addr as *mut usize, bookmark_with_type)
    };
    return (object_addr, bookmark_addr + mem::size_of::<usize>());
}