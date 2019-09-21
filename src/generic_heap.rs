use super::*;
use std::thread::ThreadId;

static LARGE_BOUND: usize = 1024 * 1024;

lazy_static! {
    static ref BIBOP_HEAP: bibop_heap::Heap = bibop_heap::Heap::new();
    static ref LARGE_HEAP: large_heap::Heap = large_heap::Heap::new();
}

pub struct ObjectMeta {
    size: usize,
    addr: Ptr,
    tid: ThreadId,
    numa: usize
}

pub unsafe fn malloc(size: Size) -> Ptr {
    if size >= LARGE_BOUND {
        LARGE_HEAP.allocate(size)
    } else {
        BIBOP_HEAP.allocate(size)
    }
}

pub unsafe fn free(ptr: Ptr) {
    if !BIBOP_HEAP.free(ptr) {}
    else if !LARGE_HEAP.free(ptr) {}
    else { warn!("Ptr {} does not existed", ptr as usize) }
}

pub unsafe fn realloc(ptr: Ptr, size: Size) -> Ptr {
    if ptr == NULL_PTR {
        return malloc(size);
    }
    if size == 0 {
        free(ptr);
        return NULL_PTR;
    }
    let old_size = if let Some(meta) = BIBOP_HEAP.meta_of(ptr) {
        meta.size
    } else if let Some(meta) = LARGE_HEAP.meta_of(ptr) {
        meta.size
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