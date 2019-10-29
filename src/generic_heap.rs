use super::*;
use libc::*;
use utils::current_thread_id;

static LARGE_OBJ_THRESHOLD: usize = 1024 * 1024;

lazy_static! {
    static ref LARGE_HEAP: large_heap::Heap = large_heap::Heap::new();
}

#[derive(Copy, Clone)]
pub struct ObjectMeta {
    pub size: usize,
    pub addr: usize,
    pub numa: usize,
    pub tier: usize,
    pub tid: usize,
}

pub unsafe fn malloc(size: Size) -> Ptr {
    if size >= LARGE_OBJ_THRESHOLD {
        LARGE_HEAP.allocate(size)
    } else {
        bibop_heap::allocate(size)
    }
}

pub unsafe fn free(ptr: Ptr) {
    if !bibop_heap::free(ptr) {
    } else if !LARGE_HEAP.free(ptr) {
    } else {
        warn!("Ptr {} does not existed", ptr as usize)
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
    let old_size = if let Some(size) = bibop_heap::size_of(ptr) {
        size
    } else if let Some(meta) = LARGE_HEAP.size_of(ptr) {
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

impl ObjectMeta {
    pub fn new(ptr: usize, size: usize) -> Self {
        Self {
            size,
            addr: ptr,
            numa: 0,
            tier: 0,
            tid: current_thread_id(),
        }
    }
}
