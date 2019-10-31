use super::*;
use crate::generic_heap::ObjectMeta;
use crate::mmap::mmap_without_fd;
use crate::utils::*;
use core::mem;
use lfmap::{Map, ObjectMap};
use mmap::munmap_memory;

// Heap for large objects exceeds maximum tier of pages
// The large heap will only track its existence and use mmap allocate
// and munmap to return resource to the OS

pub struct Heap {
    meta: ObjectMap<ObjectMeta>,
}

impl Heap {
    pub fn new() -> Self {
        Self {
            meta: ObjectMap::with_capacity(512),
        }
    }
    pub unsafe fn allocate(&self, size: usize) -> Ptr {
        let padding = align_padding(size, *SYS_PAGE_SIZE);
        let page_size = size + padding;
        let ptr = mmap_without_fd(page_size) as usize;
        let meta = ObjectMeta::new(ptr, size);
        self.meta.insert(ptr, meta);
        ptr as Ptr
    }
    pub fn free(&self, ptr: Ptr) -> bool {
        if let Some(meta) = self.meta.remove((ptr as usize)) {
            munmap_memory(ptr, meta.size);
            true
        } else {
            false
        }
    }
    pub fn meta_of(&self, ptr: Ptr) -> Option<ObjectMeta> {
        self.meta.get(ptr as usize)
    }
    pub fn size_of(&self, ptr: Ptr) -> Option<usize> {
        self.meta.get(ptr as usize).map(|m| m.size)
    }
}
