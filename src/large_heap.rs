use super::*;
use crate::generic_heap::ObjectMeta;
// use chashmap::CHashMap;
use crate::utils::*;
use crate::mmap::mmap_without_fd;

// Heap for large objects exceeds maximum tier of pages
// The large heap will only track its existence and use mmap allocate
// and munmap to return resource to the OS

pub struct Heap {
    // meta: CHashMap<usize, ObjectMeta>
}

impl Heap {
    pub fn new() -> Self {
        Self {
            // meta: CHashMap::new()
        }
    }
    pub unsafe fn allocate(&self, size: usize) -> Ptr {
        let padding = align_padding(size, *SYS_PAGE_SIZE);
        let page_size = size + padding;
        let ptr = mmap_without_fd(page_size);

        unimplemented!()
    }
    pub fn free(&self, ptr: Ptr) -> bool {
//        if let Some(meta) = self.meta.remove(&(ptr as usize)) {
//            true
//        } else {
//            false
//        }
        unimplemented!()
    }
    pub fn meta_of(&self, ptr: Ptr) -> Option<ObjectMeta> {
        unimplemented!()
    }
    pub fn size_of(&self, ptr: Ptr) -> Option<usize> {
        unimplemented!()
    }
}