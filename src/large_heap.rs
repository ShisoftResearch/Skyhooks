use super::*;
use crate::generic_heap::ObjectMeta;

pub struct Heap {
    meta: chashmap::CHashMap<usize, ObjectMeta>
}

impl Heap {
    pub fn new() -> Self {
        unimplemented!()
    }
    pub fn allocate(&self, size: usize) -> Ptr {
        unimplemented!()
    }
    pub fn contains(&self, ptr: Ptr) -> bool {
        unimplemented!()
    }
    pub fn free(&self, ptr: Ptr) -> bool {
        unimplemented!()
    }
    pub fn meta_of(&self, ptr: Ptr) -> Option<ObjectMeta> {
        unimplemented!()
    }
}