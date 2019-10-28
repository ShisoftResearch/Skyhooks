use super::*;
use crate::generic_heap::ObjectMeta;
use crate::utils::{current_numa, current_thread_id};
use crate::collections::lflist;

const NUM_SIZE_CLASS: usize = 16;
const CACHE_LINE_SIZE: usize = 64;

type TSizeClasses = [SizeClass; NUM_SIZE_CLASS];

thread_local! {
    static THREAD_META: ThreadMeta = ThreadMeta::new()
}

lazy_static! {
    // static ref PER_NODE_META: Vec
}

struct ThreadMeta {
    numa: usize,
    tid: usize,
}

struct NodeMeta {

}

struct SizeClass {
    size: usize,
    free_list: lflist::List
}

pub struct Heap {}

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
    pub fn size_of(&self, ptr: Ptr) -> Option<usize> {
        unimplemented!()
    }
}

impl ThreadMeta {
    pub fn new() -> Self {
        Self {
            numa: current_numa(),
            tid: current_thread_id(),
        }
    }
}

// Return thread resource to global
impl Drop for ThreadMeta {
    fn drop(&mut self) {
        unimplemented!()
    }
}
