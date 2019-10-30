use super::*;
use crate::collections::fixvec::FixedVec;
use crate::collections::lflist;
use crate::generic_heap::ObjectMeta;
use crate::utils::{current_numa, current_thread_id, SYS_TOTAL_MEM, NUM_NUMA_NODES};
use core::mem::MaybeUninit;
use core::mem;
use crate::mmap::mmap_without_fd;

const NUM_SIZE_CLASS: usize = 16;
const CACHE_LINE_SIZE: usize = 64;

type TSizeClasses = [SizeClass; NUM_SIZE_CLASS];

thread_local! {
    static THREAD_META: ThreadMeta = ThreadMeta::new()
}

lazy_static! {
    static ref TOTAL_HEAP_SIZE: usize = total_heap();
    static ref HEAP_BASE: usize = mmap_without_fd(*TOTAL_HEAP_SIZE) as usize;
    static ref PER_NODE_META: FixedVec<NodeMeta> = gen_numa_node_list();
    static ref NODE_SHIFT_BITS: usize = log_2_of(*TOTAL_HEAP_SIZE)  - log_2_of(*NUM_NUMA_NODES);
}

struct ThreadMeta {
    sizes: TSizeClasses,
    numa: usize,
    tid: usize,
}

struct NodeMeta {
    heap_base: usize,
    free_list: lflist::List
}

struct SizeClass {
    size: usize,
    free_list: lflist::List,
}

pub struct Heap {
    addr: usize
}

pub fn allocate(size: usize) -> Ptr {
    unimplemented!()
}
pub fn contains(ptr: Ptr) -> bool {
    unimplemented!()
}
pub fn free( ptr: Ptr) -> bool {
    unimplemented!()
}
pub fn meta_of(ptr: Ptr) -> Option<ObjectMeta> {
    unimplemented!()
}
pub fn size_of(ptr: Ptr) -> Option<usize> {
    unimplemented!()
}

impl ThreadMeta {
    pub fn new() -> Self {
        Self {
            sizes: size_classes(),
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

impl SizeClass {
    pub fn new(tier: usize) -> Self {
        Self {
            size: tier,
            free_list: lflist::List::new()
        }
    }
}

fn gen_numa_node_list() -> FixedVec<NodeMeta> {
    let num_nodes = *NUM_NUMA_NODES;
    let node_shift_bits = *NODE_SHIFT_BITS;
    let mut nodes = FixedVec::new(num_nodes);
    let mut heap_base = *HEAP_BASE;
    for i in 0..num_nodes {
        nodes[i] = NodeMeta {
            free_list: lflist::List::new(),
            heap_base: heap_base + (i << node_shift_bits)
        };
    }
    return nodes;
}

fn size_classes() -> TSizeClasses {
    let mut data: [MaybeUninit<SizeClass>; NUM_SIZE_CLASS] = unsafe { MaybeUninit::uninit().assume_init() };
    let mut tier = 2;
    for elem in &mut data[..] {
        *elem = MaybeUninit::new(SizeClass {
            size: tier,
            free_list: lflist::List::new()
        });
        tier *= 2;
    };
    unsafe { mem::transmute::<_, TSizeClasses>(data) }
}

fn per_node_heap() -> usize {
    min_power_of_2(*SYS_TOTAL_MEM)
}

fn total_heap() -> usize {
    min_power_of_2(per_node_heap() * *NUM_NUMA_NODES)
}

fn min_power_of_2(mut n: usize) -> usize {
    let mut count = 0;
    // First n in the below condition
    // is for the case where n is 0
    if n > 0 && (n & (n - 1)) == 0 { return n; }
    while n != 0 {
        n >>= 1;
        count += 1;
    }
    return 1 << count;
}

fn log_2_of(num: usize) -> usize {
    mem::size_of::<usize>() * 8 - num.leading_zeros() as usize - 1
}
