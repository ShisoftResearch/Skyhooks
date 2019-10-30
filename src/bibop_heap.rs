use super::*;
use crate::collections::fixvec::FixedVec;
use crate::collections::{lflist, lfmap};
use crate::generic_heap::ObjectMeta;
use crate::utils::{current_numa, current_thread_id, SYS_TOTAL_MEM, NUM_NUMA_NODES, is_power_of_2};
use core::mem::MaybeUninit;
use core::mem;
use crate::mmap::mmap_without_fd;
use core::sync::atomic::AtomicUsize;
use crate::collections::lfmap::Map;

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
    static ref NODE_SHIFT_BITS: usize = log_2_of(*TOTAL_HEAP_SIZE) - log_2_of(*NUM_NUMA_NODES);
}

struct ThreadMeta {
    sizes: TSizeClasses,
    numa: usize,
    tid: usize,
}

struct NodeMeta {
    id: usize,
    base: usize,
    alloc_pos: AtomicUsize,
    sizes: TSizeClasses,
    pending_free: lflist::List,
    objects: lfmap::ObjectMap<ObjectMeta>
}

struct SizeClass {
    size: usize,
    // reserved page for every size class to ensure utilization
    reserved: ReservedPage,
    free_list: lflist::List,
}

struct ReservedPage {
    addr: AtomicUsize,
    pos: AtomicUsize,
}

pub struct Heap {
    addr: usize
}

pub fn allocate(size: usize) -> Ptr {
    THREAD_META.with(|meta| {
        // allocate memory inside the thread meta

        unimplemented!()
    })
}
pub fn contains(ptr: Ptr) -> bool {
    THREAD_META.with(|meta| {
        unimplemented!()
    })
}
pub fn free(ptr: Ptr) -> bool {
    let addr = ptr as usize;
    let current_node = THREAD_META.with(|meta| { meta.numa });
    let node_id = addr_numa_id(addr);
    if node_id != current_node {
        // append address to remote node if this address does not belong to current node
        let remote_node: &NodeMeta = &PER_NODE_META[node_id];
        remote_node.append_free(addr);
    } else {
        unimplemented!()
    }
    unimplemented!()
}
pub fn meta_of(ptr: Ptr) -> Option<ObjectMeta> {
    THREAD_META.with(|meta| {
        unimplemented!()
    })
}
pub fn size_of(ptr: Ptr) -> Option<usize> {
    THREAD_META.with(|meta| {
        unimplemented!()
    })
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
            reserved: ReservedPage::new(),
            free_list: lflist::List::new()
        }
    }
}

impl ReservedPage {
    pub fn new() -> Self {
        ReservedPage {
            addr: AtomicUsize::new(0),
            pos: AtomicUsize::new(0)
        }
    }
}

impl NodeMeta {
    pub fn append_free(&self, addr: usize) {
        // append freed object to this NUMA node, to be processed by this node
        // this operation minimized communication cost by 4 atomic operations in common cases
        // maybe 3 atomic operations after get rid of reference counting in lflist
        debug_assert_eq!(self.id, addr_numa_id(addr));
        self.pending_free.push(addr);
    }
}

fn gen_numa_node_list() -> FixedVec<NodeMeta> {
    let num_nodes = *NUM_NUMA_NODES;
    let node_shift_bits = *NODE_SHIFT_BITS;
    let mut nodes = FixedVec::new(num_nodes);
    let mut heap_base = *HEAP_BASE;
    for i in 0..num_nodes {
        let node_base = heap_base + (i << node_shift_bits);
        nodes[i] = NodeMeta {
            id: i,
            base: node_base,
            alloc_pos: AtomicUsize::new(node_base),
            sizes: size_classes(),
            pending_free: lflist::List::new(),
            objects: lfmap::ObjectMap::with_capacity(512)
        };
    }
    return nodes;
}

fn size_classes() -> TSizeClasses {
    let mut data: [MaybeUninit<SizeClass>; NUM_SIZE_CLASS] = unsafe { MaybeUninit::uninit().assume_init() };
    let mut tier = 2;
    for elem in &mut data[..] {
        *elem = MaybeUninit::new(SizeClass::new(tier));
        tier *= 2;
    };
    unsafe { mem::transmute::<_, TSizeClasses>(data) }
}

#[inline]
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

#[inline]
fn log_2_of(num: usize) -> usize {
    mem::size_of::<usize>() * 8 - num.leading_zeros() as usize - 1
}

#[inline]
fn addr_numa_id(addr: usize) -> usize {
    (addr - *HEAP_BASE) >> *NODE_SHIFT_BITS
}

#[inline]
fn size_class_index_from_size(size: usize) -> usize {
    let log = log_2_of(size);
    if is_power_of_2(size) { log - 1 } else { log }
}