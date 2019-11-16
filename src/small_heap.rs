use super::*;
use crate::collections::evmap::Producer;
use crate::collections::fixvec::FixedVec;
use crate::collections::{evmap, lflist};
use crate::generic_heap::{log_2_of, size_class_index_from_size, ObjectMeta, NUM_SIZE_CLASS};
use crate::mmap::mmap_without_fd;
use crate::utils::*;
use core::mem;
use core::mem::MaybeUninit;
use core::sync::atomic::AtomicUsize;
use crossbeam_queue::SegQueue;
use lfmap::{Map, ObjectMap};
use std::cell::{Cell, RefCell};
use std::clone::Clone;
use std::os::unix::thread::JoinHandleExt;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::thread;

type SharedFreeList = lflist::WordList<BumpAllocator>;
type TSizeClasses = [SizeClass; NUM_SIZE_CLASS];
type TCommonSizeClasses = [CommonSizeClass; NUM_SIZE_CLASS];
type SizeClassFreeLists = [SharedFreeList; NUM_SIZE_CLASS];

thread_local! {
    static THREAD_META: ThreadMeta = ThreadMeta::new()
}

lazy_static! {
    static ref TOTAL_HEAP_SIZE: usize = total_heap();
    static ref HEAP_BASE: usize = mmap_without_fd(*TOTAL_HEAP_SIZE) as usize;
    static ref HEAP_UPPER_BOUND: usize = *HEAP_BASE + *TOTAL_HEAP_SIZE;
    static ref PER_NODE_META: Vec<NodeMeta> = gen_numa_node_list();
    static ref PER_CPU_META: Vec<CoreMeta> = gen_core_meta();
    static ref NODE_SHIFT_BITS: usize = node_shift_bits();
    pub static ref MAXIMUM_SIZE: usize = maximum_size();
}

struct ThreadMeta {
    sizes: TSizeClasses,
    objects: Producer<Object>,
    numa: usize,
    tid: usize,
    cpu: usize,
}

struct NodeMeta {
    alloc_pos: AtomicUsize,
    common: TCommonSizeClasses,
    pending_free: Option<RemoteNodeFree>,
    objects: evmap::EvMap<Object>,
}

struct SizeClass {
    size: usize,
    // reserved page for every size class to ensure utilization
    reserved: ReservedPage,
}

struct CommonSizeClass {
    // unused up reserves from dead threads
    reserved: SegQueue<ReservedPage>,
    free_list: lflist::WordList<BumpAllocator>,
}

struct CoreMeta {
    free_lists: SizeClassFreeLists,
}

#[derive(Clone)]
struct ReservedPage {
    addr: RefCell<usize>,
    pos: RefCell<usize>,
}

struct RemoteNodeFree {
    pending_free: Arc<lflist::WordList<BumpAllocator>>,
    // sentinel_thread: thread::Thread,
}

#[derive(Clone, Default)]
struct Object {
    cpu: usize,
    tier: usize,
}

pub fn allocate(size: usize) -> Ptr {
    let size_class_index = size_class_index_from_size(size);
    debug_assert!(size <= *MAXIMUM_SIZE);
    THREAD_META.with(|meta| {
        let size_class = &meta.sizes[size_class_index];
        let cpu_id = meta.cpu;
        let cpu_meta = &PER_CPU_META[cpu_id];
        // allocate memory inside the thread meta
        let addr = if let Some(freed) = cpu_meta.free_lists[size_class_index].pop() {
            // first, looking in the free list
            freed
        } else if let Some(reserved) = size_class.reserved.take(size_class.size) {
            // next, ask the reservation station for objects
            reserved
        } else {
            let node = &PER_NODE_META[meta.numa];
            // allocate from node common list
            if let Some(freed) = node.common[size_class_index].free_list.pop() {
                freed
            } else {
                // finally, allocate from node memory space in the reservation station
                size_class
                    .reserved
                    .allocate_from_common(size_class.size, size_class_index, &node)
            }
        };
        debug_assert_ne!(addr, 0);
        meta.objects
            .insert_to_cpu(addr, meta.object_map(size_class_index, cpu_id), meta.cpu);
        return addr as Ptr;
    })
}
pub fn contains(ptr: Ptr) -> bool {
    let addr = ptr as usize;
    if !address_in_range(addr) {
        return false;
    }
    let node_id = addr_numa_id(addr);
    let node = &PER_NODE_META[node_id];
    node.objects.refresh();
    node.objects.contains(addr)
}
pub fn free(ptr: Ptr) -> bool {
    let addr = ptr as usize;
    if !address_in_range(addr) {
        return false;
    }
    let node_id = addr_numa_id(addr);
    let node = &PER_NODE_META[node_id];
    THREAD_META.with(|meta| {
        let current_node = meta.numa;
        node.objects.refresh();
        if let Some(pending_free) = &PER_NODE_META[current_node].pending_free {
            pending_free.free_all();
        }
        if node_id != current_node {
            // append address to remote node if this address does not belong to current node
            let contains_obj = node.objects.contains(addr);
            if contains_obj {
                node.pending_free.as_ref().unwrap().push(addr);
            }
            return contains_obj;
        } else {
            if let Some(obj_meta) = node.objects.get(ptr as usize) {
                let tier = obj_meta.tier;
                let cpu_id = meta.cpu;
                let cpu_meta = &PER_CPU_META[cpu_id];
                &PER_CPU_META[obj_meta.cpu].free_lists[tier].push(ptr as usize);
                return true;
            } else {
                return false;
            }
        }
    })
}
pub fn size_of(ptr: Ptr) -> Option<usize> {
    let addr = ptr as usize;
    if !address_in_range(addr) {
        return None;
    }
    let node_id = addr_numa_id(addr);
    let node_meta = &PER_NODE_META[node_id];
    node_meta.objects.refresh();
    node_meta
        .objects
        .get(addr)
        .map(|o| THREAD_META.with(|meta| meta.sizes[o.tier].size))
}

#[inline]
pub fn address_in_range(addr: usize) -> bool {
    addr >= *HEAP_BASE && addr < *HEAP_UPPER_BOUND
}

impl ThreadMeta {
    pub fn new() -> Self {
        let cpu_id = current_cpu();
        let numa_id = numa_from_cpu_id(cpu_id);
        let numa = &PER_NODE_META[numa_id];
        let objects = numa.objects.new_producer();
        let size_classes = size_classes();
        let tid = current_thread_id();
        Self {
            numa: numa_id,
            sizes: size_classes,
            cpu: cpu_id,
            objects,
            tid,
        }
    }

    pub fn object_map(&self, tier: usize, cpu: usize) -> Object {
        Object { tier, cpu }
    }
}

// Return thread resource to global
impl Drop for ThreadMeta {
    fn drop(&mut self) {
        let page_size = *SYS_PAGE_SIZE;
        api::INNER_CALL.with(|is_inner| {
            is_inner.set(true);
            let numa_id = self.numa;
            let numa = &PER_NODE_META[numa_id];
            for (i, size_class) in self.sizes.into_iter().enumerate() {
                let common = &numa.common[i];
                let reserved = &size_class.reserved;
                let reserved_addr = *reserved.addr.borrow();
                let reserved_pos = *reserved.pos.borrow();
                if reserved_addr > 0 && reserved_pos < reserved_addr + page_size {
                    common.reserved.push(reserved.clone());
                }
            }
        });
    }
}

impl SizeClass {
    pub fn new(size: usize) -> Self {
        Self {
            size,
            reserved: ReservedPage::new(),
        }
    }
}

impl ReservedPage {
    // ATTENTION:   all of the function call in this structure should be in a single thread
    //              synchronization is not required, use RefCell for internal mutation

    pub fn new() -> Self {
        ReservedPage {
            addr: RefCell::new(0),
            pos: RefCell::new(0),
        }
    }
    pub fn take(&self, size: usize) -> Option<usize> {
        debug_assert!(is_power_of_2(size));
        let page_size = *SYS_PAGE_SIZE;
        if size >= page_size {
            return None;
        }
        let addr = *self.addr.borrow();
        if addr == 0 {
            return None;
        }
        let pos = *self.pos.borrow();
        if pos - addr >= page_size {
            return None;
        }
        *self.pos.borrow_mut() = pos + size;
        return Some(pos);
    }
    pub fn allocate_from_common(
        &self,
        size: usize,
        size_class_index: usize,
        node: &NodeMeta,
    ) -> usize {
        debug_assert!(is_power_of_2(size));
        let page_size = *SYS_PAGE_SIZE;
        let bumper = &node.alloc_pos;
        if size >= page_size {
            return bumper.fetch_add(size, Relaxed);
        }
        let mut addr = self.addr.borrow_mut();
        let mut pos = self.pos.borrow_mut();
        debug_assert!(
            *addr == 0 || *pos - *addr >= page_size,
            "only allocate when the reserved space is not enough"
        );

        if let Ok(reserved) = node.common[size_class_index].reserved.pop() {
            let old_reserved_pos = *reserved.pos.borrow();
            *addr = *(reserved.addr.borrow());
            *pos = old_reserved_pos + size;
            return old_reserved_pos;
        } else {
            let new_page_base = bumper.fetch_add(page_size, Relaxed);
            *addr = new_page_base;
            *pos = new_page_base + size;
            return *addr;
        }
    }
}

impl RemoteNodeFree {
    pub fn new(node_id: usize) -> Self {
        let list = Arc::new(lflist::WordList::new());
        Self { pending_free: list }
    }

    #[inline]
    pub fn push(&self, addr: usize) {
        self.pending_free.push(addr as usize);
        // self.sentinel_thread.unpark();
    }

    pub fn free_all(&self) {
        self.pending_free.drop_out_all(Some(|(addr, _)| {
            free(addr as Ptr);
        }));
    }
}

fn gen_numa_node_list() -> Vec<NodeMeta> {
    let num_nodes = *NUM_NUMA_NODES;
    let node_shift_bits = *NODE_SHIFT_BITS;
    let heap_base = *HEAP_BASE;
    let mut nodes = Vec::with_capacity(num_nodes);
    for i in 0..num_nodes {
        let node_base = heap_base + (i << node_shift_bits);
        let remote_free = if num_nodes > 0 {
            Some(RemoteNodeFree::new(i))
        } else {
            None
        };
        nodes.push(NodeMeta {
            alloc_pos: AtomicUsize::new(node_base),
            common: common_size_classes(),
            pending_free: remote_free,
            objects: evmap::EvMap::new(),
        });
    }
    return nodes;
}

fn size_classes() -> TSizeClasses {
    let mut data: [MaybeUninit<SizeClass>; NUM_SIZE_CLASS] =
        unsafe { MaybeUninit::uninit().assume_init() };
    let mut size = 2;
    for elem in &mut data[..] {
        *elem = MaybeUninit::new(SizeClass::new(size));
        size *= 2;
    }
    unsafe { mem::transmute::<_, TSizeClasses>(data) }
}

fn common_size_classes() -> TCommonSizeClasses {
    let mut data: [MaybeUninit<CommonSizeClass>; NUM_SIZE_CLASS] =
        unsafe { MaybeUninit::uninit().assume_init() };
    for elem in &mut data[..] {
        *elem = MaybeUninit::new(CommonSizeClass {
            reserved: SegQueue::new(),
            free_list: lflist::WordList::new(),
        });
    }
    unsafe { mem::transmute::<_, TCommonSizeClasses>(data) }
}

fn size_class_free_lists() -> SizeClassFreeLists {
    let mut data: [MaybeUninit<SharedFreeList>; NUM_SIZE_CLASS] =
        unsafe { MaybeUninit::uninit().assume_init() };
    let mut id = 0;
    for elem in &mut data[..] {
        *elem = MaybeUninit::new(lflist::WordList::new());
        id += 1;
    }
    unsafe { mem::transmute::<_, SizeClassFreeLists>(data) }
}

#[inline]
fn total_heap() -> usize {
    min_power_of_2(total_memory() / 4)
}

fn min_power_of_2(mut n: usize) -> usize {
    let mut count = 0;
    // First n in the below condition
    // is for the case where n is 0
    if n > 0 && (n & (n - 1)) == 0 {
        return n;
    }
    while n != 0 {
        n >>= 1;
        count += 1;
    }
    return 1 << count;
}

#[inline]
fn addr_numa_id(addr: usize) -> usize {
    let offset = addr - *HEAP_BASE;
    let shift_bits = *NODE_SHIFT_BITS;
    let res = offset >> shift_bits;
    res
}

#[inline]
fn node_shift_bits() -> usize {
    let total_heap_bits = log_2_of(*TOTAL_HEAP_SIZE);
    let numa_nodes_bits = log_2_of(*NUM_NUMA_NODES);
    total_heap_bits - numa_nodes_bits
}

#[inline]
fn maximum_size() -> usize {
    size_classes()[NUM_SIZE_CLASS - 1].size
}

fn gen_core_meta() -> Vec<CoreMeta> {
    (0..*NUM_CPU)
        .map(|_| CoreMeta {
            free_lists: size_class_free_lists(),
        })
        .collect()
}

#[cfg(test)]
mod test {
    use crate::small_heap::{allocate, free};

    #[test]
    pub fn general() {
        env_logger::try_init();
        let ptr = allocate(9);
        unsafe {
            for i in 0..1000 {
                *(ptr as *mut u64) = i;
                assert_eq!(*(ptr as *mut u64), i);
            }
        }
        free(ptr);
        let ptr2 = allocate(10);
        unsafe {
            for i in 0..1000 {
                *(ptr2 as *mut u64) = i + 2;
                assert_eq!(*(ptr2 as *mut u64), i + 2);
            }
        }
        assert_eq!(ptr, ptr2);
    }
}
