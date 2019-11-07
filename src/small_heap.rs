use super::*;
use crate::collections::evmap;
use crate::collections::fixvec::FixedVec;
use crate::collections::lflist;
use crate::generic_heap::ObjectMeta;
use crate::mmap::mmap_without_fd;
use crate::utils::*;
use core::mem;
use core::mem::MaybeUninit;
use core::sync::atomic::AtomicUsize;
use crossbeam_queue::SegQueue;
use lfmap::{Map, ObjectMap};
use std::cell::RefCell;
use std::os::unix::thread::JoinHandleExt;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::thread;
use std::clone::Clone;

const NUM_SIZE_CLASS: usize = 16;

type SharedFreeList = Arc<lflist::List<usize>>;
type TSizeClasses = [SizeClass; NUM_SIZE_CLASS];
type TCommonSizeClasses = [CommonSizeClass; NUM_SIZE_CLASS];
type TThreadFreeLists = [SharedFreeList; NUM_SIZE_CLASS];

thread_local! {
    static THREAD_META: ThreadMeta = ThreadMeta::new()
}

lazy_static! {
    static ref TOTAL_HEAP_SIZE: usize = total_heap();
    static ref HEAP_BASE: usize = mmap_without_fd(*TOTAL_HEAP_SIZE) as usize;
    static ref HEAP_UPPER_BOUND: usize = *HEAP_BASE + *TOTAL_HEAP_SIZE;
    static ref PER_NODE_META: FixedVec<NodeMeta> = gen_numa_node_list();
    static ref NODE_SHIFT_BITS: usize = node_shift_bits();
    pub static ref MAXIMUM_SIZE: usize = maximum_size();
}

struct ThreadMeta {
    sizes: TSizeClasses,
    objects: Arc<evmap::Producer<ObjectMeta>>,
    numa: usize,
    tid: usize,
}

struct NodeMeta {
    alloc_pos: AtomicUsize,
    common: TCommonSizeClasses,
    pending_free: Option<RemoteNodeFree>,
    thread_free: lfmap::ObjectMap<TThreadFreeLists>,
    objects: evmap::EvMap<ObjectMeta>,
}

struct SizeClass {
    size: usize,
    // reserved page for every size class to ensure utilization
    reserved: ReservedPage,
    free_list: Arc<lflist::List<usize>>,
}

struct CommonSizeClass {
    // unused up reserves from dead threads
    reserved: SegQueue<ReservedPage>,
    free_list: lflist::List<usize>,
}

#[derive(Clone)]
struct ReservedPage {
    addr: RefCell<usize>,
    pos: RefCell<usize>,
}

struct RemoteNodeFree {
    pending_free: Arc<lflist::List<usize>>,
    sentinel_thread: thread::Thread,
}

pub fn allocate(size: usize) -> Ptr {
    THREAD_META.with(|meta| {
        // allocate memory inside the thread meta
        let size_class_index = size_class_index_from_size(size);
        let size_class = &meta.sizes[size_class_index];
        let addr = if let Some(freed) = size_class.free_list.pop() {
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
        meta.objects.insert(
            addr,
            meta.object_map(addr, size_class_index, size_class.size),
        );
        return addr as Ptr;
    })
}
pub fn contains(ptr: Ptr) -> bool {
    let addr = ptr as usize;
    let node_id = addr_numa_id(addr);
    let node = &PER_NODE_META[node_id];
    node.objects.refresh();
    node.objects.contains(addr)
}
pub fn free(ptr: Ptr) -> bool {
    let addr = ptr as usize;
    if addr < *HEAP_BASE || addr > *HEAP_UPPER_BOUND {
        return false;
    }
    THREAD_META.with(|meta| {
        let current_node = meta.numa;
        let node_id = addr_numa_id(addr);
        let node = &PER_NODE_META[node_id];
        node.objects.refresh();
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
                if obj_meta.tid == meta.tid {
                    // object from this thread, insert into this free-list
                    meta.sizes[tier].free_list.push(ptr as usize);
                } else if let Some(thread_free_list) = node.thread_free.get(obj_meta.tid) {
                    // found belong thread, insert into the thread free list
                    thread_free_list[tier].push(ptr as usize);
                } else {
                    // cannot find the belong thread, insert into self free list
                    meta.sizes[tier].free_list.push(ptr as usize);
                }
                return true;
            } else {
                return false;
            }
        }
    })
}
pub fn size_of(ptr: Ptr) -> Option<usize> {
    let addr = ptr as usize;
    let node_id = addr_numa_id(addr);
    let node_meta = &PER_NODE_META[node_id];
    node_meta.objects.refresh();
    node_meta.objects.get(addr).map(|o| o.size)
}

impl ThreadMeta {
    pub fn new() -> Self {
        let numa_id = current_numa();
        let numa = &PER_NODE_META[numa_id];
        let objects = numa.objects.new_producer();
        let size_classes = size_classes();
        let thread_free_lists = thread_free_lists(&size_classes);
        let tid = current_thread_id();
        numa.thread_free.insert(tid, thread_free_lists);
        Self {
            numa: numa_id,
            objects,
            sizes: size_classes,
            tid,
        }
    }

    pub fn object_map(&self, ptr: usize, tier: usize, size: usize) -> ObjectMeta {
        ObjectMeta {
            size,
            addr: ptr,
            numa: self.numa,
            tier,
            tid: self.tid,
        }
    }
}

// Return thread resource to global
impl Drop for ThreadMeta {
    fn drop(&mut self) {
        let page_size = *SYS_PAGE_SIZE;
        api::INNER_CALL.with(|is_inner| {
            is_inner.store(true, Relaxed);
            let numa_id = self.numa;
            let numa = &PER_NODE_META[numa_id];
            numa.objects.remove_producer(&self.objects);
            numa.thread_free.remove(self.tid);
            for (i, size_class) in self.sizes.into_iter().enumerate() {
                let common = &numa.common[i];
                let reserved = &size_class.reserved;
                let reserved_addr = *reserved.addr.borrow();
                let reserved_pos = *reserved.pos.borrow();
                if reserved_addr > 0 && reserved_pos < reserved_addr + page_size {
                    common.reserved.push(reserved.clone());
                }
                if size_class.free_list.count() > 0 {
                    common.free_list.prepend_with(&size_class.free_list);
                }
            }
        });
    }
}

impl SizeClass {
    pub fn new(tier: usize) -> Self {
        Self {
            size: tier,
            reserved: ReservedPage::new(),
            free_list: Arc::new(lflist::List::new()),
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
        let list = Arc::new(lflist::List::new());
        let list_clone = list.clone();
        let handle = thread::Builder::new()
            .name(format!("Remote Free {}", node_id))
            .spawn(move || loop {
                if let Some(addr) = list_clone.pop() {
                    debug_assert_eq!(
                        addr_numa_id(addr),
                        node_id,
                        "Node freeing remote pending object"
                    );
                    free(addr as Ptr);
                } else {
                    thread::park();
                }
            })
            .unwrap();
        let pthread = handle.as_pthread_t();
        let thread = handle.thread().clone();
        set_node_affinity(node_id, pthread as u64);
        Self {
            pending_free: list,
            sentinel_thread: thread,
        }
    }

    pub fn push(&self, addr: usize) {
        self.pending_free.push(addr as usize);
        self.sentinel_thread.unpark();
    }
}

fn gen_numa_node_list() -> FixedVec<NodeMeta> {
    let num_nodes = *NUM_NUMA_NODES;
    let node_shift_bits = *NODE_SHIFT_BITS;
    let heap_base = *HEAP_BASE;
    let mut nodes = FixedVec::new(num_nodes);
    for i in 0..num_nodes {
        let node_base = heap_base + (i << node_shift_bits);
        let remote_free = if num_nodes > 0 { Some(RemoteNodeFree::new(i)) } else { None };
        nodes[i] = NodeMeta {
            alloc_pos: AtomicUsize::new(node_base),
            common: common_size_classes(),
            pending_free: remote_free,
            thread_free: ObjectMap::with_capacity(128),
            objects: evmap::EvMap::new(),
        };
    }
    return nodes;
}

fn size_classes() -> TSizeClasses {
    let mut data: [MaybeUninit<SizeClass>; NUM_SIZE_CLASS] =
        unsafe { MaybeUninit::uninit().assume_init() };
    let mut tier = 2;
    for elem in &mut data[..] {
        *elem = MaybeUninit::new(SizeClass::new(tier));
        tier *= 2;
    }
    unsafe { mem::transmute::<_, TSizeClasses>(data) }
}

fn common_size_classes() -> TCommonSizeClasses {
    let mut data: [MaybeUninit<CommonSizeClass>; NUM_SIZE_CLASS] =
        unsafe { MaybeUninit::uninit().assume_init() };
    for elem in &mut data[..] {
        *elem = MaybeUninit::new(CommonSizeClass {
            reserved: SegQueue::new(),
            free_list: lflist::List::new(),
        });
    }
    unsafe { mem::transmute::<_, TCommonSizeClasses>(data) }
}

fn thread_free_lists(size_classes: &TSizeClasses) -> TThreadFreeLists {
    let mut data: [MaybeUninit<SharedFreeList>; NUM_SIZE_CLASS] =
        unsafe { MaybeUninit::uninit().assume_init() };
    let mut id = 0;
    for elem in &mut data[..] {
        *elem = MaybeUninit::new(size_classes[id].free_list.clone());
        id += 1;
    }
    unsafe { mem::transmute::<_, TThreadFreeLists>(data) }
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
fn log_2_of(num: usize) -> usize {
    mem::size_of::<usize>() * 8 - num.leading_zeros() as usize - 1
}

#[inline]
fn addr_numa_id(addr: usize) -> usize {
    debug_assert!(addr >= *HEAP_BASE, "{} v.s {}", addr, *HEAP_BASE);
    let offset = addr - *HEAP_BASE;
    let shift_bits = *NODE_SHIFT_BITS;
    let res = offset >> shift_bits;
    debug_assert!(res <= *NUM_NUMA_NODES - 1);
    res
}

#[inline]
fn size_class_index_from_size(size: usize) -> usize {
    debug_assert!(size > 0);
    let log = log_2_of(size);
    if is_power_of_2(size) && log > 0 {
        log - 1
    } else {
        log
    }
}

fn node_shift_bits() -> usize {
    let total_heap_bits = log_2_of(*TOTAL_HEAP_SIZE);
    let numa_nodes_bits = log_2_of(*NUM_NUMA_NODES);
    total_heap_bits - numa_nodes_bits
}

fn maximum_size() -> usize {
    size_classes()[NUM_SIZE_CLASS - 1].size
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