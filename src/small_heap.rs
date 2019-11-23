use super::*;
use crate::collections::evmap::Producer;
use crate::collections::fixvec::FixedVec;
use crate::collections::lflist::WordList;
use crate::collections::{evmap, lflist};
use crate::generic_heap::{log_2_of, size_class_index_from_size, ObjectMeta, NUM_SIZE_CLASS};
use crate::utils::*;
use core::ptr;
use core::mem;
use core::mem::MaybeUninit;
use core::sync::atomic::AtomicUsize;
use crossbeam_queue::SegQueue;
use lfmap::{Map, WordMap};
use std::alloc::GlobalAlloc;
use std::cell::{Cell, RefCell};
use std::clone::Clone;
use std::os::unix::thread::JoinHandleExt;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::thread;
use lazy_init::Lazy;
use std::ops::Deref;

type TSizeClasses = [SizeClass; NUM_SIZE_CLASS];

thread_local! {
    static THREAD_META: ThreadMeta = ThreadMeta::new()
}

lazy_static! {
    static ref PER_NODE_META: Vec<LazyWrapper<NodeMeta>> = gen_numa_node_list();
    static ref PER_CPU_META: Vec<LazyWrapper<CoreMeta>> = gen_core_meta();
    static ref SUPERBLOCK_SIZE: usize = *MAXIMUM_SIZE << 2;
    static ref OBJECT_MAP: evmap::EvMap = evmap::EvMap::new();
    pub static ref MAXIMUM_SIZE: usize = maximum_size();
}

struct SuperBlock {
    cpu: usize,
    size: usize,
    data_base: usize,
    numa: usize,
    boundary: usize,
    reservation: AtomicUsize,
    used: AtomicUsize,
    free_list: lflist::WordList<BumpAllocator>,
}

struct ThreadMeta {
    numa: usize,
    tid: usize,
    cpu: usize,
}

struct NodeMeta {
    size_class_list: TSizeClasses,
    bump_allocator: bump_heap::AllocatorInstance<BumpAllocator>,
    pending_free: lflist::WordList<BumpAllocator>,
}

struct SizeClass {
    tier: usize,
    size: usize,
    numa: usize,
    cpu: usize,
    // SuperBlock ptr address list
    blocks: lflist::WordList<BumpAllocator>,
}

struct CoreMeta {
    size_class_list: TSizeClasses,
}

struct LazyWrapper<T: Sync> {
    inner: Lazy<T>,
    init: Box<dyn Fn() -> T>
}

pub fn allocate(size: usize) -> Ptr {
    let size_class_index = size_class_index_from_size(size);
    let max_size = *MAXIMUM_SIZE;
    debug_assert!(size <= *MAXIMUM_SIZE);
    THREAD_META.with(|meta| {
        let cpu_id = meta.cpu;
        let cpu_meta = &PER_CPU_META[cpu_id];
        // allocate memory from per-CPU size class list
        let superblock = &cpu_meta.size_class_list[size_class_index];
        let (addr, block) = superblock.allocate();
        debug_assert_eq!(superblock.numa, meta.numa);
        debug_assert_eq!(unsafe { &*(block as *const SuperBlock) }.numa, meta.numa);
        if cfg!(debug_assertions) {
            debug_check_cache_aligned(addr, size, 8);
            debug_check_cache_aligned(addr, size, 16);
            debug_check_cache_aligned(addr, size, 32);
            debug_check_cache_aligned(addr, size, CACHE_LINE_SIZE);
        }
        return addr as Ptr;
    })
}

pub fn contains(ptr: Ptr) -> bool {
    let addr = ptr as usize;
    OBJECT_MAP.refresh();
    OBJECT_MAP.contains(addr)
}

pub fn free(ptr: Ptr) -> bool {
    let current_numa = THREAD_META.with(|meta| meta.numa);
    PER_NODE_META[current_numa]
        .pending_free
        .drop_out_all(Some(|(addr, _)| {
            if let Some(superblock_addr) = OBJECT_MAP.get(addr) {
                let superblock_ref = unsafe { &*(superblock_addr as *const SuperBlock) };
                superblock_ref.dealloc(addr);
            }
        }));
    OBJECT_MAP.refresh();
    let addr = ptr as usize;
    if let Some(superblock_addr) = OBJECT_MAP.get(addr) {
        let superblock_ref = unsafe { &*(superblock_addr as *const SuperBlock) };
        if superblock_ref.numa == current_numa {
            superblock_ref.dealloc(addr);
        } else {
            PER_NODE_META[superblock_ref.numa].pending_free.push(addr);
        }
        return true;
    } else {
        return false;
    }
}
pub fn size_of(ptr: Ptr) -> Option<usize> {
    let addr = ptr as usize;
    OBJECT_MAP.refresh();
    OBJECT_MAP.get(ptr as usize).map(|superblock_addr| {
        let superblock_ref = unsafe { &*(superblock_addr as *const SuperBlock) };
        superblock_ref.size
    })
}

impl ThreadMeta {
    pub fn new() -> Self {
        let tid = current_thread_id();
        let cpu_id = cpu_id_from_tid(tid);
        let numa_id = numa_from_cpu_id(cpu_id);
        set_node_affinity(numa_id, tid as u64);
        Self {
            numa: numa_id,
            cpu: cpu_id,
            tid,
        }
    }
}

impl SizeClass {
    pub fn new(tier: usize, size: usize, cpu: usize, numa: usize) -> Self {
        debug_assert!(size > 1);
        Self {
            tier,
            size,
            numa,
            cpu,
            blocks: WordList::new(),
        }
    }

    pub fn allocate(&self) -> (usize, usize) {
        // allocate in the superblocks
        loop {
            for (block_addr, _) in self.blocks.iter() {
                let superblock = unsafe { &*(block_addr as *mut SuperBlock) };
                debug_assert_eq!(superblock.numa, self.numa);
                if let Some(addr) = superblock.allocate() {
                    return (addr, block_addr);
                }
            }
            let new_block = if let Some(numa_common_block) = PER_NODE_META[self.numa]
                .size_class_list[self.tier]
                .blocks
                .pop()
            {
                let superblock_ref = unsafe { &mut *(numa_common_block as *mut SuperBlock) };
                debug_assert_eq!(superblock_ref.numa, self.numa);
                superblock_ref.cpu = self.cpu;
                numa_common_block
            } else {
                debug_assert!(self.size > 1);
                SuperBlock::new(self.tier, self.cpu, self.numa, self.size) as usize
            };
            self.blocks.push(new_block);
        }
    }
}

impl SuperBlock {
    pub fn new(tier: usize, cpu: usize, numa: usize, size: usize) -> *mut Self {
        // created a cache aligned super block
        // super block will not deallocated
        let node_allocator = &PER_NODE_META[numa].bump_allocator;
        let self_size = mem::size_of::<Self>();
        let padding = align_padding(self_size, CACHE_LINE_SIZE);
        // Cache align on data
        let self_size_with_padding = self_size + padding;
        let chunk_size = self_size_with_padding + *SUPERBLOCK_SIZE;
        // use bump_allocate function for it just allocate, do't record object address
        let addr = node_allocator.bump_allocate(chunk_size);
        let data_base = addr + self_size_with_padding;
        let boundary = data_base + *SUPERBLOCK_SIZE;
        let ptr = addr as *mut Self;

        // ensure cache aligned
        debug_assert_eq!(align_padding(addr, CACHE_LINE_SIZE), 0);
        debug_assert_eq!(align_padding(data_base, CACHE_LINE_SIZE), 0);

        unsafe {
            ptr::write(ptr, Self {
                numa,
                size,
                data_base,
                boundary,
                cpu,
                reservation: AtomicUsize::new(data_base),
                used: AtomicUsize::new(0),
                free_list: lflist::WordList::new(),
            });
        }

        return ptr;
    }

    fn allocate(&self) -> Option<usize> {
        let res = self.free_list.pop().or_else(|| loop {
            let addr = self.reservation.load(Relaxed);
            if addr >= self.boundary {
                return None;
            } else {
                let new_addr = addr + self.size;
                if self.reservation.compare_and_swap(addr, new_addr, Relaxed) == addr {
                    // insert to per CPU cache to avoid synchronization
                    OBJECT_MAP.insert_to_cpu(addr, self as *const Self as usize, self.cpu);
                    return Some(addr);
                }
            }
        });
        if res.is_some() {
            self.used.fetch_add(self.size, Relaxed);
            debug_validate(res.unwrap() as Ptr, self.size);
        }
        return res;
    }

    fn dealloc(&self, addr: usize) {
        debug_assert!(addr >= self.data_base && addr < self.data_base + *SUPERBLOCK_SIZE);
        debug_assert_eq!((addr - self.data_base) % self.size, 0);
        self.free_list.push(addr);
        self.used.fetch_sub(self.size, Relaxed);
    }
}

impl <T: Sync> LazyWrapper<T> {
    pub fn new(create: Box<dyn Fn() -> T>) -> Self {
        Self {
            inner: Lazy::new(),
            init: create
        }
    }
}

impl <T: Sync> Deref for LazyWrapper<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.inner.get_or_create(|| {
            (self.init)()
        })
    }
}

unsafe impl <T: Sync> Sync for LazyWrapper<T> {}

fn gen_numa_node_list() -> Vec<LazyWrapper<NodeMeta>> {
    let num_nodes = *NUM_NUMA_NODES;
    let mut nodes = Vec::with_capacity(num_nodes);
    for i in 0..num_nodes {
        nodes.push(LazyWrapper::new(Box::new(move || {
            NodeMeta {
                size_class_list: size_classes(0, i),
                bump_allocator: bump_heap::AllocatorInstance::new(),
                pending_free: lflist::WordList::new(),
            }
        })));
    }
    return nodes;
}

fn size_classes(cpu: usize, numa: usize) -> TSizeClasses {
    let mut data: [MaybeUninit<SizeClass>; NUM_SIZE_CLASS] =
        unsafe { MaybeUninit::uninit().assume_init() };
    let mut size = 2;
    let mut tier = 0;
    for elem in &mut data[..] {
        *elem = MaybeUninit::new(SizeClass::new(tier, size, cpu, numa));
        tier += 1;
        size <<= 1;
    }
    unsafe { mem::transmute::<_, TSizeClasses>(data) }
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
fn maximum_size() -> usize {
    2 << (NUM_SIZE_CLASS - 1)
}

fn gen_core_meta() -> Vec<LazyWrapper<CoreMeta>> {
    (0..*NUM_CPU)
        .map(|cpu_id| LazyWrapper::new(Box::new(move || {
            CoreMeta {
                size_class_list: size_classes(cpu_id, SYS_CPU_NODE[&cpu_id]),
            }
        })))
        .collect()
}

fn debug_check_cache_aligned(addr: usize, size: usize, align: usize) {
    if size >= align {
        // ensure all address are cache aligned
        debug_assert_eq!(align_padding(addr, align), 0);
    }
}

#[cfg(test)]
mod test {
    use crate::api::NullocAllocator;
    use crate::small_heap::{allocate, free};
    use lfmap::Map;
    use crate::utils::AddressHasher;

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

    #[test]
    pub fn application() {
        let map = lfmap::WordMap::<NullocAllocator, AddressHasher>::with_capacity(64);
        for i in 5..10240 {
            map.insert(i, i * 2);
        }
        for i in 5..10240 {
            assert_eq!(map.get(i), Some(i * 2), "index: {}", i);
        }
        for i in 5..10240 {
            assert_eq!(map.remove(i), Some(i * 2), "index: {}", i);
        }
    }
}
