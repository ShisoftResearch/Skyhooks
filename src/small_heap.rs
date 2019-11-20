use super::*;
use crate::collections::evmap::Producer;
use crate::collections::fixvec::FixedVec;
use crate::collections::lflist::WordList;
use crate::collections::{evmap, lflist};
use crate::generic_heap::{log_2_of, size_class_index_from_size, ObjectMeta, NUM_SIZE_CLASS};
use crate::mmap::mmap_without_fd;
use crate::utils::*;
use core::mem;
use core::mem::MaybeUninit;
use core::sync::atomic::AtomicUsize;
use crossbeam_queue::SegQueue;
use lfmap::{Map, ObjectMap, WordMap};
use std::cell::{Cell, RefCell};
use std::clone::Clone;
use std::os::unix::thread::JoinHandleExt;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::thread;
use std::alloc::GlobalAlloc;

type TSizeClasses = [SizeClass; NUM_SIZE_CLASS];

thread_local! {
    static THREAD_META: ThreadMeta = ThreadMeta::new()
}

lazy_static! {
    static ref PER_NODE_META: Vec<NodeMeta> = gen_numa_node_list();
    static ref PER_CPU_META: Vec<CoreMeta> = gen_core_meta();
    static ref NODES_BITS: usize = node_bits();
    static ref SUPERBLOCK_SIZE: usize = *MAXIMUM_SIZE << 2;
    static ref NODE_SHIFT_BITS: usize = node_shift_bits();
    static ref ADDRESSIBLE_CHUNK_SIZE: usize = addressable_chunk_size();
    static ref ADDRESSIBLE_CHUNK_BITS: usize = chunk_offset_bits();
    pub static ref MAXIMUM_SIZE: usize = maximum_size();
}

struct SuperBlock {
    tier: usize,
    cpu:  usize,
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
    pending_free: RemoteNodeFree,
    objects: evmap::EvMap,
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

struct RemoteNodeFree {
    pending_free: lflist::WordList<BumpAllocator>,
    // sentinel_thread: thread::Thread,
}

pub fn allocate(size: usize) -> Ptr {
    let size_class_index = size_class_index_from_size(size);
    let max_size = *MAXIMUM_SIZE;
    debug_assert!(size <= *MAXIMUM_SIZE);
    THREAD_META.with(|meta| {
        let cpu_id = meta.cpu;
        let cpu_meta = &PER_CPU_META[cpu_id];
        let object_list = &PER_NODE_META[meta.numa].objects;
        // allocate memory from per-CPU size class list
        let superblock = &cpu_meta.size_class_list[size_class_index];
        let (addr, block) = superblock.allocate();
        debug_assert_eq!(superblock.numa, meta.numa);
        debug_assert_eq!(addr_numa_id(addr), meta.numa);
        // insert to CPU cache to avoid synchronization
        object_list.insert_to_cpu(addr, block, cpu_id);
        return addr as Ptr;
    })
}

pub fn contains(ptr: Ptr) -> bool {
    let addr = ptr as usize;
    let numa_id = addr_numa_id(addr);
    let object_list = &PER_NODE_META[numa_id].objects;
    object_list.refresh();
    object_list.contains(addr)
}

pub fn free(ptr: Ptr) -> bool {
    let addr = ptr as usize;
    let numa_id = addr_numa_id(addr);
    let object_list = &PER_NODE_META[numa_id].objects;
    THREAD_META.with(|meta| {
        let current_node = meta.numa;
        object_list.refresh();
        PER_NODE_META[current_node].pending_free.free_all();
        if let Some(superblock_addr) = object_list.get(addr) {
            let superblock_ref = unsafe { & *(superblock_addr as *const SuperBlock) };
            let obj_numa = superblock_ref.numa;
            debug_assert_eq!(superblock_ref.tier, size_class_index_from_size(superblock_ref.size));
            if superblock_ref.numa == current_node {
                superblock_ref.dealloc(addr);
            } else {
                PER_NODE_META[obj_numa].pending_free.push(addr);
            }
            return true;
        } else {
            return false;
        }
    })
}
pub fn size_of(ptr: Ptr) -> Option<usize> {
    let addr = ptr as usize;
    let numa_id = addr_numa_id(addr);
    let object_list = &PER_NODE_META[numa_id].objects;
    object_list.refresh();
    object_list.get(ptr as usize).map(|superblock_addr| {
        let superblock_ref = unsafe { & *(superblock_addr as *const SuperBlock) };
        superblock_ref.size
    })
}

impl ThreadMeta {
    pub fn new() -> Self {
        let cpu_id = current_cpu();
        let numa_id = numa_from_cpu_id(cpu_id);
        let tid = current_thread_id();
        Self {
            numa: numa_id,
            cpu: cpu_id,
            tid,
        }
    }
}

impl SizeClass {
    pub fn new(tier: usize, size: usize, cpu: usize, numa: usize) -> Self {
        Self {
            tier,
            size,
            cpu,
            numa,
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
                    debug_assert_eq!(addr_numa_id(addr), self.numa);
                    return (addr, block_addr);
                }
            }
            let new_block = if let Some(numa_common_block) = PER_NODE_META[self.numa]
                .size_class_list[self.tier]
                .blocks
                .pop()
            {
                let superblock_ref = unsafe { &mut *(numa_common_block as *mut SuperBlock) };
                superblock_ref.cpu = self.cpu;
                debug_assert_eq!(superblock_ref.numa, self.numa);
                numa_common_block
            } else {
                SuperBlock::new(self.tier, self.cpu, self.numa, self.size) as usize
            };
            self.blocks.push(new_block);
        }
    }
}

impl RemoteNodeFree {
    pub fn new(node_id: usize) -> Self {
        Self { pending_free: lflist::WordList::new() }
    }

    #[inline]
    pub fn push(&self, addr: usize) {
        self.pending_free.push(addr as usize);
    }

    pub fn free_all(&self) {
        self.pending_free.drop_out_all(Some(|(addr, _)| {
            free(addr as Ptr);
        }));
    }
}

impl SuperBlock {
    pub fn new(tier: usize, cpu :usize, numa: usize, size: usize) -> *mut Self {
        // created a cache aligned super block
        // super block will not deallocated

        // For NUMA address hints to lower contention across nodes in object map,
        // this function will generate n heaps, which n is the number of NUMA nodes
        // It will return the heap for the CPU and put rest of them into common heap of each nodes

        let self_size = mem::size_of::<Self>();
        let padding = align_padding(self_size, CACHE_LINE_SIZE);
        // Cache align on data
        let self_size_with_padding = self_size + padding;
        let chunk_size = *ADDRESSIBLE_CHUNK_SIZE;
        let addr = mmap_without_fd(chunk_size) as usize;
        let node_shift_bits = *NODE_SHIFT_BITS;
        let node_bits = *NODES_BITS;
        let superblock_size = *SUPERBLOCK_SIZE;
        let pivot_pos = chunk_size >> 1;
        let data_mask = !(pivot_pos - 1);
        let numa_id_encoding_mask = !(!0 >> node_bits << node_bits) << node_shift_bits;
        let addr_shifted = (addr + pivot_pos) & !numa_id_encoding_mask & data_mask;
        let mut cpu_heap_addr = 0;
        for numa_id in 0..*NUM_NUMA_NODES {
            let node_offset = numa_id << node_shift_bits;
            let node_base = addr_shifted + node_offset;
            let data_base = node_base + self_size_with_padding;
            let boundary = node_base + superblock_size;
            debug_assert_eq!(align_padding(data_base, CACHE_LINE_SIZE), 0);
            debug_assert_eq!(addr_numa_id(data_base), numa_id);
            debug_assert_eq!(addr_numa_id(boundary - 1), numa_id);
            unsafe {
                *(node_base as *mut Self) = Self {
                    tier,
                    numa: numa_id,
                    cpu,
                    size,
                    data_base,
                    boundary,
                    reservation: AtomicUsize::new(data_base),
                    used: AtomicUsize::new(0),
                    free_list: lflist::WordList::new(),
                };
            }
            if numa_id == numa {
                cpu_heap_addr = node_base;
            } else {
                PER_NODE_META[numa_id].size_class_list[tier].blocks.push(node_base);
            }
        }
        cpu_heap_addr as *mut Self
    }

    fn allocate(&self) -> Option<usize> {
        let res = self.free_list.pop().or_else(|| loop {
            let addr = self.reservation.load(Relaxed);
            debug_assert_eq!(addr_numa_id(self.data_base), self.numa);
            if addr >= self.boundary {
                return None;
            } else {
                let new_addr = addr + self.size;
                debug_assert_eq!(addr_numa_id(addr), self.numa);
                if self.reservation.compare_and_swap(addr, new_addr, Relaxed) == addr {
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

fn gen_numa_node_list() -> Vec<NodeMeta> {
    let num_nodes = *NUM_NUMA_NODES;
    let mut nodes = Vec::with_capacity(num_nodes);
    for i in 0..num_nodes {
        nodes.push(NodeMeta {
            size_class_list: size_classes(0, i),
            pending_free: RemoteNodeFree::new(i),
            objects: evmap::EvMap::new(),
        });
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

#[inline]
fn node_shift_bits() -> usize {
    log_2_of(*SUPERBLOCK_SIZE)
}

fn chunk_offset_bits() -> usize {
    log_2_of(*ADDRESSIBLE_CHUNK_SIZE)
}

#[inline]
fn addr_numa_id(addr: usize) -> usize {
    let node_shift_bits = *NODE_SHIFT_BITS;
    let node_bit_mask = (1 << *NODES_BITS) - 1;
    let shifted = addr >> node_shift_bits;
    let masked = shifted & node_bit_mask;
    masked
}

#[inline]
fn addressable_chunk_size() -> usize {
    let node_chunk_size = *SUPERBLOCK_SIZE;
    let node_bits = *NODES_BITS;
    node_chunk_size << (node_bits + 2)
}

fn node_bits() -> usize {
    let num_nodes = *NUM_NUMA_NODES;
    if num_nodes == 1 { return  1; }
    let res = log_2_of(num_nodes);
    res
}

fn gen_core_meta() -> Vec<CoreMeta> {
    (0..*NUM_CPU)
        .map(|cpu_id| CoreMeta {
            size_class_list: size_classes(cpu_id, SYS_CPU_NODE[&cpu_id]),
        })
        .collect()
}

#[cfg(test)]
mod test {
    use crate::small_heap::{allocate, free};
    use lfmap::Map;
    use crate::api::NullocAllocator;

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
        let map = lfmap::WordMap::<NullocAllocator>::with_capacity(64);
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
