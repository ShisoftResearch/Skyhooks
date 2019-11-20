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

type TSizeClasses = [SizeClass; NUM_SIZE_CLASS];

thread_local! {
    static THREAD_META: ThreadMeta = ThreadMeta::new()
}

lazy_static! {
    static ref PER_NODE_META: Vec<NodeMeta> = gen_numa_node_list();
    static ref PER_CPU_META: Vec<CoreMeta> = gen_core_meta();
    static ref SUPERBLOCK_SIZE: usize = *MAXIMUM_SIZE << 2;
    static ref OBJECT_LIST: evmap::EvMap = evmap::EvMap::new();
    pub static ref MAXIMUM_SIZE: usize = maximum_size();
}

struct SuperBlock {
    head: usize,
    tier: usize,
    size: usize,
    numa: usize,
    cpu:  usize,
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
    pending_free: Option<RemoteNodeFree>,
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
    pending_free: Arc<lflist::WordList<BumpAllocator>>,
    // sentinel_thread: thread::Thread,
}

pub fn allocate(size: usize) -> Ptr {
    let size_class_index = size_class_index_from_size(size);
    debug_assert!(size <= *MAXIMUM_SIZE);
    THREAD_META.with(|meta| {
        let cpu_id = meta.cpu;
        let cpu_meta = &PER_CPU_META[cpu_id];
        // allocate memory from per-CPU size class list
        let (addr, block) = cpu_meta.size_class_list[size_class_index].allocate();
        // insert to CPU cache to avoid synchronization
        OBJECT_LIST.insert_to_cpu(addr, block, cpu_id);
        return addr as Ptr;
    })
}

pub fn contains(ptr: Ptr) -> bool {
    let addr = ptr as usize;
    OBJECT_LIST.refresh();
    OBJECT_LIST.contains(addr)
}

pub fn free(ptr: Ptr) -> bool {
    let addr = ptr as usize;
    THREAD_META.with(|meta| {
        let current_node = meta.numa;
        OBJECT_LIST.refresh();
        if let Some(pending_free) = &PER_NODE_META[current_node].pending_free {
            pending_free.free_all();
        }
        if let Some(superblock_addr) = OBJECT_LIST.get(addr) {
            let superblock_ref = unsafe { & *(superblock_addr as *const SuperBlock) };
            let obj_numa = superblock_ref.numa;
            if superblock_ref.numa == current_node {
                superblock_ref.dealloc(addr);
            } else {
                if let Some(pending_free) = &PER_NODE_META[obj_numa].pending_free {
                    pending_free.push(addr);
                } else {
                    superblock_ref.dealloc(addr);
                }
            }
            return true;
        } else {
            return false;
        }
    })
}
pub fn size_of(ptr: Ptr) -> Option<usize> {
    unimplemented!()
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
                superblock_ref.cpu = self.cpu;
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
        let list = Arc::new(lflist::WordList::new());
        Self { pending_free: list }
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
        let self_size = mem::size_of::<Self>();
        let padding = align_padding(self_size, CACHE_LINE_SIZE);
        let self_size_with_padding = self_size + padding;
        let total_size = self_size_with_padding + *SUPERBLOCK_SIZE;
        let addr = alloc_mem::<CacheLineType, BumpAllocator>(total_size);
        unsafe {
            *(addr as *mut Self) = Self {
                tier,
                numa,
                size,
                cpu,
                head: addr + self_size_with_padding,
                reservation: AtomicUsize::new(addr),
                used: AtomicUsize::new(0),
                free_list: lflist::WordList::new(),
            };
        }
        addr as *mut Self
    }

    fn allocate(&self) -> Option<usize> {
        let res = self.free_list.pop().or_else(|| loop {
            let addr = self.reservation.load(Relaxed);
            if addr >= self.head + *SUPERBLOCK_SIZE {
                return None;
            } else {
                let new_addr = addr + self.size;
                if self.reservation.compare_and_swap(addr, new_addr, Relaxed) == addr {
                    return Some(addr);
                }
            }
        });
        if res.is_some() {
            self.used.fetch_add(self.size, Relaxed);
        }
        return res;
    }

    fn dealloc(&self, addr: usize) {
        debug_assert!(addr >= self.head && addr < self.head + *SUPERBLOCK_SIZE);
        debug_assert_eq!((addr - self.head) % self.size, 0);
        self.free_list.push(addr);
        self.used.fetch_sub(self.size, Relaxed);
    }
}

fn gen_numa_node_list() -> Vec<NodeMeta> {
    let num_nodes = *NUM_NUMA_NODES;
    let mut nodes = Vec::with_capacity(num_nodes);
    for i in 0..num_nodes {
        let remote_free = if num_nodes > 0 {
            Some(RemoteNodeFree::new(i))
        } else {
            None
        };
        nodes.push(NodeMeta {
            size_class_list: size_classes(0, i),
            pending_free: remote_free,
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
        size << 1;
    }
    unsafe { mem::transmute::<_, TSizeClasses>(data) }
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
fn maximum_size() -> usize {
    size_classes(0, 0)[NUM_SIZE_CLASS - 1].size
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
