use super::*;
use crate::collections::fixvec::FixedVec;
use crate::collections::lflist::WordList;
use crate::collections::{evmap, lflist};
use crate::generic_heap::{log_2_of, size_class_index_from_size, ObjectMeta, NUM_SIZE_CLASS};
use crate::utils::*;
use core::mem;
use core::mem::MaybeUninit;
use core::ptr;
use core::sync::atomic::{AtomicU32, AtomicUsize};
use crossbeam_queue::SegQueue;
use lazy_init::Lazy;
use lfmap::{Map, WordMap};
use std::alloc::GlobalAlloc;
use std::cell::{Cell, RefCell};
use std::clone::Clone;
use std::ops::Deref;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::thread;
use smallvec::SmallVec;

type TSizeClasses = [SizeClass; NUM_SIZE_CLASS];
type PerNodeMeta = SmallVec<[LazyWrapper<NodeMeta>; 4]>;
type PerCPUMeta = SmallVec<[LazyWrapper<CoreMeta>; 64]>;

thread_local! {
    static THREAD_META: ThreadMeta = ThreadMeta::new()
}

lazy_static! {
    static ref PER_NODE_META: PerNodeMeta = gen_numa_node_list();
    static ref PER_CPU_META: PerCPUMeta = gen_core_meta();
    static ref SUPERBLOCK_SIZE: usize = *MAXIMUM_SIZE << 2;
    pub static ref MAXIMUM_SIZE: usize = maximum_size();
}

#[cfg_attr(target_arch = "x86_64", repr(align(128)))]
#[cfg_attr(not(target_arch = "x86_64"), repr(align(64)))]
struct SuperBlock {
    cpu: u16,
    numa: u16,
    size: u32,
    reservation: AtomicU32,
    used: AtomicU32,
    data_base: usize,
    free_list: lflist::WordList<BumpAllocator>,
}

struct ThreadMeta {
    numa: u16,
    cpu: u16,
}

struct NodeMeta {
    size_class_list: TSizeClasses,
    bump_allocator: bump_heap::AllocatorInstance<BumpAllocator>,
    pending_free: lflist::WordList<BumpAllocator>,
    objects: lfmap::WordMap<BumpAllocator>,
}

struct SizeClass {
    numa: u16,
    cpu: u16,
    tier: u32,
    size: u32,
    // SuperBlock ptr address list
    blocks: lflist::WordList<BumpAllocator>,
}

struct CoreMeta {
    size_class_list: TSizeClasses,
}

pub fn allocate(size: usize) -> Ptr {
    let size_class_index = size_class_index_from_size(size);
    let max_size = *MAXIMUM_SIZE;
    debug_assert!(size <= *MAXIMUM_SIZE);
    THREAD_META.with(|meta| {
        let cpu_id = meta.cpu;
        let cpu_meta = &PER_CPU_META[cpu_id as usize];
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

pub fn free(ptr: Ptr) -> bool {
    let current_numa = THREAD_META.with(|meta| meta.numa);
    let numa_meta = &PER_NODE_META[current_numa as usize];
    numa_meta.pending_free.drop_out_all(Some(|(addr, _)| {
        if let Some(superblock_addr) = numa_meta.objects.get(addr) {
            let superblock_ref = unsafe { &*(superblock_addr as *const SuperBlock) };
            superblock_ref.dealloc(addr);
        }
    }));
    let addr = ptr as usize;
    if let Some(superblock_addr) = get_from_objects(current_numa, addr) {
        let superblock_ref = unsafe { &*(superblock_addr as *const SuperBlock) };
        if superblock_ref.numa == current_numa {
            superblock_ref.dealloc(addr);
        } else {
            PER_NODE_META[superblock_ref.numa as usize]
                .pending_free
                .push(addr);
        }
        return true;
    } else {
        return false;
    }
}
pub fn size_of(ptr: Ptr) -> Option<usize> {
    let addr = ptr as usize;
    let current_numa = THREAD_META.with(|meta| meta.numa);
    get_from_objects(current_numa, addr).map(|superblock_addr| {
        let superblock_ref = unsafe { &*(superblock_addr as *const SuperBlock) };
        superblock_ref.size as usize
    })
}

impl ThreadMeta {
    pub fn new() -> Self {
        let cpu_id = current_cpu();
        let numa_id = numa_from_cpu_id(cpu_id);
        // set_node_affinity(numa_id, tid);
        Self {
            numa: numa_id,
            cpu: cpu_id,
        }
    }
}

impl SizeClass {
    pub fn new(tier: u32, size: u32, cpu: u16, numa: u16) -> Self {
        debug_assert!(size > 1);
        Self {
            tier,
            size,
            numa,
            cpu,
            blocks: WordList::new(false),
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
            let new_block = if let Some(numa_common_block) = PER_NODE_META[self.numa as usize]
                .size_class_list[self.tier as usize]
                .blocks
                .pop()
            {
                let superblock_ref = unsafe { &mut *(numa_common_block as *mut SuperBlock) };
                debug_assert_eq!(superblock_ref.numa, self.numa);
                superblock_ref.cpu = self.cpu;
                numa_common_block
            } else {
                debug_assert!(self.size > 1);
                SuperBlock::new(self.tier, self.size, self.cpu, self.numa) as usize
            };
            self.blocks.push(new_block);
        }
    }
}

impl SuperBlock {
    pub fn new(tier: u32, size: u32, cpu: u16, numa: u16) -> *mut Self {
        // created a cache aligned super block
        // super block will not deallocated
        let node_allocator = &PER_NODE_META[numa as usize].bump_allocator;
        let self_size = mem::size_of::<Self>();
        let padding = align_padding(self_size, CACHE_LINE_SIZE);
        // Cache align on data
        let self_size_with_padding = self_size + padding;
        let chunk_size = self_size_with_padding + *SUPERBLOCK_SIZE;
        // use bump_allocate function for it just allocate, do't record object address
        let addr = node_allocator.bump_allocate(chunk_size);
        let data_base = addr + self_size_with_padding;
        let ptr = addr as *mut Self;

        // ensure cache aligned
        debug_assert_eq!(align_padding(addr, CACHE_LINE_SIZE), 0);
        debug_assert_eq!(align_padding(data_base, CACHE_LINE_SIZE), 0);

        unsafe {
            ptr::write(
                ptr,
                Self {
                    numa,
                    size,
                    data_base,
                    cpu,
                    reservation: AtomicU32::new(0),
                    used: AtomicU32::new(0),
                    free_list: lflist::WordList::new(false),
                },
            );
        }

        return ptr;
    }

    fn allocate(&self) -> Option<usize> {
        let res = self.free_list.pop().or_else(|| loop {
            let pos = self.reservation.load(Relaxed);
            let pos_ext = pos as usize;
            if pos_ext as usize >= *SUPERBLOCK_SIZE {
                return None;
            } else {
                let new_pos = pos + self.size;
                if self.reservation.compare_and_swap(pos, new_pos, Relaxed) == pos {
                    // insert to per CPU cache to avoid synchronization
                    let address = pos_ext + self.data_base;
                    PER_NODE_META[self.numa as usize]
                        .objects
                        .insert(address, self as *const Self as usize);
                    return Some(address);
                }
            }
        });
        if res.is_some() {
            self.used.fetch_add(self.size, Relaxed);
            debug_validate(res.unwrap() as Ptr, self.size as usize);
        }
        return res;
    }

    fn dealloc(&self, addr: usize) {
        debug_assert!(addr >= self.data_base && addr < self.data_base + *SUPERBLOCK_SIZE);
        debug_assert_eq!((addr - self.data_base) % self.size as usize, 0);
        self.free_list.push(addr);
        self.used.fetch_sub(self.size, Relaxed);
    }
}

fn gen_numa_node_list() -> PerNodeMeta {
    let num_nodes = *NUM_NUMA_NODES;
    let mut nodes = PerNodeMeta::with_capacity(num_nodes as usize);
    for i in 0..num_nodes {
        nodes.push(LazyWrapper::new(Box::new(move || NodeMeta {
            size_class_list: size_classes(0, i),
            bump_allocator: bump_heap::AllocatorInstance::new(),
            pending_free: lflist::WordList::new(false),
            objects: lfmap::WordMap::with_capacity(*SYS_PAGE_SIZE),
        })));
    }
    return nodes;
}

fn size_classes(cpu: u16, numa: u16) -> TSizeClasses {
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

fn gen_core_meta() -> PerCPUMeta {
    let mut vec = PerCPUMeta::new();
    for cpu_id in 0..*NUM_CPU {
        vec.push(LazyWrapper::new(Box::new(move || CoreMeta {
            size_class_list: size_classes(cpu_id, SYS_CPU_NODE[&cpu_id]),
        })));
    }
    return vec;
}

fn debug_check_cache_aligned(addr: usize, size: usize, align: usize) {
    if size >= align {
        // ensure all address are cache aligned
        debug_assert_eq!(align_padding(addr, align), 0);
    }
}

fn get_from_objects(current_numa: u16, addr: usize) -> Option<usize> {
    let current_numa_ext = current_numa as usize;
    if let Some(addr) = PER_NODE_META[current_numa_ext].objects.get(addr) {
        return Some(addr);
    } else {
        for (numa_id, numa_meta) in PER_NODE_META
            .iter()
            .enumerate()
            .filter(|(i, _)| i != &current_numa_ext)
        {
            if let Some(addr) = PER_NODE_META[numa_id].objects.get(addr) {
                return Some(addr);
            }
        }
    }
    return None;
}

#[cfg(test)]
mod test {
    use crate::api::SkyhooksAllocator;
    use crate::small_heap::{allocate, free};
    use crate::utils::AddressHasher;
    use lfmap::Map;

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
        let map = lfmap::WordMap::<SkyhooksAllocator, AddressHasher>::with_capacity(64);
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
