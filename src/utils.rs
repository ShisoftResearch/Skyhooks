use crate::bump_heap::BumpAllocator;
use crate::{Ptr, Size};
use alloc::alloc::Global;
use core::alloc::{Alloc, GlobalAlloc, Layout};
use core::mem;
use core::ptr::NonNull;
use libc::{sysconf, _SC_PAGESIZE};
use regex::Regex;
use std::collections::HashMap;
use std::fs::read_dir;
use std::hash::Hasher;
use lfmap::hash;
use std::collections::hash_map::DefaultHasher;

pub const CACHE_LINE_SIZE: usize = 64;
pub type CacheLineType = (usize, usize, usize, usize, usize, usize, usize, usize);


const HASH_MAGIC_NUMBER_1: usize = 67280421310721;
const HASH_MAGIC_NUMBER_2: usize = 123456789;
const HASH_MAGIC_NUMBER_3: usize = 362436069;

lazy_static! {
    pub static ref SYS_PAGE_SIZE: usize = unsafe { sysconf(_SC_PAGESIZE) as usize };
    pub static ref SYS_NODE_CPUS: HashMap<usize, Vec<usize>> = node_topology();
    pub static ref SYS_CPU_NODE: HashMap<usize, usize> = cpu_topology();
    pub static ref NUM_NUMA_NODES: usize = num_numa_nodes();
    pub static ref NUM_CPU: usize = unsafe { libc::sysconf(libc::_SC_NPROCESSORS_ONLN) as usize };
    pub static ref SYS_TOTAL_MEM: usize = total_memory();
}

pub struct AddressHasher {
    num: u64
}

impl Hasher for AddressHasher {
    fn finish(&self) -> u64 {
        self.num
    }

    fn write(&mut self, bytes: &[u8]) {
        unimplemented!()
    }

    fn write_usize(&mut self, i: usize) {
        self.num = (i >> i.trailing_zeros()) as u64
    }
}

impl Default for AddressHasher {
    fn default() -> Self {
        Self { num: 0 }
    }
}

pub fn align_padding(len: usize, align: usize) -> usize {
    let len_rounded_up = len.wrapping_add(align).wrapping_sub(1) & !align.wrapping_sub(1);
    len_rounded_up.wrapping_sub(len)
}

pub fn current_thread_id() -> usize {
    unsafe { libc::pthread_self() as usize }
}

pub fn cpu_topology() -> HashMap<usize, usize> {
    let cpus = SYS_NODE_CPUS.iter()
        .map(|(node, cpus)| {
            let node = *node;
            let cpus = cpus.clone();
            cpus.into_iter()
                .map(|cpu| (cpu, node))
                .collect::<Vec<_>>()
                .into_iter()
        })
        .flatten()
        .collect();
    cpus
}

pub fn node_topology() -> HashMap<usize, Vec<usize>> {
    let node_regex = Regex::new(r"node[0-9]*$").unwrap();
    let cpu_regex = Regex::new(r"cpu[0-9]*$").unwrap();
    let number_regex = Regex::new(r"\d+").unwrap();
    match read_dir("/sys/devices/system/node") {
        Ok(dir) => dir
            .filter_map(|entry| {
                entry.ok().and_then(|e| {
                    let p = e.path();
                    if p.is_dir() {
                        p.file_name()
                            .and_then(|os_str| os_str.to_str().map(|s| (s.to_string(), e)))
                    } else {
                        None
                    }
                })
            })
            .filter(|(file_name, _)| node_regex.is_match(file_name))
            .map(|(node_name, de)| {
                let node_num = number_regex.captures_iter(&node_name).next().unwrap()[0]
                    .parse::<usize>()
                    .unwrap();
                let path = de.path();
                let node_dir = read_dir(path).unwrap();
                let cpus = node_dir
                    .filter_map(|f| f.ok().map(|f| f.file_name().to_str().unwrap().to_string()))
                    .filter(|f| cpu_regex.is_match(f))
                    .map(|f| {
                        let id = number_regex.captures_iter(&f).next().unwrap()[0]
                            .parse::<usize>()
                            .unwrap();
                        id
                    })
                    .map(|cpu_id| cpu_id)
                    .collect::<Vec<_>>();
                (node_num, cpus)
            })
            .collect(),
        Err(_) => vec![(0, (0..num_cpus::get()).map(|n| n).collect())].into_iter().collect(),
    }
}

pub fn num_numa_nodes() -> usize {
    let mut vec = SYS_CPU_NODE.iter().map(|(_, v)| *v).collect::<Vec<_>>();
    vec.sort();
    vec.dedup();
    vec.len()
}

pub fn total_memory() -> usize {
    let mem_info = sys_info::mem_info().unwrap();
    mem_info.avail as usize * 1024 // in bytes
}

#[cfg(target_os = "linux")]
pub fn current_cpu() -> usize {
    unsafe { (libc::sched_getcpu() as usize) }
}

#[cfg(not(target_os = "linux"))]
pub fn current_cpu() -> usize {
    current_thread_id() % *NUM_CPU
}

#[cfg(target_os = "linux")]
pub fn current_numa() -> usize {
    let cpu = current_cpu();
    numa_from_cpu_id(cpu)
}

#[cfg(target_os = "linux")]
pub fn numa_from_cpu_id(cpu_id: usize) -> usize {
    SYS_CPU_NODE.get(&cpu_id).map(|x| *x).unwrap_or(0)
}

#[cfg(not(target_os = "linux"))]
pub fn numa_from_cpu_id(cpu_id: usize) -> usize {
    0
}

#[cfg(not(target_os = "linux"))]
pub fn current_numa() -> usize {
    0
}

#[cfg(target_os = "linux")]
pub fn set_node_affinity(node_id: usize, thread_id: u64) {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        SYS_NODE_CPUS[&node_id]
            .iter()
            .map(|cpu| *cpu)
            .for_each(|cpu| libc::CPU_SET(cpu, &mut set));
        libc::pthread_setaffinity_np(thread_id, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}

#[cfg(not(target_os = "linux"))]
pub fn set_node_affinity(node_id: usize, thread_id: u64) {
    // TODO: Make it work for non-linux systems
}

#[inline]
pub fn is_power_of_2(x: usize) -> bool {
    (x & (x - 1)) == 0
}

#[inline]
pub fn alloc_mem<A: Alloc + Default>(size: usize) -> usize {
    let mut a = A::default();
    let align = 16;
    let layout = Layout::from_size_align(size, align).unwrap();
    // must be all zeroed
    unsafe { a.alloc_zeroed(layout) }.unwrap().as_ptr() as usize
}

#[inline]
pub fn dealloc_mem<A: Alloc + Default>(ptr: usize, size: usize) {
    let mut a = A::default();
    let align = 16;
    let layout = Layout::from_size_align(size, align).unwrap();
    unsafe { a.dealloc(NonNull::<u8>::new(ptr as *mut u8).unwrap(), layout) }
}

#[inline(always)]
pub fn debug_validate(ptr: Ptr, size: Size) -> Ptr {
    unsafe {
        debug!(
            "Validated address: {:x}",
            libc::memset(ptr, 0, size) as usize
        );
        ptr
    }
}

#[cfg(test)]
mod test {
    use crate::api::NullocAllocator;
    use crate::collections::lflist::WordList;
    use lfmap::{Map, WordMap, PassthroughHasher};
    use rand::{thread_rng, Rng, SeedableRng};
    use rand_xorshift::XorShiftRng;
    use rand_xoshiro::Xoroshiro64StarStar;
    use std::alloc::{GlobalAlloc, Layout, System, Global};
    use std::collections::HashMap;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::Relaxed;
    use test::Bencher;
    use std::time::Instant;
    use crate::utils::AddressHasher;

    #[test]
    fn numa_nodes() {
        for (cpu, node) in super::SYS_CPU_NODE.iter() {
            // println!("{} in {}", cpu, node);
        }
    }

    #[test]
    fn numa() {
        let numa = super::current_numa();
        println!("current numa {}", numa);
    }

    #[bench]
    fn get_cpu(b: &mut Bencher) {
        b.iter(|| {
            super::current_cpu();
        });
    }

    #[bench]
    fn baseline(b: &mut Bencher) {
        b.iter(|| {
            let _: (usize, usize, usize, usize) = (1, 2, 3, 4).clone();
        });
    }

    #[bench]
    fn atomic_lock(b: &mut Bencher) {
        let atomic = AtomicUsize::new(0);
        b.iter(|| {
            atomic.fetch_add(1, Relaxed);
        });
    }

    #[bench]
    fn atomic_cas(b: &mut Bencher) {
        let atomic = AtomicUsize::new(0);
        b.iter(|| {
            atomic.compare_and_swap(0, 1, Relaxed);
        });
    }

    #[bench]
    fn atomic_load(b: &mut Bencher) {
        let atomic = AtomicUsize::new(100);
        let mut res = 0;
        b.iter(|| {
            res = atomic.load(Relaxed);
        });
    }

    #[bench]
    fn atomic_store(b: &mut Bencher) {
        let atomic = AtomicUsize::new(100);
        b.iter(|| {
            atomic.store(1, Relaxed);
        });
    }

    #[bench]
    fn random_xoshiro(b: &mut Bencher) {
        let mut thread_rng = thread_rng();
        b.iter(|| {
            let mut rng = Xoroshiro64StarStar::from_rng(&mut thread_rng).unwrap();
            rng.gen_range(0, 256);
        });
    }

    #[bench]
    fn random_xorshift(b: &mut Bencher) {
        let mut thread_rng = thread_rng();
        let mut rng = XorShiftRng::from_rng(&mut thread_rng).unwrap();
        b.iter(|| {
            rng.gen_range(0, 256);
        });
    }

    #[bench]
    fn lfmap(b: &mut Bencher) {
        let map = WordMap::<Global, PassthroughHasher>::with_capacity(128);
        let mut i = 5;
        b.iter(|| {
            map.insert(i, i);
            i += 1;
        });
    }

    #[bench]
    fn hashmap(b: &mut Bencher) {
        let mut map = HashMap::new();
        let mut i = 5;
        b.iter(|| {
            map.insert(i, i);
            i += 1;
        });
    }

    #[bench]
    fn lflist_push(b: &mut Bencher) {
        let list = WordList::<System>::new();
        let mut i = 5;
        b.iter(|| {
            list.push(i);
            i += 1;
        });
    }

    #[bench]
    fn lflist_exclusive_push(b: &mut Bencher) {
        let list = WordList::<System>::new();
        let mut i = 5;
        b.iter(|| {
            list.exclusive_push(i);
            i += 1;
        });
    }

    #[bench]
    fn alloc(b: &mut Bencher) {
        let allocator = NullocAllocator;
        b.iter(|| unsafe {
            allocator.alloc(Layout::from_size_align(1, 1).unwrap());
        });
    }

    #[bench]
    fn timing(b: &mut Bencher) {
        let now = Instant::now();
        b.iter(|| unsafe {
            let _ = now.elapsed().as_nanos();
        });
    }
}
