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

lazy_static! {
    pub static ref SYS_PAGE_SIZE: usize = unsafe { sysconf(_SC_PAGESIZE) as usize };
    pub static ref SYS_CPU_NODE: HashMap<usize, usize> = cpu_topology();
    pub static ref NUM_NUMA_NODES: usize = num_numa_nodes();
    pub static ref NUM_CPU: usize = SYS_CPU_NODE.len();
    pub static ref SYS_TOTAL_MEM: usize = total_memory();
}

pub fn align_padding(len: usize, align: usize) -> usize {
    let len_rounded_up = len.wrapping_add(align).wrapping_sub(1) & !align.wrapping_sub(1);
    len_rounded_up.wrapping_sub(len)
}

pub fn current_thread_id() -> usize {
    unsafe { libc::pthread_self() as usize }
}

pub fn cpu_topology() -> HashMap<usize, usize> {
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
                    .map(|cpu_id| (cpu_id, node_num))
                    .collect::<Vec<_>>();
                cpus
            })
            .flatten()
            .collect(),
        Err(_) => (0..num_cpus::get()).map(|n| (n, 0)).collect(),
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
        SYS_CPU_NODE
            .iter()
            .filter_map(|(cpu, node)| if *node == node_id { Some(*cpu) } else { None })
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
pub fn alloc_mem<T, A: Alloc + Default>(size: usize) -> usize {
    let mut a = A::default();
    let align = mem::align_of::<T>();
    let layout = Layout::from_size_align(size, align).unwrap();
    // must be all zeroed
    unsafe { a.alloc_zeroed(layout) }.unwrap().as_ptr() as usize
}

#[inline]
pub fn dealloc_mem<T, A: Alloc + Default>(ptr: usize, size: usize) {
    let mut a = A::default();
    let align = mem::align_of::<T>();
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
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::Relaxed;
    use test::Bencher;
    use rand::{thread_rng, SeedableRng, Rng};
    use rand_xoshiro::Xoroshiro64StarStar;
    use rand_xorshift::XorShiftRng;
    use lfmap::{WordMap, Map};
    use std::alloc::System;
    use std::collections::HashMap;
    use crate::collections::lflist::WordList;

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
        let map = WordMap::<System>::with_capacity(128);
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
}
