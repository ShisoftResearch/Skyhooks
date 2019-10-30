use libc::{syscall, sysconf, _SC_PAGESIZE};
use regex::Regex;
use std::collections::HashMap;
use std::fs::{read_dir, ReadDir};
use std::io::Error;
use sysinfo::{ProcessExt, SystemExt};

lazy_static! {
    pub static ref SYS_PAGE_SIZE: usize = unsafe { sysconf(_SC_PAGESIZE) as usize };
    pub static ref SYS_CPU_NODE: HashMap<usize, usize> = cpu_topology();
    pub static ref NUM_NUMA_NODES: usize = num_numa_nodes();
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
    vec.dedup();
    vec.len()
}

pub fn total_memory() -> usize {
    let mut system = sysinfo::System::new();
    system.refresh_system();
    system.get_total_memory() as usize * 1024 // in bytes
}

#[cfg(target_os = "linux")]
pub fn current_numa() -> usize {
    let cpu = unsafe { (libc::sched_getcpu() as usize) };
    SYS_CPU_NODE.get(&cpu).map(|x| *x).unwrap_or(0)
}

#[cfg(not(target_os = "linux"))]
pub fn current_numa() -> usize {
    0
}

#[inline]
pub fn is_power_of_2(x: usize) -> bool {
    (x & (x - 1)) == 0
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn numa_nodes() {
        for (cpu, node) in SYS_CPU_NODE.iter() {
            // println!("{} in {}", cpu, node);
        }
    }

    #[test]
    fn numa() {
        let numa = current_numa();
        println!("current numa {}", numa);
    }
}
