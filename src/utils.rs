use libc::{_SC_PAGESIZE, sysconf, syscall};
use std::fs::{read_dir, ReadDir};
use std::io::Error;
use regex::Regex;
use std::collections::HashMap;

lazy_static!{
    pub static ref SYS_PAGE_SIZE: usize = unsafe { sysconf(_SC_PAGESIZE) as usize };
    pub static ref SYS_CPU_NODE: HashMap<usize, usize> = {
        let node_regex = Regex::new(r"node[0-9]*$").unwrap();
        let cpu_regex = Regex::new(r"cpu[0-9]*$").unwrap();
        let number_regex = Regex::new(r"\d+").unwrap();
        match read_dir("/sys/devices/system/node") {
            Ok(dir) => {
                dir.filter_map(|entry|
                    entry.ok()
                        .and_then(|e| {
                            let p = e.path();
                            if p.is_dir() {
                                p.file_name().and_then(|os_str|
                                    os_str.to_str().map(|s|
                                        (s.to_string(), e)))
                            } else {
                                None
                            }
                        }))
                    .filter(|(file_name, _)| node_regex.is_match(file_name))
                    .map(|(node_name, de)| {
                        let node_num = number_regex
                            .captures_iter(&node_name)
                            .next()
                            .unwrap()[0]
                            .parse::<usize>()
                            .unwrap();
                        let path = de.path();
                        let node_dir = read_dir(path).unwrap();
                        let cpus = node_dir
                            .filter_map(|f|
                                f.ok().map(|f|
                                    f.file_name().to_str().unwrap().to_string()))
                            .filter(|f| cpu_regex.is_match(f))
                            .map(|f| number_regex
                                .captures_iter(&node_name)
                                .next()
                                .unwrap()[0]
                                .parse::<usize>()
                                .unwrap())
                            .map(|cpu_id| (cpu_id, node_num))
                            .collect::<Vec<_>>();
                        cpus
                    })
                    .flatten()
                    .collect()
            }
            Err(_) => (0..num_cpus::get()).map(|n| (n, 0)).collect()
        }
    };
}

pub fn align_padding(len: usize, align: usize) -> usize {
    let len_rounded_up = len.wrapping_add(align).wrapping_sub(1)
        & !align.wrapping_sub(1);
    len_rounded_up.wrapping_sub(len)
}

pub fn current_thread_id() -> usize {
    unsafe { libc::pthread_self() as usize }
}

pub fn current_numa() -> usize {
    unimplemented!()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn numa_nodes() {
        for node in SYS_NUMA_NODES.iter() {
            println!("{}", node);
        }
    }
}