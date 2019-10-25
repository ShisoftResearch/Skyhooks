use libc::{_SC_PAGESIZE, sysconf};
use std::fs::{read_dir, ReadDir};
use std::io::Error;
use regex::Regex;

lazy_static!{
    pub static ref SYS_PAGE_SIZE: usize = unsafe { sysconf(_SC_PAGESIZE) as usize };
    pub static ref SYS_NUMA_NODES: Vec<usize> = unsafe {
        let node_regex = Regex::new(r"node[0-9]*$").unwrap();
        let number_regex = Regex::new("d+").unwrap();
        match read_dir("/sys/devices/system/node") {
            Ok(dir) => {
                dir.filter_map(|entry|
                    entry.ok()
                        .and_then(|e| {
                            let p = e.path();
                            if p.is_dir() {
                                p.file_name().and_then(|os_str|
                                    os_str.to_str().map(|s|
                                        s.to_string()))
                            } else {
                                None
                            }
                        }))
                    .filter(|file_name| node_regex.is_match(file_name))
                    .map(|node_name| number_regex
                        .captures_iter(&node_name)
                        .next()
                        .unwrap()[0]
                        .parse::<usize>()
                        .unwrap())
                    .collect::<Vec<usize>>()
            }
            Err(_) => vec![]
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