// eventual-consistent map based on lfmap and lflist from Shisoft
use crate::collections::lflist;
use crate::utils::*;
use core::cell::Cell;
use lfmap::{Map, WordMap};
use std::marker::PhantomData;
use std::mem;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;
use std::cmp::min;
use std::alloc::Global;
use crate::bump_heap::BumpAllocator;

const MAX_BINS: usize = 32;
const BIN_INDEX_MASK: usize = MAX_BINS - 1;
type EvBins = Vec<LazyWrapper<NumaBin>>;

pub struct EvMap {
    map: lfmap::WordMap<BumpAllocator, AddressHasher>,
    source: EvBins,
}

struct EvBin {
    list: lflist::ObjectList<(usize, usize)>
}

struct NumaBin {
    bins: Vec<LazyWrapper<EvBin>>,
    cpu_mask: usize
}

impl EvMap {
    pub fn new() -> Self {
        let nodes = *NUM_NUMA_NODES;
        let mut source = Vec::with_capacity(nodes);
        for i in 0..nodes {
            source.push(LazyWrapper::new(Box::new(move || {
                let node = i;
                let node_cpus = SYS_NODE_CPUS[&node].len();
                let cpu_slots = min(upper_power_of_2(node_cpus), 16);
                let mut cpu_source = Vec::with_capacity(cpu_slots);
                debug_assert!(is_power_of_2(cpu_slots));
                for j in 0..cpu_slots {
                    cpu_source.push(LazyWrapper::new(Box::new(|| EvBin::new())));
                }
                NumaBin {
                    bins: cpu_source,
                    cpu_mask: cpu_slots - 1
                }
            })));
        }
        Self {
            map: WordMap::with_capacity(256),
            source,
        }
    }

    pub fn refresh(&self, lookup: Option<usize>) -> Option<usize> {
        // get all items from producers and insert into the local map
        let mut lookup_res = 0;
        if self.source
            .iter()
            .map(|c| c.bins.iter())
            .flatten()
            .any(|p| {
                p.list.drop_out_all(Some(|(_, (k, v))| {
                    if lookup == Some(k) {
                        lookup_res = v;
                    }
                    self.map.insert(k, v);
                }));
                lookup_res != 0
            })
        {
            Some(lookup_res)
        } else {
            None
        }
    }

    pub fn insert_to_cpu(&self, key: usize, value: usize, numa_id: usize, cpu_id: usize) {
        let node = &self.source[numa_id];
        node.bins[cpu_id & node.cpu_mask].push(key, value);
    }

    #[inline]
    pub fn insert(&self, key: usize, value: usize) -> Option<()> {
        self.map.insert(key, value)
    }

    #[inline]
    pub fn get(&self, key: usize) -> Option<usize> {
        self.map.get(key)
    }

    #[inline]
    pub fn remove(&self, key: usize) -> Option<usize> {
        self.map.remove(key)
    }

    #[inline]
    pub fn contains(&self, key: usize) -> bool {
        self.map.contains(key)
    }
}

impl EvBin {
    pub fn new() -> Self {
        Self {
            list: lflist::ObjectList::with_capacity(8) // fit into 2 cache lines
        }
    }

    pub fn push(&self, key: usize, value: usize) {
        self.list.push((key, value));
    }
}
