// eventual-consistent map based on lfmap and lflist from Shisoft
use crate::collections::lflist;
use crate::utils::{current_cpu, NUM_CPU};
use core::cell::Cell;
use lfmap::{Map, ObjectMap, WordMap};
use std::marker::PhantomData;
use std::mem;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;
use std::cmp::min;

const MAX_BINS: usize = 16;
const BIN_INDEX_MASK: usize = MAX_BINS - 1;
type EvBins = Arc<Vec<EvBin>>;

#[derive(Clone)]
pub struct Producer {
    cache: EvBins,
}

pub struct EvMap {
    map: lfmap::WordMap,
    source: EvBins,
}

struct EvBin {
    list: lflist::ObjectList<(usize, usize)>,
}

impl EvMap {
    pub fn new() -> Self {
        let cap = min(*NUM_CPU, MAX_BINS);
        let mut source = Vec::with_capacity(cap);
        for _ in 0..cap {
            source.push(EvBin::new());
        }
        Self {
            map: WordMap::with_capacity(4096),
            source: Arc::new(source),
        }
    }

    pub fn new_producer(&self) -> Producer {
        Producer {
            cache: self.source.clone(),
        }
    }

    pub fn refresh(&self) {
        // get all items from producers and insert into the local map
        self.source.iter().for_each(|p| {
            p.list.drop_out_all(Some(|(_, (k, v))| {
                self.map.insert(k, v);
            }));
        });
    }

    pub fn insert_to_cpu(&self, key: usize, value: usize, cpu_id: usize) {
        self.source[cpu_id & BIN_INDEX_MASK].push(key, value);
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

impl Producer {
    #[inline]
    pub fn insert(&self, key: usize, value: usize) {
        // current_cpu is cheap in Linux: 16 ns/iter (+/- 1)
        // Have to get current cpu in real-time or the may affinity won't work
        self.insert_to_cpu(key, value, current_cpu());
    }
    #[inline]
    pub fn insert_to_cpu(&self, key: usize, value: usize, cpu_id: usize) {
        self.cache[cpu_id & BIN_INDEX_MASK].push(key, value);
    }
}

impl EvBin {
    pub fn new() -> Self {
        Self {
            list: lflist::ObjectList::with_capacity(128),
        }
    }

    pub fn push(&self, key: usize, value: usize) {
        self.list.push((key, value));
    }
}
