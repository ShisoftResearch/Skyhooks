// eventual-consistent map based on lfmap and lflist from Shisoft
use crate::collections::lflist;
use crate::utils::{current_cpu, NUM_CPU};
use core::cell::Cell;
use lfmap::{Map, ObjectMap};
use std::marker::PhantomData;
use std::mem;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

type EvBins<V> = Arc<Vec<lflist::ObjectList<(usize, V)>>>;

#[derive(Clone)]
pub struct Producer<V: Default> {
    cache: EvBins<V>,
    shadow: PhantomData<V>,
}

pub struct EvMap<V: Clone + Default> {
    map: lfmap::ObjectMap<V>,
    source: EvBins<V>,
}

impl<V: Clone + Default> EvMap<V> {
    pub fn new() -> Self {
        let mut source = Vec::with_capacity(*NUM_CPU);
        for _ in 0..*NUM_CPU {
            source.push(lflist::ObjectList::new());
        }
        Self {
            map: ObjectMap::with_capacity(4096),
            source: Arc::new(source),
        }
    }

    pub fn new_producer(&self) -> Producer<V> {
        Producer {
            cache: self.source.clone(),
            shadow: PhantomData,
        }
    }

    pub fn refresh(&self) {
        // get all items from producers and insert into the local map
        let mut dropped = vec![];
        self.source.iter().for_each(|p| {
            p.drop_out_all(Some(&mut dropped));
        });
        for (_, (k, v)) in dropped {
            self.map.insert(k, v);
        }
    }

    #[inline]
    pub fn insert(&self, key: usize, value: V) -> Option<()> {
        self.map.insert(key, value)
    }

    #[inline]
    pub fn get(&self, key: usize) -> Option<V> {
        self.map.get(key)
    }

    #[inline]
    pub fn remove(&self, key: usize) -> Option<V> {
        self.map.remove(key)
    }

    #[inline]
    pub fn contains(&self, key: usize) -> bool {
        self.map.contains(key)
    }
}

impl<V: Default> Producer<V> {
    #[inline]
    pub fn insert(&self, key: usize, value: V) {
        // current_cpu is cheap in Linux: 16 ns/iter (+/- 1)
        // Have to get current cpu in real-time or the may affinity won't work
        self.insert_to_cpu(key, value, current_cpu());
    }
    #[inline]
    pub fn insert_to_cpu(&self, key: usize, value: V, cpu_id: usize) {
        if key == 0 {
            panic!();
        }
        self.cache[cpu_id].exclusive_push((key, value));
    }
}
