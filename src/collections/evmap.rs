// eventual-consistent map based on lfmap and lflist from Shisoft
use crate::collections::lflist;
use lfmap::{Map, ObjectMap};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use crate::utils::{current_cpu, NUM_CPU};
use std::marker::PhantomData;

type EvBins<V> = Arc<Vec<lflist::List<(usize, V)>>>;

#[derive(Clone)]
pub struct Producer<V> {
    cache: EvBins<V>,
    shadow: PhantomData<V>
}

pub struct EvMap<V: Clone> {
    map: lfmap::ObjectMap<V>,
    source: EvBins<V>,
}

impl<V: Clone> EvMap<V> {
    pub fn new() -> Self {
        let mut source = Vec::with_capacity(*NUM_CPU);
        for _ in 0..*NUM_CPU {
            source.push(lflist::List::new());
        }
        Self {
            map: ObjectMap::with_capacity(4096),
            source: Arc::new(source)
        }
    }

    pub fn new_producer(&self) -> Producer<V> {
        Producer {
            cache: self.source.clone(),
            shadow: PhantomData
        }
    }

    pub fn refresh(&self) {
        // get all items from producers and insert into the local map
        let items = {
            self.source
                .iter()
                .filter_map(|p| p.drop_out_all())
                .flatten()
                .collect::<Vec<_>>()
        };
        for (k, v) in items {
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

impl<V> Producer<V> {
    #[inline]
    pub fn insert(&self, key: usize, value: V) {
        // current_cpu is cheap in Linux: 16 ns/iter (+/- 1)
        self.cache[current_cpu()].exclusive_push((key, value));
    }
}
