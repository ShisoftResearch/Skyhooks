// eventual-consistent map based on lfmap and lflist from Shisoft
use crate::collections::lflist;
use lfmap::{Map, ObjectMap};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

pub struct Producer<V> {
    id: usize,
    cache: lflist::List<(usize, V)>,
}

pub struct EvMap<V: Clone> {
    map: lfmap::ObjectMap<V>,
    producers: lfmap::ObjectMap<Arc<Producer<V>>>,
    counter: AtomicUsize,
}

impl<V: Clone> EvMap<V> {
    pub fn new() -> Self {
        Self {
            map: ObjectMap::with_capacity(512),
            producers: ObjectMap::with_capacity(128),
            counter: AtomicUsize::new(0),
        }
    }

    pub fn new_producer(&self) -> Arc<Producer<V>> {
        let id = self.counter.fetch_add(1, Relaxed);
        let producer = Producer {
            id,
            cache: lflist::List::new(),
        };
        let reference = Arc::new(producer);
        self.producers.insert(id, reference.clone());
        return reference;
    }

    pub fn remove_producer(&self, producer: &Arc<Producer<V>>) {
        if let Some(p) = self.producers.remove(producer.id) {
            let items = p.cache.drop_out_all();
            for (k, v) in items {
                self.map.insert(k, v);
            }
        }
    }

    pub fn refresh(&self) {
        // get all items from producers and insert into the local map
        let items = {
            self.producers
                .entries()
                .into_iter()
                .map(|(_, p)| p.cache.drop_out_all())
                .flatten()
                .collect::<Vec<_>>()
        };
        for (k, v) in items {
            self.map.insert(k, v);
        }
    }

    pub fn insert(&self, key: usize, value: V) -> Option<()> {
        self.map.insert(key, value)
    }

    pub fn get(&self, key: usize) -> Option<V> {
        self.map.get(key)
    }

    pub fn remove(&self, key: usize) -> Option<V> {
        self.map.remove(key)
    }

    pub fn contains(&self, key: usize) -> bool { self.map.contains(key) }
}

impl<V> Producer<V> {
    pub fn insert(&self, key: usize, value: V) {
        self.cache.push((key, value));
    }
}
