// eventual-consistent map based on lfmap and lflist from Shisoft
use lfmap::WordMap;
use parking_lot::Mutex;
use crate::collections::lflist;

pub struct Producer<V> {
    cache: lflist::List<V>
}

pub struct EnvMap<V: Clone> {
    map: lfmap::ObjectMap<V>,
    producers: Mutex<Vec<Producer<V>>>
}