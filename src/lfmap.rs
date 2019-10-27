// usize to usize lock-free, wait free table
use core::alloc::{Layout, GlobalAlloc};
use core::{mem, ptr, intrinsics};
use core::sync::atomic::{AtomicUsize, AtomicPtr, fence, AtomicBool};
use core::sync::atomic::Ordering::{Relaxed, Acquire, Release, SeqCst};
use core::iter::Copied;
use core::cmp::Ordering;
use core::ptr::NonNull;
use ModOp::Empty;
use alloc::string::String;
use core::ops::Deref;
use core::marker::PhantomData;
use crate::bump_heap::BumpAllocator;

pub type EntryTemplate = (usize, usize);

const EMPTY_KEY: usize = 0;
const EMPTY_VALUE: usize = 0;
const SENTINEL_VALUE: usize = 1;

struct Value {
    raw: usize,
    parsed: ParsedValue
}

enum ParsedValue {
    Val(usize),
    Prime(usize),
    Sentinel,
    Empty
}

#[derive(Debug)]
enum ModResult {
    Replaced(usize),
    Fail(usize),
    Sentinel,
    NotFound,
    Done(usize), // address of placement
    TableFull
}

struct ModOutput  {
    result: ModResult,
    index: usize
}

#[derive(Debug)]
enum ModOp<T> {
    Insert(usize, T),
    AttemptInsert(usize, T),
    Sentinel,
    Empty
}

pub struct Chunk<V, A: Attachment<V>> {
    capacity: usize,
    base: usize,
    // floating-point multiplication is slow, cache this value and recompute every time when resize
    occu_limit: usize,
    occupation: AtomicUsize,
    refs: AtomicUsize,
    attachment: A,
    shadow: PhantomData<V>
}

pub struct ChunkRef<V, A: Attachment<V>> {
    chunk: *mut Chunk<V, A>
}

pub struct Table<V, A: Attachment<V>> {
    old_chunk: AtomicPtr<Chunk<V, A>>,
    new_chunk: AtomicPtr<Chunk<V, A>>,
    val_bit_mask: usize, // 0111111..
    inv_bit_mask: usize  // 1000000..
}

impl <V: Copy, A: Attachment<V>> Table <V, A> {
    pub fn with_capacity(cap: usize) -> Self {
        if !is_power_of_2(cap) {
            panic!("capacity is not power of 2");
        }
        // Each entry key value pair is 2 words
        // steal 1 bit in the MSB of value indicate Prime(1)
        let val_bit_mask = !0 << 1 >> 1;
        let chunk = Chunk::alloc_chunk(cap);
        Self {
            old_chunk: AtomicPtr::new(chunk),
            new_chunk: AtomicPtr::new(chunk),
            val_bit_mask,
            inv_bit_mask: !val_bit_mask
        }
    }

    pub fn new() -> Self {
        Self::with_capacity(64)
    }

    pub fn get(&self, key: usize) -> Option<(usize, V)> {
        let mut chunk = unsafe { Chunk::borrow(self.old_chunk.load(SeqCst)) };
        loop {
            let (val, idx) = self.get_from_chunk(&*chunk, key);
            match val.parsed {
                ParsedValue::Prime(val) | ParsedValue::Val(val) => {
                    return Some((val, chunk.attachment.get(idx, key)))
                },
                ParsedValue::Sentinel => {
                    let old_chunk_base = chunk.base;
                    chunk = unsafe { Chunk::borrow(self.new_chunk.load(SeqCst)) };
                    debug_assert_ne!(old_chunk_base, chunk.base);
                }
                ParsedValue::Empty => return None
            }
        }
    }

    pub fn insert(&self, key: usize, value: usize, attached_val: V) -> Option<(usize)> {
        debug!("Inserting key: {}, value: {}", key, value);
        let result = self.ensure_write_new(|new_chunk_ptr| {
            let old_chunk_ptr = self.old_chunk.load(Relaxed);
            let copying = new_chunk_ptr != old_chunk_ptr;
            if !copying && self.check_resize(old_chunk_ptr) {
                debug!("Resized, retry insertion key: {}, value {}", key, value);
                return Err(self.insert(key, value, attached_val));
            }
            let new_chunk = unsafe { Chunk::borrow(new_chunk_ptr) };
            let old_chunk = unsafe { Chunk::borrow_if_cond(old_chunk_ptr, copying) };
            let value_insertion = self.modify_entry(&*new_chunk, key, ModOp::Insert(value, attached_val));
            let insertion_index = value_insertion.index;
            let mut result = None;
            match value_insertion.result {
                ModResult::Done(_) => {},
                ModResult::Replaced(v) | ModResult::Fail(v) => {
                    result = Some(v)
                }
                ModResult::TableFull => {
                    panic!("Insertion is too fast");
                }
                ModResult::Sentinel => {
                    debug!("Insert new and see sentinel, abort");
                    return Ok(None);
                }
                _ => unreachable!("{:?}, copying: {}", value_insertion.result, copying)
            }
            if copying {
                debug_assert_ne!(new_chunk_ptr, old_chunk_ptr);
                fence(Acquire);
                self.modify_entry(&*old_chunk, key, ModOp::Sentinel);
                fence(Release);
            }
            new_chunk.occupation.fetch_add(1, Relaxed);
            Ok(result)
        });
        result
    }

    pub fn remove(&self, key: usize) -> Option<(usize, V)> {
        self.ensure_write_new(|new_chunk_ptr| {
            let old_chunk_ptr = self.old_chunk.load(Relaxed);
            let copying = new_chunk_ptr != old_chunk_ptr;
            let new_chunk = unsafe { Chunk::borrow(new_chunk_ptr) };
            let old_chunk = unsafe { Chunk::borrow_if_cond(old_chunk_ptr, copying) };
            let mut res = self.modify_entry(&*new_chunk, key, ModOp::Empty);
            let mut retr = None;
            match res.result {
                ModResult::Done(v) | ModResult::Replaced(v) => if copying {
                    retr = Some((v, new_chunk.attachment.get(res.index, key)));
                    debug_assert_ne!(new_chunk_ptr, old_chunk_ptr);
                    fence(Acquire);
                    self.modify_entry(&*old_chunk, key, ModOp::Sentinel);
                    fence(Release);
                    new_chunk.attachment.erase(res.index, key);
                }
                ModResult::NotFound => {
                    let remove_from_old = self.modify_entry(&*old_chunk, key, ModOp::Empty);
                    match remove_from_old.result {
                        ModResult::Done(v) | ModResult::Replaced(v) => {
                            retr = Some((v, new_chunk.attachment.get(res.index, key)));
                            old_chunk.attachment.erase(res.index, key);
                        },
                        _ => {}
                    }
                    res = remove_from_old;
                }
                ModResult::TableFull => panic!("need to handle TableFull by remove"),
                _ => {}
            };
            Ok(retr)
        })
    }

    fn ensure_write_new<R, F>(&self, f: F) -> R where F: Fn(*mut Chunk<V, A>) -> Result<R, R> {
        loop {
            let new_chunk_ptr = self.new_chunk.load(SeqCst);
            let f_res = f(new_chunk_ptr);
            match f_res {
                Ok(r) if self.new_chunk.load(SeqCst) == new_chunk_ptr => return r,
                Err(r) => return r,
                _ => { debug!("Invalid write new, retry"); }
            }
        }
    }

    fn get_from_chunk(&self, chunk: &Chunk<V, A>, key: usize) -> (Value, usize) {
        let mut idx = key;
        let entry_size = mem::size_of::<EntryTemplate>();
        let cap = chunk.capacity;
        let base = chunk.base;
        let mut counter = 0;
        while counter < cap {
            idx &= (cap - 1);
            let addr = base + idx * entry_size;
            let k = self.get_key(addr);
            if k == key {
                let val_res = self.get_value(addr);
                match val_res.parsed {
                    ParsedValue::Empty => {},
                    _ => return (val_res, idx)
                }
            }
            if k == EMPTY_KEY {
                return (Value::new(0, self), 0);
            }
            idx += 1; // reprobe
            counter += 1;
        }

        // not found
        return (Value::new(0, self), 0);
    }

    fn modify_entry(&self, chunk: &Chunk<V, A>, key: usize, op: ModOp<V>) -> ModOutput {
        let cap = chunk.capacity;
        let base = chunk.base;
        let mut idx = key;
        let entry_size = mem::size_of::<EntryTemplate>();
        let mut replaced = None;
        let mut count = 0;
        while count <= cap {
            idx &= (cap - 1);
            let addr = base + idx * entry_size;
            let k = self.get_key(addr);
            if k == key {
                // Probing non-empty entry
                let val = self.get_value(addr);
                match &val.parsed {
                    ParsedValue::Val(v) | ParsedValue::Prime(v) => {
                        match op {
                            ModOp::Sentinel => {
                                self.set_sentinel(addr);
                                return ModOutput::new(ModResult::Done(addr), idx);
                            }
                            ModOp::Empty | ModOp::Insert(_, _) => {
                                if !self.set_tombstone(addr, val.raw) {
                                    // this insertion have conflict with others
                                    // other thread changed the value
                                    // should fail (?)
                                    return ModOutput::new(ModResult::Fail(*v), idx)
                                } else {
                                    // we have put tombstone on the value
                                    replaced = Some(*v);
                                }
                            }
                            ModOp::AttemptInsert(_, _) => {
                                // Attempting insert existed entry, skip
                                return ModOutput::new(ModResult::Fail(*v), idx);
                            }
                        }
                        match op {
                            ModOp::Empty => {
                                return ModOutput::new(ModResult::Replaced(*v), idx)
                            }
                            _ => {}
                        }
                    }
                    ParsedValue::Empty => {
                        // found the key with empty value, shall do nothing and continue probing
                    },
                    ParsedValue::Sentinel => return ModOutput::new(ModResult::Sentinel, idx) // should not reachable for insertion happens on new list
                }

            } else if k == EMPTY_KEY {
                // Probing empty entry
                let put_in_empty = |value, attach_val| {
                    // found empty slot, try to CAS key and value
                    if self.cas_value(addr, 0, value) {
                        // CAS value succeed, shall store key
                        if let Some(attach_val) = attach_val {
                            chunk.attachment.set(idx, k, attach_val);
                        }
                        unsafe { intrinsics::atomic_store_relaxed(addr as *mut usize, key) }
                        match replaced {
                            Some(v) => ModResult::Replaced(v),
                            None => ModResult::Done(addr)
                        }
                    } else {
                        // CAS failed, this entry have been taken, reprobe
                        ModResult::Fail(0)
                    }
                };
                let mod_res = match op {
                    ModOp::Insert(val, attach_val) | ModOp::AttemptInsert(val, attach_val) => {
                        debug!("Inserting entry key: {}, value: {}, raw: {:b}, addr: {}",
                               key, val & self.val_bit_mask, val, addr);
                        put_in_empty(val, Some(attach_val))
                    },
                    ModOp::Sentinel => put_in_empty(SENTINEL_VALUE, None),
                    ModOp::Empty => return ModOutput::new(ModResult::Fail(0), idx),
                    _ => unreachable!()
                };
                match &mod_res {
                    ModResult::Fail(_) => {},
                    _ => return ModOutput::new(mod_res, idx)
                }
            }
            idx += 1; // reprobe
            count += 1;
        }
        match op {
            ModOp::Insert(_, _) | ModOp::AttemptInsert(_, _)  => ModOutput::new(ModResult::TableFull, 0),
            _ => ModOutput::new(ModResult::NotFound, 0)
        }
    }

    #[inline(always)]
    fn get_key(&self, entry_addr: usize) -> usize {
        unsafe { intrinsics::atomic_load_relaxed(entry_addr as *mut usize) }
    }

    #[inline(always)]
    fn get_value(&self, entry_addr: usize) -> Value {
        let addr = entry_addr + mem::size_of::<usize>();
        let val = unsafe { intrinsics::atomic_load_relaxed(addr as *mut usize) };
        Value::new(val, self)
    }

    #[inline(always)]
    fn set_tombstone(&self, entry_addr: usize, original: usize) -> bool {
        self.cas_value(entry_addr, original, 0)
    }
    #[inline(always)]
    fn set_sentinel(&self, entry_addr: usize) {
        let addr = entry_addr + mem::size_of::<usize>();
        unsafe { intrinsics::atomic_store_relaxed(addr as *mut usize, SENTINEL_VALUE) }
    }
    #[inline(always)]
    fn cas_value(&self, entry_addr: usize, original: usize, value: usize) -> bool {
        let addr = entry_addr + mem::size_of::<usize>();
        unsafe { intrinsics::atomic_cxchg_relaxed(addr as *mut usize, original, value).0 == original }
    }

    #[inline(always)]
    fn check_resize(&self, old_chunk_ptr: *mut Chunk<V, A>) -> bool {
        let old_chunk_ins = unsafe { Chunk::borrow(old_chunk_ptr) };
        let occupation = old_chunk_ins.occupation.load(Relaxed);
        let occu_limit = old_chunk_ins.occu_limit;
        if occupation > occu_limit {
            // resize
            debug!("Resizing");
            let old_cap = old_chunk_ins.capacity;
            let mult = if old_cap < 2048 { 4 } else { 1 };
            let new_cap = old_cap << mult;
            let new_chunk_ptr = Chunk::alloc_chunk(new_cap);
            if self.new_chunk.compare_and_swap(old_chunk_ptr, new_chunk_ptr, SeqCst) != old_chunk_ptr {
                // other thread have allocated new chunk and wins the competition, exit
                unsafe { Chunk::mark_garbage(new_chunk_ptr); }
                return true;
            }
            let new_chunk_ins = unsafe { Chunk::borrow(new_chunk_ptr) };
            let new_base = new_chunk_ins.base;
            let mut old_address = old_chunk_ins.base as usize;
            let boundary = old_address + chunk_size_of(old_cap);
            let mut effective_copy = 0;
            let mut idx = 0;
            while old_address < boundary  {
                // iterate the old chunk to extract entries that is NOT empty
                let key = self.get_key(old_address);
                let value = self.get_value(old_address);
                if key != EMPTY_KEY // Empty entry, skip
                {
                    // Reasoning value states
                    match &value.parsed {
                        ParsedValue::Val(v) => {
                            // Insert entry into new chunk, in case of failure, skip this entry
                            // Value should be primed
                            debug!("Moving key: {}, value: {}", key, v);
                            let primed_val = value.raw | self.inv_bit_mask;
                            let attached_val = old_chunk_ins.attachment.get(idx, key);
                            let new_chunk_insertion = self.modify_entry(
                                &*new_chunk_ins,
                                key,
                                ModOp::AttemptInsert(primed_val, attached_val)
                            );
                            let inserted_addr = match new_chunk_insertion.result {
                                ModResult::Done(addr) => Some(addr), // continue procedure
                                ModResult::Fail(v) => None,
                                ModResult::Replaced(_) => {
                                    unreachable!("Attempt insert does not replace anything");
                                }
                                ModResult::Sentinel => {
                                    unreachable!("New chunk should not have sentinel");
                                }
                                ModResult::NotFound => {
                                    unreachable!()
                                }
                                ModResult::TableFull => panic!()
                            };
                            if let Some(entry_addr) = inserted_addr {
                                fence(Acquire);
                                // cas to ensure sentinel into old chunk
                                if self.cas_value(old_address, value.raw, SENTINEL_VALUE) {
                                    // strip prime
                                    let stripped = primed_val & self.val_bit_mask;
                                    debug_assert_ne!(stripped, SENTINEL_VALUE);
                                    if self.cas_value(entry_addr, primed_val, stripped) {
                                        debug!("Effective copy key: {}, value {}, addr: {}",
                                               key, stripped, entry_addr);
                                        effective_copy += 1;
                                    }
                                } else {
                                    fence(Release);
                                    continue; // retry this entry
                                }
                                fence(Release);
                            }
                            old_chunk_ins.attachment.erase(idx, key);
                        }
                        ParsedValue::Prime(v) => {
                            // Should never have prime in old chunk
                            panic!("Prime in old chunk when resizing")
                        }
                        ParsedValue::Sentinel => {
                            // Sentinel, skip
                            // Sentinel in old chunk implies its new value have already in the new chunk
                            debug!("Skip copy sentinel");
                        }
                        ParsedValue::Empty => {
                            // Empty, skip
                            debug!("Skip copy empty, key: {}", key);
                        }
                    }
                }
                old_address += entry_size();
                idx += 1;
            }
            // resize finished, make changes on the numbers
            new_chunk_ins.occupation.fetch_add(effective_copy, Relaxed);
            debug_assert_ne!(old_chunk_ptr as usize, new_base);
            if self.old_chunk.compare_and_swap(old_chunk_ptr, new_chunk_ptr, SeqCst) != old_chunk_ptr {
                panic!();
            }
            unsafe { Chunk::mark_garbage(old_chunk_ptr); }
            debug!("{}", self.dump(new_base, new_cap));
            return true;
        }
        false
    }

    fn dump(&self, base: usize, cap: usize) -> &str {
        for i in 0..cap {
            let addr = base + i * entry_size();
            debug!("{}-{}\t", self.get_key(addr), self.get_value(addr).raw);
            if i % 8 == 0 { debug!("") }
        }
        "DUMPED"
    }


}

impl Value {
    pub fn new<V, A: Attachment<V>> (val: usize, table: &Table<V, A>) -> Self {
        let res = {
            if val == 0 {
                ParsedValue::Empty
            } else {
                let actual_val = val & table.val_bit_mask;
                let flag = val & table.inv_bit_mask;
                if flag == 1 {
                    ParsedValue::Prime(actual_val)
                } else if actual_val == 1 {
                    ParsedValue::Sentinel
                } else {
                    ParsedValue::Val(actual_val)
                }
            }
        };
        Value {
            raw: val,
            parsed: res
        }
    }
}

impl ParsedValue {
    fn unwrap(&self) -> usize {
        match self {
            ParsedValue::Val(v) | ParsedValue::Val(v) => *v,
            _ => panic!()
        }
    }
}

impl <V, A: Attachment<V>> Chunk <V, A> {
    fn alloc_chunk(capacity: usize) -> *mut Self {
        let base = alloc_mem(chunk_size_of(capacity));
        let ptr = alloc_mem(mem::size_of::<Self>()) as *mut Self;
        unsafe { ptr::write(ptr, Self {
            base, capacity,
            occupation: AtomicUsize::new(0),
            occu_limit: occupation_limit(capacity),
            refs: AtomicUsize::new(1),
            attachment: A::new(capacity),
            shadow: PhantomData
        }) };
        ptr
    }
    unsafe fn borrow(ptr: *mut Chunk<V, A>) -> ChunkRef<V, A> {
        let chunk = &*ptr;
        chunk.refs.fetch_add(1, Relaxed);
        ChunkRef {
            chunk: ptr
        }
    }

    unsafe fn borrow_if_cond(ptr: *mut Chunk<V, A>, cond: bool) -> ChunkRef<V, A> {
        if cond { unsafe { Chunk::borrow(ptr) } } else { ChunkRef::null_ref() }
    }

    unsafe fn mark_garbage(ptr: *mut Chunk<V, A>) {
        // Caller promise this chunk will not be reachable from the outside except snapshot in threads
        {
            let chunk = &*ptr;
            chunk.refs.fetch_sub(1, Relaxed);
        }
        Self::check_gc(ptr);
    }
    unsafe fn check_gc(ptr: *mut Chunk<V, A>) {
        let chunk = &*ptr;
        if  chunk.refs.compare_and_swap(0, std::usize::MAX, Relaxed) == 0 {
            chunk.attachment.dealloc();
            dealloc_mem(ptr as usize, mem::size_of::<Self>());
            dealloc_mem(chunk.base, chunk_size_of(chunk.capacity));
        }
    }
}

impl ModOutput {
    pub fn new(res: ModResult, idx: usize) -> Self {
        Self {
            result: res, index: idx
        }
    }
}

impl <V, A: Attachment<V>>  Drop for ChunkRef<V, A> {
    fn drop(&mut self) {
        if self.chunk as usize == 0 { return }
        let chunk = unsafe { &*self.chunk };
        chunk.refs.fetch_sub(1, Relaxed);
        unsafe { Chunk::check_gc(self.chunk) }
    }
}

impl <V, A: Attachment<V>>  Deref for ChunkRef<V, A> {
    type Target = Chunk<V, A>;

    fn deref(&self) -> &Self::Target {
        debug_assert_ne!(self.chunk as usize, 0);
        unsafe { &*self.chunk }
    }
}

impl <V, A: Attachment<V>>  ChunkRef <V, A> {
    fn null_ref() -> Self { Self { chunk: 0 as *mut Chunk<V, A> } }
}

fn is_power_of_2(num: usize) -> bool {
    if num < 1 {return false}
    if num <= 2 {return true}
    if num % 2 == 1 {return false};
    return is_power_of_2(num / 2);
}

#[inline(always)]
fn occupation_limit(cap: usize) -> usize {
    (cap as f64 * 0.7f64) as usize
}

#[inline(always)]
fn entry_size() -> usize {
    mem::size_of::<EntryTemplate>()
}

#[inline(always)]
fn chunk_size_of(cap: usize) -> usize {
    cap * entry_size()
}

#[inline(always)]
fn alloc_mem(size: usize) -> usize {
    let align = mem::align_of::<EntryTemplate>();
    let layout = Layout::from_size_align(size, align).unwrap();
    // must be all zeroed
    unsafe { BumpAllocator.alloc_zeroed(layout) as usize }
}

#[inline(always)]
fn dealloc_mem(ptr: usize, size: usize) {
    let align = mem::align_of::<EntryTemplate>();
    let layout = Layout::from_size_align(size, align).unwrap();
    unsafe { BumpAllocator.dealloc(ptr as *mut u8, layout) }
}

pub trait Attachment<V> {
    fn new(cap: usize) -> Self;
    fn get(&self, index: usize, key: usize) -> V;
    fn set(&self, index: usize, key: usize, att_value: V);
    fn erase(&self, index: usize, key: usize);
    fn dealloc(&self);
}

pub struct WordAttachment;

// this attachment basically do nothing and sized zero
impl Attachment <()> for WordAttachment {
    fn new(cap: usize) -> Self { Self }

    fn get(&self, index: usize, key: usize) -> () {}

    fn set(&self, index: usize, key: usize, att_value: ()) {}

    fn erase(&self, index: usize, key: usize) {}

    fn dealloc(&self) {}
}

pub type WordTable = Table<(), WordAttachment>;

pub struct ObjectAttachment<T> {
    obj_chunk: usize,
    size: usize,
    obj_size: usize,
    shadow: PhantomData<T>
}

impl  <T: Copy> Attachment<T> for ObjectAttachment<T> {
    fn new(cap: usize) -> Self {
        let obj_size = mem::size_of::<T>();
        let obj_chunk_size = cap * obj_size;
        let addr = alloc_mem(obj_chunk_size);
        Self {
            obj_chunk: addr,
            size: obj_chunk_size,
            obj_size,
            shadow: PhantomData
        }
    }

    fn get(&self, index: usize, key: usize) -> T {
        let addr = self.addr_by_index(index);
        unsafe { *(addr as *mut T) }
    }

    fn set(&self, index: usize, key: usize, att_value: T) {
        let addr = self.addr_by_index(index);
        unsafe { ptr::write(addr as *mut T, att_value) }
    }

    fn erase(&self, index: usize, key: usize) {
       // will not erase
    }

    fn dealloc(&self) {
        dealloc_mem(self.obj_chunk, self.size);
    }
}

impl <T> ObjectAttachment <T> {
    fn addr_by_index(&self, index: usize) -> usize {
        self.obj_chunk + index * self.obj_size
    }
}

pub trait Map<K, V> {
    fn with_capacity(cap: usize) -> Self;
    fn get(&self, key: K) -> Option<V>;
    fn insert(&self, key: K, value: V)-> Option<()> ;
    fn remove(&self, key: K)-> Option<V> ;
}

pub struct ObjectMap<V: Copy> {
    table: Table<V, ObjectAttachment<V>>,
}

impl <V: Copy> Map<usize, V> for ObjectMap<V> {
    fn with_capacity(cap: usize) -> Self {
        Self {
            table: Table::with_capacity(cap)
        }
    }

    fn get(&self, key: usize) -> Option<V> {
        self.table.get(key).map(|v| v.1)
    }

    fn insert(&self, key: usize, value: V) -> Option<()> {
        self.table.insert(key, !0, value).map(|_| ())
    }

    fn remove(&self, key: usize) -> Option<V> {
        self.table.remove(key).map(|(_, v)| v)
    }
}

pub struct WordMap{
    table: WordTable
}

impl Map<usize, usize> for WordMap {
    fn with_capacity(cap: usize) -> Self {
        Self {
            table: Table::with_capacity(cap)
        }
    }

    fn get(&self, key: usize) -> Option<usize> {
        self.table.get(key).map(|v| v.0)
    }

    fn insert(&self, key: usize, value: usize) -> Option<()> {
        self.table.insert(key, value, ()).map(|_| ())
    }

    fn remove(&self, key: usize) -> Option<usize> {
        self.table.remove(key,).map(|(v, _)| v)
    }
}

#[cfg(test)]
mod test {
    use crate::lfmap::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn will_not_overflow() {
        env_logger::try_init();
        let table = WordMap::with_capacity(16);
        for i in 50..60 {
            assert_eq!(table.insert(i, i), None);
        }
        for i in 50..60 {
            assert_eq!(table.get(i), Some(i));
        }
    }

    #[test]
    fn resize () {
        env_logger::try_init();
        let map = WordMap::with_capacity(16);
        for i in 5..2048 {
            map.insert(i, i * 2);
        }
        for i in 5..2048 {
            match map.get(i) {
                Some(r) => assert_eq!(r, i * 2),
                None => panic!("{}", i)
            }
        }
    }

    #[test]
    fn parallel_no_resize() {
        env_logger::try_init();
        let map = Arc::new(WordMap::with_capacity(65536));
        let mut threads = vec![];
        for i in 5..99 {
            map.insert(i, i * 10);
        }
        for i in 100..900 {
            let map = map.clone();
            threads.push(
                thread::spawn(move || {
                    for j in 5..60 {
                        map.insert(i * 100 + j, i * j);
                    }
                })
            );
        }
        for i in 5..9 {
            for j in 1..10 {
                map.remove(i * j);
            }
        }
        for thread in threads {
            let _ = thread.join();
        }
        for i in 100..900 {
            for j in 5..60 {
                assert_eq!(map.get(i * 100 + j), Some(i * j))
            }
        }
        for i in 5..9 {
            for j in 1..10 {
                assert!(map.get(i * j).is_none())
            }
        }
    }

    #[test]
    fn parallel_with_resize() {
        let map = Arc::new(WordMap::with_capacity(32));
        let mut threads = vec![];
        for i in 5..24 {
            let map = map.clone();
            threads.push(
                thread::spawn(move || {
                    for j in 5..1000 {
                        map.insert(i + j * 100, i * j);
                    }

                })
            );
        }
        for thread in threads {
            let _ = thread.join();
        }
        for i in 5..24 {
            for j in 5..1000 {
                let k = i + j * 100;
                match map.get(k) {
                    Some(v) => assert_eq!(v, i * j),
                    None => {
                        panic!("Value should not be None for key: {}", k)
                    }
                }
            }
        }
    }

    #[test]
    fn parallel_hybird() {
        let map = Arc::new(WordMap::with_capacity(32));
        for i in 5..128 {
            map.insert(i, i * 10);
        }
        let mut threads = vec![];
        for i in 256..265 {
            let map = map.clone();
            threads.push(
                thread::spawn(move || {
                    for j in 5..60 {
                        map.insert(i * 10 + j , 10);
                    }

                })
            );
        }
        for i in 5..8 {
            let map = map.clone();
            threads.push(
                thread::spawn(move || {
                    for j in 5..8 {
                        map.remove(i * j);
                    }
                })
            );
        }
        for thread in threads {
            let _ = thread.join();
        }
        for i in 256..265 {
            for j in 5..60 {
                assert_eq!(map.get(i * 10 + j), Some(10))
            }
        }
    }


    #[test]
    fn obj_map() {
        #[derive(Copy, Clone)]
        struct Obj {
            a: usize,
            b: usize,
            c: usize,
            d: usize
        }
        impl Obj {
            fn new(num: usize) -> Self {
                Obj {
                    a: num,
                    b: num + 1,
                    c: num + 2,
                    d: num + 3
                }
            }
            fn validate(&self, num: usize) {
                assert_eq!(self.a, num);
                assert_eq!(self.b, num + 1);
                assert_eq!(self.c, num + 2);
                assert_eq!(self.d, num + 3);
            }
        }
        let map = ObjectMap::with_capacity(16);
        for i in 5..2048 {
            map.insert(i, Obj::new(i));
        }
        for i in 5..2048 {
            match map.get(i) {
                Some(r) => {
                    r.validate(i)
                },
                None => panic!("{}", i)
            }
        }
    }
}