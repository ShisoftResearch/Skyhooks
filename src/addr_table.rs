// usize to usize lock-free, wait free table

use alloc::alloc::Global;
use core::alloc::{Alloc, Layout};
use core::{mem, ptr};
use alloc::raw_vec::RawVec;
use crate::Ptr;
use core::sync::atomic::{AtomicUsize, AtomicPtr};
use core::sync::atomic::Ordering::Relaxed;
use core::iter::Copied;

type EntryTemplate = (usize, usize);

const EMPTY_KEY: usize = 0;
const EMPTY_VALUE: usize = 0;
const SENTINEL_VALUE: usize = 1;

enum Value {
    SENTINEL(usize),
    PRIME(usize),
    VAL(usize)
}

struct Table {
    cap: AtomicUsize,
    chunk: AtomicUsize,
    val_bit_mask: usize, // 0111111..
    inv_bit_mask: usize  // 1000000..
}

impl Table {
    pub fn with_capacity(cap: usize) -> Self {
        if !is_power_of_2(cap) {
            panic!("capacity is not power of 2");
        }
        // Each entry key value pair is 2 words
        let chunk_size = cap * mem::size_of::<EntryTemplate>();
        let align = mem::align_of::<EntryTemplate>();
        let layout = Layout::from_size_align(chunk_size, align).unwrap();
        let chunk = unsafe { Global.alloc(layout) }.unwrap().as_ptr() as usize;
        // steal 1 bit in the MSB of value indicate Prime(1)
        let val_bit_mask = !0 << 1 >> 1;
        Self {
            cap: AtomicUsize::new(cap),
            chunk: AtomicUsize(chunk),
            val_bit_mask,
            inv_bit_mask: !val_bit_mask
        }
    }

    pub fn new() -> Self {
        Self::with_capacity(64)
    }

    pub fn get(&self, key: usize) -> Option<usize> {
        let mut base = self.chunk.load(Relaxed);
        loop {
            let mut val = self.get_from_chunk(base);
            match val {
                Some(Value::SENTINEL(addr)) => {
                    base = addr;
                }
                Some(Value::PRIME(val)) | Some(Value::VAL(val)) => return Some(val),
                None => return None
            }
        }
    }

    pub fn insert(&self, key: usize, value: usize) -> Option<usize> {

    }

    fn get_from_chunk(&self, base: usize) -> Option<Value> {
        let size = self.cap.load(Relaxed);
        let idx = key;
        loop {
            idx &= (size - 1);
            let addr = base + idx * mem::size_of::<EntryTemplate>();
            let k = self.get_key(addr);
            if k == key {
                return self.get_value(addr);
            }
            if k == EMPTY_KEY {
                return None;
            }
            idx += 1; // reprobe
        }
    }

    #[inline(always)]
    fn get_key(&self, entry_addr: usize) -> usize {
        unsafe { ptr::read(addr as *mut usize) }
    }

    #[inline(always)]
    fn get_value(&self, entry_addr: usize) -> Option<Value> {
        addr += mem::size_of::<usize>();
        let val = unsafe { ptr::read(addr as *mut usize) };
        if val != 0 {
            let actual_val = val & *self.val_bit_mask;
            let flag = val & *self.inv_bit_mask;
            let res = if flag == 1 {
                Value::PRIME(actual_val)
            } else if actual_val == 1 {
                Value::SENTINEL(actual_val)
            } else {
                Value::VAL(actual_val)
            };
            Some(res)
        } else {
            None
        }
    }
}

fn is_power_of_2(num: usize) -> bool {
    if num < 1 {return false}
    if num <= 2 {return true}
    if num % 2 == 1 {return false};
    return is_power_of_2(num / 2);
}