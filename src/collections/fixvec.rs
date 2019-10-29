// a boring fixed sized vector, for index only

use crate::bump_heap::BumpAllocator;
use core::alloc::Layout;
use core::mem;
use std::alloc::GlobalAlloc;
use std::ops::{Index, IndexMut};

pub struct FixedVec<T> {
    ptr: *mut T,
    size: usize,
}

impl<T> FixedVec<T> {
    pub fn new(cap: usize) -> Self {
        let obj_size = mem::size_of::<T>();
        let align = mem::align_of::<T>();
        let total_size = obj_size * cap;
        let layout = Layout::from_size_align(total_size, align).unwrap();
        Self {
            size: total_size,
            ptr: unsafe { BumpAllocator.alloc(layout) } as *mut T,
        }
    }
}

impl<T> Index<usize> for FixedVec<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        let obj_ptr = self.ptr as usize + index * mem::size_of::<T>();
        return unsafe { &*(obj_ptr as *mut T) };
    }
}

impl<T> IndexMut<usize> for FixedVec<T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        let obj_ptr = self.ptr as usize + index * mem::size_of::<T>();
        return unsafe { &mut *(obj_ptr as *mut T) };
    }
}

unsafe impl<T> Send for FixedVec<T> {}
unsafe impl<T> Sync for FixedVec<T> {}
