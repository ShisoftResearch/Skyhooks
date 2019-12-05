// a boring fixed sized vector, for index only

use crate::utils::{alloc_mem, dealloc_mem};
use core::alloc::Layout;
use core::{mem, ptr};
use std::alloc::{Alloc, Global, GlobalAlloc};
use std::marker::PhantomData;
use std::ops::{Index, IndexMut};
use std::ptr::null_mut;

pub struct FixedVec<T, A: Alloc + Default = Global> {
    ptr: *mut T,
    capacity: usize,
    shadow: PhantomData<A>,
}

impl<T, A: Alloc + Default> FixedVec<T, A> {
    pub fn new(cap: usize) -> Self {
        let heap_size = total_size::<T>(cap);
        Self {
            ptr: unsafe { alloc_mem::<A>(heap_size) } as *mut T,
            capacity: cap,
            shadow: PhantomData,
        }
    }
    pub fn capacity(&self) -> usize {
        self.capacity
    }
    fn object_ptr(&self, index: usize) -> usize {
        self.ptr as usize + index * mem::size_of::<T>()
    }
}

impl<T, A: Alloc + Default> Index<usize> for FixedVec<T, A> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        debug_assert!(index < self.capacity);
        let obj_ptr = self.object_ptr(index);
        return unsafe { &*(obj_ptr as *mut T) };
    }
}

impl<T, A: Alloc + Default> IndexMut<usize> for FixedVec<T, A> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        debug_assert!(index < self.capacity);
        let obj_ptr = self.object_ptr(index);
        return unsafe { &mut *(obj_ptr as *mut T) };
    }
}

fn total_size<T>(cap: usize) -> usize {
    let object_size = mem::size_of::<T>();
    object_size * cap
}

unsafe impl<T> Send for FixedVec<T> {}
unsafe impl<T> Sync for FixedVec<T> {}

impl<T, A: Alloc + Default> Drop for FixedVec<T, A> {
    fn drop(&mut self) {
        if self.ptr == null_mut() {
            return;
        }
        debug_assert_ne!(self.capacity, 0);
        let heap_size = total_size::<T>(self.capacity);
        debug_assert_ne!(heap_size, 0);
        if mem::needs_drop::<T>() {
            for i in 0..self.capacity {
                let object_ptr = self.object_ptr(i);
                let object = unsafe { ptr::read(object_ptr as *mut T) };
                drop(object)
            }
        }
        dealloc_mem::<A>(self.ptr as usize, heap_size)
    }
}
