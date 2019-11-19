// a boring fixed sized vector, for index only

use crate::utils::alloc_mem;
use core::alloc::Layout;
use core::mem;
use std::alloc::{Alloc, Global, GlobalAlloc};
use std::marker::PhantomData;
use std::ops::{Index, IndexMut};

pub struct FixedVec<T, A: Alloc + Default = Global> {
    ptr: *mut T,
    shadow: PhantomData<A>,
}

impl<T, A: Alloc + Default> FixedVec<T, A> {
    pub fn new(cap: usize) -> Self {
        let obj_size = mem::size_of::<T>();
        let total_size = obj_size * cap;
        Self {
            ptr: unsafe { alloc_mem::<T, A>(total_size) } as *mut T,
            shadow: PhantomData,
        }
    }
}

impl<T, A: Alloc + Default> Index<usize> for FixedVec<T, A> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        let obj_ptr = self.ptr as usize + index * mem::size_of::<T>();
        return unsafe { &*(obj_ptr as *mut T) };
    }
}

impl<T, A: Alloc + Default> IndexMut<usize> for FixedVec<T, A> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        let obj_ptr = self.ptr as usize + index * mem::size_of::<T>();
        return unsafe { &mut *(obj_ptr as *mut T) };
    }
}

unsafe impl<T> Send for FixedVec<T> {}
unsafe impl<T> Sync for FixedVec<T> {}
