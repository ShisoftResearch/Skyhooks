// usize lock-free, wait free paged linked list stack

use crate::bump_heap::BumpAllocator;
use crate::utils::*;
use core::ptr;
use core::{intrinsics, mem};
use std::ops::Deref;
use std::ptr::null;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicPtr, AtomicUsize};

struct BufferMeta<T> {
    head: AtomicUsize,
    next: AtomicPtr<BufferMeta<T>>,
    refs: AtomicUsize,
    upper_bound: usize,
    lower_bound: usize,
}

pub struct List<T> {
    head: AtomicPtr<BufferMeta<T>>,
    count: AtomicUsize,
}

impl<T> List<T> {
    pub fn new() -> Self {
        let first_buffer = BufferMeta::new();
        Self {
            head: AtomicPtr::new(first_buffer),
            count: AtomicUsize::new(0),
        }
    }

    pub fn push(&self, item: T) {
        let mut pos = 0;
        let mut page;
        loop {
            let head_ptr = self.head.load(Relaxed);
            page = BufferMeta::borrow(head_ptr);
            pos = page.head.load(Relaxed);
            let next_pos = pos + mem::size_of::<T>();
            if next_pos > page.upper_bound {
                // buffer overflow, make new and link to last buffer
                let new_head = BufferMeta::new();
                unsafe {
                    (*new_head).next.store(head_ptr, Relaxed);
                }
                if self.head.compare_and_swap(head_ptr, new_head, Relaxed) != head_ptr {
                    BufferMeta::mark_garbage(new_head);
                }
                // either case, retry
                continue;
            } else {
                if page.head.compare_and_swap(pos, next_pos, Relaxed) == pos {
                    break;
                }
            }
        }
        let ptr = pos as *mut T;
        unsafe {
            ptr::write(ptr, item);
        }
        self.count.fetch_add(1, Relaxed);
    }

    pub fn pop(&self) -> Option<T> {
        let mut page;
        loop {
            let head_ptr = self.head.load(Relaxed);
            page = BufferMeta::borrow(head_ptr);
            let pos = page.head.load(Relaxed);
            let new_pos = pos - mem::size_of::<T>();
            if pos == page.lower_bound && page.next.load(Relaxed) == null_buffer() {
                // empty buffer chain
                return None;
            }
            if pos == page.lower_bound {
                // last item, need to remove this head and swap to the next one
                let next = page.next.load(Relaxed);
                if next != null_buffer() {
                    if self.head.compare_and_swap(head_ptr, next, Relaxed) == head_ptr {
                        BufferMeta::mark_garbage(head_ptr);
                    }
                }
                continue;
            }
            if new_pos >= page.lower_bound
                && page.head.compare_and_swap(pos, new_pos, Relaxed) != pos
            {
                // cannot swap head
                continue;
            }
            self.count.fetch_sub(1, Relaxed);
            return Some(unsafe { ptr::read(new_pos as *mut T) });
        }
    }
    pub fn drop_out_all(&self) -> Vec<T> {
        let new_head_buffer = BufferMeta::new();
        let mut buffer_ptr = self.head.swap(new_head_buffer, Relaxed);
        let mut res = vec![];
        'main: while buffer_ptr != null_buffer() {
            let buffer = BufferMeta::borrow(buffer_ptr);
            let next_ptr = buffer.next.load(Relaxed);
            loop {
                //wait until reference counter reach 2 (one for not garbage one for current reference)
                let rc = buffer.refs.load(Relaxed);
                if rc == 2 {
                    break;
                } else if rc == 1 {
                    // means the buffer have already been mark as garbage, should skip this one
                    buffer_ptr = next_ptr;
                    continue 'main;
                }
            }
            res.append(&mut BufferMeta::flush_buffer(&*buffer));
            BufferMeta::mark_garbage(buffer_ptr);
            buffer_ptr = next_ptr;
        }
        self.count.fetch_sub(res.len(), Relaxed);
        return res;
    }

    pub fn prepend_with(&self, other: &Self) {
        let other_head = other.head.load(Relaxed);
        loop {
            let mut other_tail = BufferMeta::borrow(other_head);
            // probe the last buffer in other link
            loop {
                let next_ptr = other_tail.next.load(Relaxed);
                if next_ptr == null_buffer() {
                    break;
                }
                other_tail = BufferMeta::borrow(next_ptr);
            }
            let this_head = self.head.load(Relaxed);
            if other_tail
                .next
                .compare_and_swap(null_buffer(), this_head, Relaxed)
                != null_buffer()
            {
                continue;
            }
            if self.head.compare_and_swap(this_head, other_head, Relaxed) != this_head {
                continue;
            }
            break;
        }
        if other
            .head
            .compare_and_swap(other_head, BufferMeta::new(), Relaxed)
            != other_head
        {
            panic!() // TODO: deal with this one, should not happened in current use case
        }
        self.count.fetch_add(other.count.load(Relaxed), Relaxed);
    }

    pub fn count(&self) -> usize {
        self.count.load(Relaxed)
    }
}

impl<T> Drop for List<T> {
    fn drop(&mut self) {
        unsafe {
            let mut node_ptr = self.head.load(Relaxed);
            while node_ptr as usize != 0 {
                let next_ptr = (&*node_ptr).next.load(Relaxed);
                BufferMeta::mark_garbage(node_ptr);
                node_ptr = next_ptr;
            }
        }
    }
}

impl<T> BufferMeta<T> {
    pub fn new() -> *mut BufferMeta<T> {
        let page_size = *SYS_PAGE_SIZE;
        let head_page = alloc_mem::<usize>(page_size) as *mut BufferMeta<T>;
        let head_page_address = head_page as usize;
        let start = head_page_address + mem::size_of::<BufferMeta<T>>();
        *(unsafe { &mut *head_page }) = Self {
            head: AtomicUsize::new(start),
            next: AtomicPtr::new(null_buffer()),
            refs: AtomicUsize::new(1),
            upper_bound: head_page_address + page_size,
            lower_bound: start,
        };
        head_page
    }

    pub fn mark_garbage(buffer: *mut BufferMeta<T>) {
        {
            let buffer = unsafe { &*buffer };
            buffer.refs.fetch_sub(1, Relaxed);
        }
        Self::check_gc(buffer);
    }

    fn check_gc(buffer: *mut BufferMeta<T>) {
        {
            let buffer = unsafe { &*buffer };
            if buffer.refs.compare_and_swap(0, std::usize::MAX, Relaxed) != 0 {
                return;
            }
            for obj in Self::flush_buffer(buffer) {
                drop(obj)
            }
        }
        dealloc_mem::<usize>(buffer as usize, *SYS_PAGE_SIZE)
    }

    fn flush_buffer(buffer: &BufferMeta<T>) -> Vec<T> {
        let size_of_obj = mem::size_of::<T>();
        let mut addr = buffer.lower_bound;
        let data_bound = buffer.head.load(Relaxed);
        let mut res = vec![];
        while addr < data_bound {
            let ptr = addr as *mut T;
            let obj = unsafe { ptr::read(ptr) };
            res.push(obj);
            addr += size_of_obj;
        }
        buffer.head.store(buffer.lower_bound, Relaxed);
        return res;
    }

    fn borrow(buffer: *mut BufferMeta<T>) -> BufferRef<T> {
        {
            let buffer = unsafe { &*buffer };
            buffer.refs.fetch_add(1, Relaxed);
        }
        BufferRef { ptr: buffer }
    }
}

struct BufferRef<T> {
    ptr: *mut BufferMeta<T>,
}

impl<T> Drop for BufferRef<T> {
    fn drop(&mut self) {
        {
            let buffer = unsafe { &*self.ptr };
            buffer.refs.fetch_sub(1, Relaxed);
        }
        BufferMeta::check_gc(self.ptr);
    }
}

impl<T> Deref for BufferRef<T> {
    type Target = BufferMeta<T>;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.ptr }
    }
}

#[inline(always)]
fn null_buffer<T>() -> *mut BufferMeta<T> {
    0 as *mut BufferMeta<T>
}

#[cfg(test)]
mod test {
    use crate::collections::lflist::List;
    use crate::utils::SYS_PAGE_SIZE;
    use std::sync::Arc;
    use std::thread;

    #[test]
    pub fn general() {
        let list = List::new();
        let page_size = *SYS_PAGE_SIZE;
        for i in 2..page_size {
            list.push(i);
        }
        for i in (2..page_size).rev() {
            assert_eq!(list.pop(), Some(i));
        }
        for i in 2..page_size {
            assert_eq!(list.pop(), None);
        }
        list.push(32);
        list.push(25);
        assert_eq!(list.count(), 2);
        assert_eq!(list.drop_out_all(), vec![32, 25]);
        assert_eq!(list.count(), 0);
    }

    #[test]
    pub fn parallel() {
        let list = Arc::new(List::new());
        let page_size = *SYS_PAGE_SIZE;
        let mut threads = (1..page_size)
            .map(|i| {
                let list = list.clone();
                thread::spawn(move || {
                    list.push(i);
                })
            })
            .collect::<Vec<_>>();
        for t in threads {
            t.join();
        }

        let mut counter = 0;
        while list.pop().is_some() {
            counter += 1;
        }
        assert_eq!(counter, page_size - 1);

        for i in 1..page_size {
            list.push(i);
        }
        let recev_list = Arc::new(List::new());
        threads = (page_size..(page_size * 2))
            .map(|i| {
                let list = list.clone();
                let recev_list = recev_list.clone();
                thread::spawn(move || {
                    if i % 2 == 0 {
                        list.push(i);
                    } else {
                        let pop_val = list.pop().unwrap();
                        recev_list.push(pop_val);
                    }
                })
            })
            .collect::<Vec<_>>();
        for t in threads {
            t.join();
        }

        let mut agg = vec![];
        while let Some(v) = list.pop() {
            agg.push(v);
        }
        while let Some(v) = recev_list.pop() {
            agg.push(v);
        }
        agg.sort();
        agg.dedup_by_key(|k| *k);
        let total_insertion = page_size + page_size / 2 - 1;
        assert_eq!(agg.len(), total_insertion);
    }
}
