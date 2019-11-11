// usize lock-free, wait free paged linked list stack

use crate::utils::*;
use core::mem;
use core::ptr;
use std::ops::Deref;
use std::sync::atomic::Ordering::{Relaxed};
use std::sync::atomic::{AtomicPtr, AtomicUsize};
use core::alloc::Alloc;
use std::ptr::null_mut;
use std::alloc::Global;

struct BufferMeta<T, A: Alloc + Default> {
    head: AtomicUsize,
    next: AtomicPtr<BufferMeta<T, A>>,
    refs: AtomicUsize,
    upper_bound: usize,
    lower_bound: usize,
}

pub struct List<T, A: Alloc + Default = Global> {
    head: AtomicPtr<BufferMeta<T, A>>,
    count: AtomicUsize,
}

impl<T, A: Alloc + Default> List<T, A> {
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
            // detect obsolete buffer, try again
            // if pos > page.upper_bound { continue; }
            if next_pos > page.upper_bound {
                // buffer overflow, make new and link to last buffer
                let new_head = BufferMeta::new();
                unsafe {
                    (*new_head).next.store(head_ptr, Relaxed);
                }
                if self.head.compare_and_swap(head_ptr, new_head, Relaxed) != head_ptr {
                    BufferMeta::unref(new_head);
                }
                // either case, retry
                continue;
            } else {
                let ptr = pos as *mut T;
                if page.head.compare_and_swap(pos, next_pos, Relaxed) == pos {
                    unsafe {
                        ptr::write(ptr, item);
                    }
                    self.count.fetch_add(1, Relaxed);
                    break;
                }
            }
        }
    }

    pub fn exclusive_push(&self, item: T) {
        // user ensure the push is exclusive, thus no CAS except for header
        let mut pos = 0;
        let mut page;
        loop {
            let head_ptr = self.head.load(Relaxed);
            page = BufferMeta::borrow(head_ptr);
            pos = page.head.load(Relaxed);
            let next_pos = pos + mem::size_of::<T>();
            if pos > page.upper_bound {
                // detect obsolete buffer, try again
                continue;
            }
            if next_pos > page.upper_bound {
                // buffer overflow, make new and link to last buffer
                let new_head = BufferMeta::new();
                unsafe {
                    (*new_head).next.store(head_ptr, Relaxed);
                }
                self.head.store(new_head, Relaxed);
                if self.head.compare_and_swap(head_ptr, new_head, Relaxed) != head_ptr {
                    BufferMeta::unref(new_head);
                }
                // either case, retry
                continue;
            } else {
                let ptr = pos as *mut T;
                page.head.store(next_pos, Relaxed);
                unsafe {
                    ptr::write(ptr, item);
                }
                self.count.fetch_add(1, Relaxed);
                break;
            }
        }
    }

    pub fn pop(&self) -> Option<T> {
        if self.count.load(Relaxed) == 0 { return None; }
        let mut page;
        loop {
            let head_ptr = self.head.load(Relaxed);
            page = BufferMeta::borrow(head_ptr);
            let pos = page.head.load(Relaxed);
            let obj_size = mem::size_of::<T>();
            let new_pos = pos - obj_size;
            // detect obsolete buffer, try again
            // if pos > page.upper_bound << 1 { continue; }
            if pos == page.lower_bound && page.next.load(Relaxed) == null_mut() {
                // empty buffer chain
                return None;
            }
            if pos == page.lower_bound {
                // last item, need to remove this head and swap to the next one
                let next = page.next.load(Relaxed);
                // CAS page head to four times of the upper bound indicates this buffer is obsolete
                if next != null_mut()
                    // && page.head.compare_and_swap(pos, page.upper_bound << 2, Relaxed) == pos
                {
                    if self.head.compare_and_swap(head_ptr, next, Relaxed) == head_ptr {
                        BufferMeta::unref(head_ptr);
                    } else {
                        page.head.store(pos, Relaxed);
                    }
                }
                continue;
            }
            let mut res = None;
            if new_pos >= page.lower_bound{
                res = Some(unsafe { ptr::read(new_pos as *mut T) });
                if page.head.compare_and_swap(pos, new_pos, Relaxed) != pos
                {
                    // cannot swap head
                    mem::forget(res.unwrap()); // won't call drop for this one
                    continue;
                }
            } else {
                continue;
            }
            self.count.fetch_sub(1, Relaxed);
            return res;
        }
    }
    pub fn drop_out_all(&self) -> Vec<T> {
        let mut res = vec![];
        if self.count.load(Relaxed) == 0 { return res; }
        let new_head_buffer = BufferMeta::new();
        let mut buffer_ptr = self.head.swap(new_head_buffer, Relaxed);
        'main: while buffer_ptr != null_mut() {
            let buffer = BufferMeta::borrow(buffer_ptr);
            let next_ptr = buffer.next.load(Relaxed);
            loop {
                //wait until reference counter reach 2 (one for not garbage one for current reference)
                let rc = buffer.refs.load(Relaxed);
                if rc == 2 {
                    break;
                } else if rc <= 1 {
                    // means the buffer have already been mark as garbage, should skip this one
                    buffer_ptr = next_ptr;
                    continue 'main;
                } else {
                    continue
                }
            }
            res.append(&mut BufferMeta::flush_buffer(&*buffer));
            BufferMeta::unref(buffer_ptr);
            buffer_ptr = next_ptr;
        }
        self.count.fetch_sub(res.len(), Relaxed);
        return res;
    }

    pub fn prepend_with(&self, other: &Self) {
        if other.count.load(Relaxed) == 0 { return; }
        let other_head = other.head.swap(BufferMeta::new(), Relaxed);
        let other_count = other.count.swap(0, Relaxed);
        let mut other_tail = BufferMeta::borrow(other_head);
        // probe the last buffer in other link
        loop {
            while other_tail.refs.load(Relaxed) > 2 {}
            let next_ptr = other_tail.next.load(Relaxed);
            if next_ptr == null_mut() {
                break;
            }
            other_tail = BufferMeta::borrow(next_ptr);
        }

        // CAS this head to other head then reset other tail next buffer to this head
        loop {
            let this_head = self.head.load(Relaxed);
            if self.head.compare_and_swap(this_head, other_head, Relaxed) != this_head {
                continue;
            } else {
                other_tail.next.store(this_head, Relaxed);
                break;
            }
        }
        self.count.fetch_add(other_count, Relaxed);
    }

    pub fn count(&self) -> usize {
        self.count.load(Relaxed)
    }
}

impl<T, A: Alloc + Default> Drop for List<T, A> {
    fn drop(&mut self) {
        unsafe {
            let mut node_ptr = self.head.load(Relaxed);
            while node_ptr as usize != 0 {
                let next_ptr = (&*node_ptr).next.load(Relaxed);
                BufferMeta::unref(node_ptr);
                node_ptr = next_ptr;
            }
        }
    }
}

impl<T, A: Alloc + Default> BufferMeta<T, A> {
    pub fn new() -> *mut BufferMeta<T, A> {
        let page_size = *SYS_PAGE_SIZE;
        let head_page = alloc_mem::<T, A>(page_size) as *mut Self;
        let head_page_address = head_page as usize;
        let start = head_page_address + mem::size_of::<Self>();
        *(unsafe { &mut *head_page }) = Self {
            head: AtomicUsize::new(start),
            next: AtomicPtr::new(null_mut()),
            refs: AtomicUsize::new(1),
            upper_bound: head_page_address + page_size,
            lower_bound: start,
        };
        head_page
    }

    pub fn unref(buffer: *mut Self) {
        let rc = {
            let buffer = unsafe { &*buffer };
            buffer.refs.fetch_sub(1, Relaxed)
        };
        if rc == 1 {
            Self::gc(buffer);
        }
    }

    fn gc(buffer: *mut Self) {
        for obj in Self::flush_buffer(unsafe { &*buffer }) {
            drop(obj)
        }
        dealloc_mem::<T, A>(buffer as usize, *SYS_PAGE_SIZE)
    }

    fn flush_buffer(buffer: &Self) -> Vec<T> {
        let size_of_obj = mem::size_of::<T>();
        let mut addr = buffer.lower_bound;
        let data_bound = buffer.head.load(Relaxed);
        let mut res = vec![];
        if data_bound <= buffer.upper_bound{
            // this buffer is not empty
            while addr < data_bound {
                let ptr = addr as *mut T;
                let obj = unsafe { ptr::read(ptr) };
                res.push(obj);
                addr += size_of_obj;
            }
        }
        buffer.head.store(buffer.lower_bound, Relaxed);
        return res;
    }

    fn borrow(buffer: *mut Self) -> BufferRef<T, A> {
        {
            let buffer = unsafe { &*buffer };
            buffer.refs.fetch_add(1, Relaxed);
        }
        BufferRef { ptr: buffer }
    }
}

struct BufferRef<T, A: Alloc + Default> {
    ptr: *mut BufferMeta<T, A>,
}

impl<T, A: Alloc + Default> Drop for BufferRef<T, A> {
    fn drop(&mut self) {
        BufferMeta::unref(self.ptr);
    }
}

impl<T, A: Alloc + Default> Deref for BufferRef<T, A> {
    type Target = BufferMeta<T, A>;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.ptr }
    }
}

#[cfg(test)]
mod test {
    use crate::collections::lflist::List;
    use crate::utils::SYS_PAGE_SIZE;
    use std::sync::Arc;
    use std::thread;
    use std::alloc::Global;

    #[test]
    pub fn general() {
        let list = List::<_, Global>::new();
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
        let list = Arc::new(List::<_, Global>::new());
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
        let recev_list = Arc::new(List::<_, Global>::new());
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
