// usize lock-free, wait free paged linked list stack

use crate::bump_heap::BumpAllocator;
use crate::utils::SYS_PAGE_SIZE;
use core::{intrinsics, mem};
use std::alloc::{GlobalAlloc, Layout};
use std::ops::Deref;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicPtr, AtomicUsize};

const NULL_BUFFER: *mut BufferMeta = 0 as *mut BufferMeta;

struct BufferMeta {
    head: AtomicUsize,
    next: AtomicPtr<BufferMeta>,
    refs: AtomicUsize,
    upper_bound: usize,
    lower_bound: usize,
}

pub struct List {
    head: AtomicPtr<BufferMeta>,
}

impl List {
    pub fn new() -> Self {
        let first_buffer = BufferMeta::new();
        Self {
            head: AtomicPtr::new(first_buffer),
        }
    }

    pub fn push(&self, item: usize) {
        loop {
            let head_ptr = self.head.load(Relaxed);
            let page = BufferMeta::borrow(head_ptr);
            let pos = page.head.load(Relaxed);
            let next_pos = pos + mem::size_of::<usize>();
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
                    let ptr = pos as *mut usize;
                    if unsafe { intrinsics::atomic_xchg_relaxed(ptr, item) } == 0 {
                        return;
                    } else {
                        unreachable!()
                    }
                }
            }
        }
    }

    pub fn pop(&self) -> Option<usize> {
        loop {
            let head_ptr = self.head.load(Relaxed);
            let page = BufferMeta::borrow(head_ptr);
            let pos = page.head.load(Relaxed);
            let new_pos = pos - mem::size_of::<usize>();
            if pos == page.lower_bound && page.next.load(Relaxed) == NULL_BUFFER {
                // empty buffer chain
                return None;
            }
            if pos == page.lower_bound {
                // last item, need to remove this head and swap to the next one
                let next = page.next.load(Relaxed);
                if next != NULL_BUFFER {
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
            let res = unsafe { intrinsics::atomic_xchg_relaxed(new_pos as *mut usize, 0) };
            assert_ne!(res, 0, "return empty");
            return Some(res);
        }
    }
}

impl BufferMeta {
    pub fn new() -> *mut BufferMeta {
        let page_size = *SYS_PAGE_SIZE;
        let head_page = alloc_mem(page_size) as *mut BufferMeta;
        let head_page_address = head_page as usize;
        let start = head_page_address + mem::size_of::<BufferMeta>();
        *(unsafe { &mut *head_page }) = Self {
            head: AtomicUsize::new(start),
            next: AtomicPtr::new(NULL_BUFFER),
            refs: AtomicUsize::new(1),
            upper_bound: head_page_address + page_size,
            lower_bound: start,
        };
        head_page
    }

    pub fn mark_garbage(buffer: *mut BufferMeta) {
        {
            let buffer = unsafe { &*buffer };
            buffer.refs.fetch_sub(1, Relaxed);
        }
        Self::check_gc(buffer);
    }

    fn check_gc(buffer: *mut BufferMeta) {
        {
            let buffer = unsafe { &*buffer };
            if buffer.refs.compare_and_swap(0, std::usize::MAX, Relaxed) != 0 {
                return;
            }
        }
        dealloc_mem(buffer as usize, *SYS_PAGE_SIZE)
    }

    fn borrow(buffer: *mut BufferMeta) -> BufferRef {
        {
            let buffer = unsafe { &*buffer };
            buffer.refs.fetch_add(1, Relaxed);
        }
        BufferRef { ptr: buffer }
    }
}

#[inline(always)]
fn alloc_mem(size: usize) -> usize {
    let align = mem::align_of::<usize>();
    let layout = Layout::from_size_align(size, align).unwrap();
    // must be all zeroed
    unsafe { BumpAllocator.alloc_zeroed(layout) as usize }
}

#[inline(always)]
fn dealloc_mem(ptr: usize, size: usize) {
    let align = mem::align_of::<usize>();
    let layout = Layout::from_size_align(size, align).unwrap();
    unsafe { BumpAllocator.dealloc(ptr as *mut u8, layout) }
}

struct BufferRef {
    ptr: *mut BufferMeta,
}

impl Drop for BufferRef {
    fn drop(&mut self) {
        {
            let buffer = unsafe { &*self.ptr };
            buffer.refs.fetch_sub(1, Relaxed);
        }
        BufferMeta::check_gc(self.ptr);
    }
}

impl Deref for BufferRef {
    type Target = BufferMeta;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.ptr }
    }
}

#[cfg(test)]
mod test {
    use crate::lflist::List;
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
    }

    #[test]
    pub fn parallel() {
        let list = Arc::new(List::new());
        let page_size = *SYS_PAGE_SIZE;
        let mut threads = (1..page_size).map(|i| {
            let list = list.clone();
            thread::spawn(move || {
                list.push(i);
            })
        }).collect::<Vec<_>>();
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
        threads = (page_size..(page_size * 2)).map(|i| {
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
        }).collect::<Vec<_>>();
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
