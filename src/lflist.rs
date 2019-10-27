// usize lock-free, wait free paged linked list stack

use std::sync::atomic::{AtomicPtr, AtomicUsize};
use crate::bump_heap::BumpAllocator;
use std::alloc::{GlobalAlloc, Layout};
use crate::utils::SYS_PAGE_SIZE;
use core::{mem, intrinsics};
use std::sync::atomic::Ordering::{Relaxed, SeqCst};

const NULL_BUFFER: *mut BufferMeta = 0 as *mut BufferMeta;

struct BufferMeta {
    head: AtomicUsize,
    next: AtomicPtr<BufferMeta>,
    head_address: usize,
    upper_bound: usize
}

pub struct List {
    head: AtomicPtr<BufferMeta>
}

impl List {
    pub fn new() -> Self {
        let first_buffer = BufferMeta::new();
        Self {
            head: AtomicPtr::new(first_buffer)
        }
    }

    pub fn push(&self, item: usize) {
        loop {
            let head_ptr = self.head.load(Relaxed);
            let page = unsafe { &*head_ptr };
            let pos = page.head.load(Relaxed);
            let next_pos = pos + mem::size_of::<usize>();
            if next_pos > page.upper_bound {
                // buffer overflow, make new and link to last buffer
                let new_head = BufferMeta::new();
                unsafe {
                    (*new_head).next.store(head_ptr, Relaxed);
                }
                if self.head.compare_and_swap(head_ptr, new_head, Relaxed) != head_ptr {
                    BufferMeta::dealloc(new_head);
                }
                // either case, retry
                continue;
            } else {
                unsafe {
                    if intrinsics::atomic_cxchg_relaxed(pos as *mut usize, 0, item).0 == 0 {
                        page.head.fetch_add(mem::size_of::<usize>(), Relaxed);
                        return;
                    }
                }
            }
        }
    }

    pub fn pop(&self) -> usize {
        unimplemented!()
    }
}

impl BufferMeta {
    pub fn new() -> *mut BufferMeta {
        let page_size = *SYS_PAGE_SIZE;
        let head_page = alloc_mem(page_size) as *mut BufferMeta;
        let head_page_address = head_page as usize;
        let start = head_page_address + mem::size_of::<BufferMeta>();
        *(unsafe { &mut*head_page }) = Self {
            head: AtomicUsize::new(start),
            next: AtomicPtr::new(NULL_BUFFER),
            head_address: head_page_address,
            upper_bound: head_page_address + page_size
        };
        head_page
    }

    pub fn dealloc(buffer: *mut BufferMeta) {
        dealloc_mem(buffer as usize, *SYS_PAGE_SIZE);
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