// A simple bump heap allocator for internal use
// Each allocation and free will produce a system call
// Used virtual address will not be reclaimed
// If the virtual address space is full and an allocation cannot been done on current address space,
// new address space will be allocated from the system

// Because we cannot use heap in this allocator, meta data will not be kept, dealloc will free
// memory space immediately. It is very unsafe.

use crate::mmap::{dealloc_regional, mmap_without_fd, munmap_memory};
use crate::Ptr;
use alloc::vec::Vec;
use core::alloc::{GlobalAlloc, Layout};
use core::sync::atomic::Ordering::Relaxed;
use core::sync::atomic::{AtomicUsize, Ordering};

lazy_static! {
    static ref ALLOC_INNER: AllocatorInner = AllocatorInner::new();
}

pub struct AllocatorInner {
    tail: AtomicUsize,
    addr: AtomicUsize,
}

const HEAP_VIRT_SIZE: usize = 2 * 1024 * 1024 * 1024; // 2GB

fn allocate_address_space() -> Ptr {
    mmap_without_fd(HEAP_VIRT_SIZE)
}

fn actual_size(layout: &Layout) -> usize {
    let size = layout.size();
    let align = layout.align();
    layout.padding_needed_for(align) + size
}

// dealloc address space only been used when CAS base failed
// Even noop will be fine, we still want to return the space the the OS because we can
fn dealloc_address_space(address: Ptr) {
    munmap_memory(address, HEAP_VIRT_SIZE);
}

impl AllocatorInner {
    pub fn new() -> Self {
        let addr = allocate_address_space();
        Self {
            addr: AtomicUsize::new(addr as usize),
            tail: AtomicUsize::new(addr as usize),
        }
    }
}

unsafe impl GlobalAlloc for AllocatorInner {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let actual_size = actual_size(&layout);
        debug!("Allocate {}", actual_size);
        loop {
            let addr = self.addr.load(Relaxed);
            let current_tail = self.tail.load(Relaxed);
            let new_tail = current_tail + actual_size;
            if new_tail > addr + HEAP_VIRT_SIZE {
                // may overflow the address space, need to allocate another address space
                // Fetch the old base address for reference in CAS
                let new_base = allocate_address_space();
                if self
                    .addr
                    .compare_and_swap(addr, new_base as usize, Ordering::Relaxed)
                    != addr
                {
                    // CAS base address failed, give up and release allocated address space
                    // Other thread is also trying to allocate address space and succeeded
                    dealloc_address_space(new_base);
                } else {
                    // update tail by store. This will fail all ongoing allocation and retry
                    self.tail.store(new_base as usize, Ordering::Relaxed);
                }
                // Anyhow, skip follow statements and retry
                continue;
            }
            if self
                .tail
                .compare_and_swap(current_tail, new_tail, Ordering::Relaxed)
                == current_tail
            {
                return current_tail as *mut u8;
            }
            // CAS tail failed, retry
        }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // use system call to invalidate underlying physical memory (pages)
        debug!("Dealloc {}", ptr as usize);
        dealloc_regional(ptr as Ptr, actual_size(&layout));
    }
}

pub struct BumpAllocator;

unsafe impl GlobalAlloc for BumpAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_INNER.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        ALLOC_INNER.dealloc(ptr, layout)
    }
}
