// A simple bump heap allocator for internal use
// Each allocation and free will produce a system call
// Used virtual address will not be reclaimed
// If the virtual address space is full and an allocation cannot been done on current address space,
// new address space will be allocated from the system

// Because we cannot use heap in this allocator, meta data will not be kept, dealloc will free
// memory space immediately. It is very unsafe.

use crate::mmap::{dealloc_regional, mmap_without_fd, munmap_memory};
use crate::utils::*;
use crate::{Ptr, Size, NULL_PTR};
use crate::mmap_heap::*;
use core::alloc::{GlobalAlloc, Layout, Alloc, AllocErr};
use core::sync::atomic::Ordering::Relaxed;
use core::sync::atomic::{AtomicUsize, Ordering};
use core::{mem, ptr};
use lfmap::Map;
use libc::memcpy;
use crate::collections::lflist;
use crate::generic_heap::{NUM_SIZE_CLASS, size_class_index_from_size};
use std::mem::MaybeUninit;

type SizeClasses = [SizeClass; NUM_SIZE_CLASS];

lazy_static! {
    static ref ALLOC_INNER: AllocatorInner = AllocatorInner::new();
    static ref MALLOC_SIZE: lfmap::WordMap<MmapAllocator> = lfmap::WordMap::<MmapAllocator>::with_capacity(1024);
    static ref MAXIMUM_FREE_LIST_COVERED_SIZE: usize = maximum_free_list_covered_size();
}

pub struct AllocatorInner {
    tail: AtomicUsize,
    addr: AtomicUsize
}

struct SizeClass {
    size: usize,
    free_list: lflist::List<usize, MmapAllocator>
}

pub const HEAP_VIRT_SIZE: usize = 2 * 1024 * 1024 * 1024; // 2GB

fn allocate_address_space() -> Ptr {
    mmap_without_fd(HEAP_VIRT_SIZE)
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
            tail: AtomicUsize::new(addr as usize)
        }
    }
}

unsafe impl GlobalAlloc for AllocatorInner {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let align = layout.align();
        let size = layout.size();
        let actual_size = actual_size(align, size);
        let size_class_index = size_class_index_from_size(actual_size);
        let origin_addr =
        {
            let mut current_tail = 0;
            loop {
                let addr = self.addr.load(Relaxed);
                current_tail = self.tail.load(Relaxed);
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
                    debug_assert!(current_tail > 0);
                    debug_assert!(current_tail >= addr);
                    break
                }
                // CAS tail failed, retry
            }
            debug_assert_ne!(current_tail, 0);
            current_tail
        };
        let align_padding = align_padding(origin_addr, align);
        let final_addr = origin_addr + align_padding;
        debug_assert_ne!(final_addr, 0);
        debug_assert_eq!(final_addr % align, 0);
        debug_assert!(final_addr > 0x50);
        return final_addr as *mut u8;
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        let align = layout.align();
        let size = layout.size();
        let actual_size = actual_size(align, size);
        let size_class_index = size_class_index_from_size(actual_size);

        let addr = ptr as usize;
        let actual_addr = addr;

        // use system call to invalidate underlying physical memory (pages)
        debug!("Dealloc {}", ptr as usize);
        // Will not dealloc objects smaller than page size in physical memory
        if actual_size < *SYS_PAGE_SIZE {
            return;
        }
        dealloc_regional(actual_addr as Ptr, actual_size);
    }

}

unsafe impl Alloc for BumpAllocator {
    unsafe fn alloc(&mut self, layout: Layout) -> Result<ptr::NonNull<u8>, AllocErr> {
        Ok(ptr::NonNull::new(ALLOC_INNER.alloc(layout)).unwrap())
    }

    unsafe fn dealloc(&mut self, ptr: ptr::NonNull<u8>, layout: Layout) {
        ALLOC_INNER.dealloc(ptr.as_ptr(), layout)
    }
}

impl SizeClass {
    pub fn new(size: usize) -> Self {
        Self {
            size,
            free_list: lflist::List::new()
        }
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

impl Default for BumpAllocator {
    fn default() -> Self { Self }
}

pub unsafe fn malloc(size: Size) -> Ptr {
    let layout = Layout::from_size_align(size, 1).unwrap();
    let ptr = BumpAllocator.alloc(layout) as Ptr;
    MALLOC_SIZE.insert(ptr as usize, size as usize);
    ptr
}
pub unsafe fn free(ptr: Ptr) -> bool {
    if let Some(size) = MALLOC_SIZE.remove(ptr as usize) {
        let layout = Layout::from_size_align(size, 1).unwrap();
        BumpAllocator.dealloc(ptr as *mut u8, layout);
        true
    } else {
        false
    }
}

fn size_classes() -> SizeClasses {
    let mut data: [MaybeUninit<SizeClass>; NUM_SIZE_CLASS] =
        unsafe { MaybeUninit::uninit().assume_init() };
    let mut size = 2;
    for elem in &mut data[..] {
        *elem = MaybeUninit::new(SizeClass::new(size));
        size *= 2;
    }
    unsafe { mem::transmute::<_, SizeClasses>(data) }
}

#[inline(always)]
pub fn size_of(ptr: Ptr) -> Option<usize> {
    MALLOC_SIZE.get(ptr as usize)
}

#[inline(always)]
fn maximum_free_list_covered_size() -> usize {
    size_classes()[NUM_SIZE_CLASS - 1].size
}

#[inline(always)]
fn actual_size(align: usize, size: usize) -> usize {
    size + align - 1
}

pub unsafe fn realloc(ptr: Ptr, size: Size) -> Ptr {
    if ptr == NULL_PTR {
        return malloc(size);
    }
    if size == 0 {
        free(ptr);
        return NULL_PTR;
    }
    let old_size = if let Some(size) = MALLOC_SIZE.get(ptr as usize) {
        size
    } else {
        warn!("Cannot determinate old object");
        return NULL_PTR;
    };
    if old_size >= size {
        info!("old size is larger than requesting size, untouched");
        return ptr;
    }
    let new_ptr = malloc(size);
    memcpy(new_ptr, ptr, old_size);
    free(ptr);
    new_ptr
}

#[cfg(test)]
mod test {
    use crate::bump_heap::BumpAllocator;
    use lfmap::Map;

    #[test]
    pub fn general() {
        let map = lfmap::WordMap::<BumpAllocator>::with_capacity(1024);
        for i in 5..10240 {
            map.insert(i, i * 2);
        }
        for i in 5..10240 {
            assert_eq!(map.get(i), Some(i * 2), "index: {}", i);
        }
        for i in 5..10240 {
            assert_eq!(map.remove(i), Some(i * 2), "index: {}", i);
        }
    }
}