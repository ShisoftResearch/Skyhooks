// A simple bump heap allocator for internal use
// Each allocation and free will produce a system call
// Used virtual address will not be reclaimed
// If the virtual address space is full and an allocation cannot been done on current address space,
// new address space will be allocated from the system

// Because we cannot use heap in this allocator, meta data will not be kept, dealloc will free
// memory space immediately. It is very unsafe.

use crate::collections::lflist;
use crate::generic_heap::{size_class_index_from_size, NUM_SIZE_CLASS};
use crate::mmap::{dealloc_regional, mmap_without_fd, munmap_memory};
use crate::mmap_heap::*;
use crate::utils::*;
use crate::{Ptr, Size, NULL_PTR};
use core::alloc::{Alloc, AllocErr, GlobalAlloc, Layout};
use core::sync::atomic::Ordering::Relaxed;
use core::sync::atomic::{AtomicUsize, Ordering};
use core::{mem, ptr};
use lfmap::Map;
use libc::memcpy;
use std::mem::MaybeUninit;

type SizeClasses = [SizeClass; NUM_SIZE_CLASS];

lazy_static! {
    static ref ALLOC_INNER: AllocatorInner = AllocatorInner::new();
    static ref MALLOC_SIZE: lfmap::WordMap<MmapAllocator> =
        lfmap::WordMap::<MmapAllocator>::with_capacity(1024);
    static ref MAXIMUM_FREE_LIST_COVERED_SIZE: usize = maximum_free_list_covered_size();
}

pub struct AllocatorInner {
    tail: AtomicUsize,
    addr: AtomicUsize,
    address_map: lfmap::ObjectMap<Object, MmapAllocator>,
    sizes: SizeClasses,
}

#[derive(Clone)]
struct Object {
    addr: usize,
    size: usize,
}

struct SizeClass {
    size: usize,
    free_list: lflist::List<usize, MmapAllocator>,
}

pub const HEAP_VIRT_SIZE: usize = 2 * 1024 * 1024 * 1024; // 2GB

fn allocate_address_space() -> Ptr {
    mmap_without_fd(HEAP_VIRT_SIZE + 4096)
}

// dealloc address space only been used when CAS base failed
// Even noop will be fine, we still want to return the space the the OS because we can
fn dealloc_address_space(address: Ptr) {
    munmap_memory(address, HEAP_VIRT_SIZE + 4096);
}

impl AllocatorInner {
    pub fn new() -> Self {
        let addr = allocate_address_space();
        Self {
            addr: AtomicUsize::new(addr as usize),
            tail: AtomicUsize::new(addr as usize),
            address_map: lfmap::ObjectMap::with_capacity(1024),
            sizes: size_classes(),
        }
    }
}

unsafe impl GlobalAlloc for AllocatorInner {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let align = layout.align();
        let size = layout.size();
        let mut actual_size = actual_size(align, size);
        let size_class_index = size_class_index_from_size(actual_size);
        if size_class_index < self.sizes.len() {
            let size_class_size = self.sizes[size_class_index].size;
            debug_assert!(size_class_size >= actual_size);
            actual_size = size_class_size;
        } else {
            debug!("allocate large {}", actual_size);
        }
        let origin_addr = self
            .sizes
            .get(size_class_index)
            .and_then(|sc| sc.free_list.pop())
            .unwrap_or_else(|| {
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
                        break;
                    }
                    // CAS tail failed, retry
                }
                debug_assert_ne!(current_tail, 0);
                current_tail
            });
        let align_padding = align_padding(origin_addr, align);
        let final_addr = origin_addr + align_padding;
        self.address_map.insert(
            final_addr,
            Object {
                addr: origin_addr,
                size: actual_size,
            },
        );
        return final_addr as *mut u8;
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        let addr = ptr as usize;
        if let Some(obj) = self.address_map.remove(addr) {
            let actual_addr = obj.addr;
            let actual_size = obj.size;
            let size_class_index = size_class_index_from_size(actual_size);
            if size_class_index < self.sizes.len() {
                self.sizes[size_class_index].free_list.push(actual_addr);
            }
            if actual_size > *SYS_PAGE_SIZE {
                dealloc_regional(actual_addr as Ptr, actual_size);
            }
        }
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
            free_list: lflist::List::new(),
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
    fn default() -> Self {
        Self
    }
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

#[inline]
pub fn size_of(ptr: Ptr) -> Option<usize> {
    MALLOC_SIZE.get(ptr as usize)
}

#[inline]
fn maximum_free_list_covered_size() -> usize {
    size_classes()[NUM_SIZE_CLASS - 1].size
}

#[inline]
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
    use crate::Ptr;
    use lfmap::Map;
    use std::alloc::{GlobalAlloc, Layout};

    #[test]
    pub fn generic() {
        unsafe {
            let a = BumpAllocator;
            let o1_size = 128;
            let o1_layout = Layout::from_size_align(o1_size, 8).unwrap();
            let addr_1 = a.alloc(o1_layout);
            libc::memset(addr_1 as Ptr, 255, o1_size);
            for add in addr_1 as usize..addr_1 as usize + o1_size {
                assert_eq!(*(add as *const u8), 255);
            }
            let o2_size = 256;
            let o2_layout = Layout::from_size_align(o2_size, 8).unwrap();
            let addr_2 = a.alloc(o2_layout);
            libc::memset(addr_2 as Ptr, 233, o2_size);
            for add in addr_1 as usize..addr_1 as usize + o1_size {
                assert_eq!(*(add as *const u8), 255);
            }
            for add in addr_2 as usize..addr_2 as usize + o2_size {
                assert_eq!(*(add as *const u8), 233);
            }
            a.dealloc(addr_1, o1_layout);
            let addr_3 = a.alloc(o1_layout);
            assert_eq!(addr_3, addr_1);
            for add in addr_3 as usize..addr_3 as usize + o1_size {
                assert_eq!(*(add as *const u8), 255);
            }
            libc::memset(addr_3 as Ptr, 24, o1_size);
            for add in addr_3 as usize..addr_3 as usize + o1_size {
                assert_eq!(*(add as *const u8), 24);
            }
            for add in addr_2 as usize..addr_2 as usize + o2_size {
                assert_eq!(*(add as *const u8), 233);
            }
        }
    }

    #[test]
    pub fn application() {
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
