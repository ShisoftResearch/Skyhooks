// A simple bump heap allocator for internal use
// Each allocation and free may produce a system call
// Used virtual address will not be reclaimed
// If the virtual address space is full and an allocation cannot been done on current address space,
// new address space will be allocated from the system

use crate::collections::lflist;
use crate::generic_heap::{size_class_index_from_size, NUM_SIZE_CLASS, bookmark_size, make_bookmark, size_of_bookmark_word, BOOKMARK_TYPE_FLAG_MASK, object_bookmark};
use crate::mmap::{dealloc_regional, mmap_without_fd, munmap_memory};
use crate::mmap_heap::*;
use crate::utils::*;
use crate::{Ptr, Size, NULL_PTR};
use core::alloc::{Alloc, AllocErr, GlobalAlloc, Layout};
use core::sync::atomic::Ordering::Relaxed;
use core::sync::atomic::{AtomicUsize, Ordering};
use core::{mem, ptr};
use crossbeam::utils::Backoff;
use lfmap::Map;
use libc::*;
use std::mem::MaybeUninit;
use std::sync::atomic::fence;
use std::sync::atomic::Ordering::{SeqCst, Acquire, Release};

const BUMP_SIZE_CLASS: usize = NUM_SIZE_CLASS << 1;

type SizeClasses<A: Alloc + Default> = [SizeClass<A>; BUMP_SIZE_CLASS];
type Bookmark = (usize, usize);

lazy_static! {
    static ref ALLOC_INNER: AllocatorInstance<MmapAllocator> = AllocatorInstance::new();
    static ref MAXIMUM_FREE_LIST_COVERED_SIZE: usize = maximum_free_list_covered_size();
}

pub struct AllocatorInstance<A: Alloc + Default> {
    tail: AtomicUsize,
    base: AtomicUsize,
    address_map: lfmap::WordMap<A, AddressHasher>,
    sizes: SizeClasses<A>,
}

struct SizeClass<A: Alloc + Default> {
    size: usize,
    free_list: lflist::WordList<A>,
}

pub const HEAP_VIRT_SIZE: usize = 128 * 1024 * 1024; // 128MB

fn allocate_address_space() -> Ptr {
    mmap_without_fd(HEAP_VIRT_SIZE)
}

// dealloc address space only been used when CAS base failed
// Even noop will be fine, we still want to return the space the the OS because we can
fn dealloc_address_space(address: Ptr) {
    munmap_memory(address, HEAP_VIRT_SIZE);
}

impl<A: Alloc + Default> AllocatorInstance<A> {
    pub fn new() -> Self {
        let addr = allocate_address_space();
        Self {
            base: AtomicUsize::new(addr as usize),
            tail: AtomicUsize::new(addr as usize),
            address_map: lfmap::WordMap::with_capacity(4096),
            sizes: size_classes(),
        }
    }

    pub fn bump_allocate(&self, size: usize) -> usize {
        let backoff = Backoff::new();
        loop {
            let base = self.base.load(Relaxed);
            let current_tail = self.tail.load(Relaxed);
            let new_tail = current_tail + size;
            let upper_bound = base + HEAP_VIRT_SIZE;
            if current_tail < base || current_tail > upper_bound {
                // current out of range, wrong memory target
            } else if new_tail > upper_bound {
                // may overflow the address space, need to allocate another address space
                // Fetch the old base address for reference in CAS
                self.swap_memory(base);
            // Anyhow, skip follow statements and retry
            } else if self
                .tail
                .compare_and_swap(current_tail, new_tail, Ordering::SeqCst)
                == current_tail
            {
                debug_assert!(current_tail > 0);
                debug_assert!(current_tail >= base);
                debug_assert!(current_tail < base + HEAP_VIRT_SIZE);
                debug_validate(current_tail as Ptr, size);
                return current_tail;
            }
            // CAS tail failed, retry
        }
    }

    fn size_of_object(&self, layout: &Layout) -> (usize, usize) {
        let align = layout.align();
        let size = layout.size();
        let mut actual_size = actual_size_of(align, size);
        let size_class_index = size_class_index_from_size(actual_size);
        if size_class_index < self.sizes.len() {
            let size_class_size = self.sizes[size_class_index].size;
            debug_assert!(size_class_size >= actual_size);
            actual_size = size_class_size;
        } else {
            actual_size = actual_size + align_padding(actual_size, *SYS_PAGE_SIZE);
            debug!("allocate large {}", actual_size);
        }
        (actual_size, size_class_index)
    }

    fn swap_memory(&self, old_base: usize) {
        let new_base = allocate_address_space();
        if self
            .base
            .compare_and_swap(old_base, new_base as usize, Ordering::Relaxed)
            != old_base
        {
            // CAS base address failed, give up and release allocated address space
            // Other thread is also trying to allocate address space and succeeded
            dealloc_address_space(new_base);
        } else {
            // update tail by store. This will fail all ongoing allocation and retry
            self.tail.store(new_base as usize, Ordering::SeqCst);
        }
    }
}

unsafe impl<A: Alloc + Default> GlobalAlloc for AllocatorInstance<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let (actual_size, size_class_index) = self.size_of_object(&layout);
        let align = layout.align();
        let bookmark_size = size_of_bookmark_word::<Bookmark>();
        let origin_addr = self
            .sizes
            .get(size_class_index)
            .and_then(|sc| sc.free_list.pop())
            .unwrap_or_else(|| {
                let size_with_bookmark = actual_size + bookmark_size;
                let addr = self.bump_allocate(size_with_bookmark);
                addr + bookmark_size
            });
        let align_padding = align_padding(origin_addr, align);
        let final_addr = origin_addr + align_padding;
        unsafe {
            debug_assert_eq!(actual_size & BOOKMARK_TYPE_FLAG_MASK, 0);
            let size_ptr = (final_addr - mem::size_of::<usize>()) as *mut usize;
            let origin_ptr = (size_ptr as usize - mem::size_of::<usize>()) as *mut usize;
            ptr::write(size_ptr, actual_size + 1);
            ptr::write(origin_ptr, origin_addr);
        }
        debug_validate(final_addr as Ptr, actual_size);
        fence(Acquire);
        return final_addr as *mut u8;
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        fence(Release);
        let (actual_size, size_class_index) = self.size_of_object(&layout);
        let addr = ptr as usize;
        let size_ptr = (addr - mem::size_of::<usize>()) as *mut usize;
        let origin_ptr = (size_ptr as usize - mem::size_of::<usize>()) as *mut usize;
        let record_size = ptr::read(size_ptr) - 1;
        let origin_addr = ptr::read(origin_ptr);
        if origin_addr < addr
            || origin_addr > addr + maximum_free_list_covered_size()
            || record_size != actual_size
        {
            // fail safe, not sure how this happened though
            warn!("Cannot dealloc object at {:x}", ptr as usize);
            return;
        }
        let size_class_index = size_class_index_from_size(actual_size);
        if size_class_index < self.sizes.len() {
            debug_validate(ptr as Ptr, actual_size);
            self.sizes[size_class_index].free_list.push(origin_addr);
        } else {
            // this may be a problem
            self.address_map.remove(addr);
            let size_of_bookmark = mem::size_of::<Bookmark>();
            dealloc_regional((origin_addr - size_of_bookmark) as Ptr, actual_size + size_of_bookmark);
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

impl<A: Alloc + Default> SizeClass<A> {
    pub fn new(size: usize) -> Self {
        Self {
            size,
            free_list: lflist::WordList::new(),
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
    BumpAllocator.alloc(layout) as Ptr
}
pub unsafe fn free(ptr: Ptr, bookmark: usize) {
    // bookmark is the size here
    let layout = Layout::from_size_align(bookmark, 1).unwrap();
    BumpAllocator.dealloc(ptr as *mut u8, layout);
}

fn size_classes<A: Alloc + Default>() -> SizeClasses<A> {
    let mut data: [MaybeUninit<SizeClass<A>>; BUMP_SIZE_CLASS] =
        unsafe { MaybeUninit::uninit().assume_init() };
    let mut size = 2;
    for elem in &mut data[..] {
        *elem = MaybeUninit::new(SizeClass::new(size));
        size *= 2;
    }
    unsafe { mem::transmute::<_, SizeClasses<A>>(data) }
}

pub unsafe fn realloc(ptr: Ptr, size: Size) -> Ptr {
    if ptr == NULL_PTR {
        return malloc(size);
    }
    let (bookmark, is_bump) = object_bookmark(ptr as usize);
    debug_assert!(is_bump);
    if size == 0 {
        free(ptr, bookmark);
        return NULL_PTR;
    }
    let old_size = size_of(ptr, bookmark);
    if old_size >= size {
        info!("old size is larger than requesting size, untouched");
        return ptr;
    }
    let new_ptr = malloc(size);
    memcpy(new_ptr, ptr, old_size);
    free(ptr, bookmark);
    new_ptr
}

#[inline]
pub fn size_of(ptr: Ptr, bookmark: usize) -> usize {
    bookmark
}

#[inline]
fn maximum_free_list_covered_size() -> usize {
    2 << (BUMP_SIZE_CLASS - 1)
}

#[inline]
fn actual_size_of(align: usize, size: usize) -> usize {
    upper_power_of_2(size + align - 1)
}

#[cfg(test)]
mod test {
    use crate::bump_heap::BumpAllocator;
    use crate::utils::AddressHasher;
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
        let map = lfmap::WordMap::<BumpAllocator, AddressHasher>::with_capacity(1024);
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
