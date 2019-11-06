use core::alloc::{Alloc, Layout, AllocErr};
use core::ptr;
use crate::mmap::{mmap_without_fd, munmap_memory};
use crate::Ptr;

// Heap allocator calls mmap and unmap and does NOT respect layout
pub struct MmapAllocator;

unsafe impl Alloc for MmapAllocator {
    unsafe fn alloc(&mut self, layout: Layout) -> Result<ptr::NonNull<u8>, AllocErr> {
        let addr = mmap_without_fd(layout.size());
        debug_assert_ne!(addr as usize, 0);
        Ok(ptr::NonNull::new(addr as *mut u8).unwrap())
    }

    unsafe fn dealloc(&mut self, ptr: ptr::NonNull<u8>, layout: Layout) {
        munmap_memory(ptr.as_ptr() as Ptr, layout.size())
    }
}

impl Default for MmapAllocator {
    fn default() -> Self { Self }
}