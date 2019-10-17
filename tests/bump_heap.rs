extern crate nulloc;

 use nulloc::bump_heap::BumpAllocator;

 #[global_allocator]
 static INTERNAL_ALLOCATOR: BumpAllocator = BumpAllocator;
