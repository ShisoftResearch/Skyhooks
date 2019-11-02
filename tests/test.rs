extern crate nulloc;

use nulloc::api::NullocAllocator;


#[global_allocator]
static GLOBAL: NullocAllocator = NullocAllocator;

#[test]
pub fn generic() {}
