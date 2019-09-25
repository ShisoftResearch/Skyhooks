use libc::{_SC_PAGESIZE, sysconf};

lazy_static!{
    pub static ref SYS_PAGE_SIZE: usize = unsafe { sysconf(_SC_PAGESIZE) as usize };
}

pub fn align_padding(len: usize, align: usize) -> usize {
    let len_rounded_up = len.wrapping_add(align).wrapping_sub(1)
        & !align.wrapping_sub(1);
    len_rounded_up.wrapping_sub(len)
}