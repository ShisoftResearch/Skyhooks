use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{fence, AtomicUsize};

pub struct XorRand {
    x: AtomicUsize
}

impl XorRand {
    pub fn new(seed: usize) -> Self {
        Self {
            x: AtomicUsize::new(seed)
        }
    }

    pub fn rand(&self) -> usize {
        let mut x = self.x.load(Relaxed);
        x = self.x.fetch_xor(x.wrapping_shl(13), Relaxed);
        x = self.x.fetch_xor(x.wrapping_shr(7), Relaxed);
        self.x.fetch_xor(x.wrapping_shl(17), Relaxed);
        self.x.load(Relaxed)
    }

    pub fn rand_range(&self, a: usize, b: usize) -> usize {
        let m = b - a + 1;
        return a + (self.rand() % m);
    }
}