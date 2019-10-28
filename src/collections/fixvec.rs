// a boring fixed sized vector, for index only

pub struct  FixedVec<T> {
    ptr: *mut T,
    size: usize
}


