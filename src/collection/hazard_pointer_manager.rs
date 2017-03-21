pub trait HazardPointerManager<T> {
    fn acquire(&self, idx: usize, pointer: *mut T);
    fn release(&self, idx: usize);
    fn retire(&self, pointer: *mut T);
}