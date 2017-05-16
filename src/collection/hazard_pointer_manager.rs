use std::boxed::Box;
use std::mem::transmute;

type Deleter = Box<FnMut(usize)->()>;

pub struct RetiredPointer {
    pub ptr: usize,
    pub deleter: Deleter 
}

impl Drop for RetiredPointer {
    fn drop(&mut self) {
        (self.deleter)(self.ptr)
    }
}

pub trait HazardPointerManager {
    fn acquire(&self, idx: usize, pointer: usize);
    fn release(&self, idx: usize);
    fn retire(&self, pointer: RetiredPointer);

    fn acquire_ptr<T>(&self, idx: usize, pointer: *mut T) {
        self.acquire(idx, pointer as usize);
    }

    fn retire_ptr<T>(&self, pointer: *mut T) {
        let deleter = |ptr| {
            let _: Box<T> = unsafe { Box::from_raw(transmute::<usize, *mut T>(ptr)) };
        };

        self.retire(RetiredPointer {
            ptr: pointer as usize,
            deleter: Box::new(deleter)
        });
    }
}

pub fn is_null(ptr: usize) -> bool {
    unsafe { transmute::<usize, *mut ()>(ptr).is_null() }
}