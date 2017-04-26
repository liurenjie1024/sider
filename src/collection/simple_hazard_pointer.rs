use std::vec::Vec;
use std::collections::LinkedList;
use std::collections::HashSet;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering;
use std::ptr;
use std::boxed::Box;
use std::collections::HashMap;
use std::cell::RefCell;
use std::mem::transmute;


use ::thread::thread_context::ThreadContext;
use super::hazard_pointer_manager::HazardPointerManager;



type AcquiredPointers<T> = Box<[AtomicPtr<T>]>;
type RetiredPointers = LinkedList<usize>;
type Id = u32;

thread_local! {
  static RETIRED_POINTERS: RefCell<HashMap<Id, RetiredPointers>> = RefCell::new(HashMap::new()); 
}

#[derive(Debug)]
pub struct Config {
  pub thread_num: usize,
  pub pointer_num: usize,            // Maximum number of pointers acquired per thread.
  pub scan_threshold: usize,         // The threshold to trigger scan.
  pub id: Id                        // Hazard pointer manager id.
}

/// This is just a simple implementation of hazard pointer manager and only for demo usage.
/// Any serious hazard pointer managers should be combined with thread manager.
pub struct SimpleHazardPointerManager<T> {
  //TODO: Replace pointers with another more efficient type to avoid false sharing
  acquired_pointers: Box<[AcquiredPointers<T>]>,
  config: Config,
  deallocator: Box<Fn(*mut T)>
}

impl<T> SimpleHazardPointerManager<T> {
  pub fn new(config: Config, deallocator: Option<Box<Fn(*mut T)>>) -> SimpleHazardPointerManager<T> {
    let acquired_pointers  = (0...config.thread_num).map(|_| {
      (0..config.pointer_num)
        .map(|_| AtomicPtr::default())
        .collect::<Vec<AtomicPtr<T>>>()
        .into_boxed_slice()
    })
    .collect::<Vec<AcquiredPointers<T>>>()
    .into_boxed_slice();

    SimpleHazardPointerManager { 
      acquired_pointers: acquired_pointers,
      config: config,
      deallocator: deallocator.unwrap_or(Box::new(|p| unsafe{Box::from_raw(p);}))
    }
  }

  fn with_retired_pointers<F, V>(&self, f: F) -> V
    where F: Fn(&mut RetiredPointers) -> V {
    RETIRED_POINTERS.with(|cell| {
      let mut map = cell.borrow_mut();
      if !map.contains_key(&self.config.id) {
        map.insert(self.config.id, LinkedList::new());
      }

      f(map.get_mut(&self.config.id).unwrap())
    })
  }

  fn scan(&self, retired_pointer_list: &mut RetiredPointers) {
    let thread_id = ThreadContext::current().thread_id;
    let acquired_pointers = self.acquired_pointers
      .iter()
      .flat_map(|v| {
        v.iter()
          .map(|p| p.load(Ordering::Acquire))
          .filter(|&p| p != ptr::null_mut())
      }).collect::<HashSet<*mut T>>();

    let (reserved_pointers, free_pointers): (LinkedList<*mut T>, LinkedList<*mut T>) = unsafe {
      retired_pointer_list
        .iter()
        .map(|&p| transmute::<usize, *mut T>(p))
        .partition(|p| acquired_pointers.contains(p))
    };
    
    retired_pointer_list.clear();
    retired_pointer_list.extend(reserved_pointers
      .into_iter()
      .map(|p| p as usize));

    for p in free_pointers {
      (self.deallocator)(p);
    }
  }
}

impl<T> HazardPointerManager<T> for SimpleHazardPointerManager<T> {
  fn acquire(&self, idx: usize, pointer: *mut T) {
    let thread_context = ThreadContext::current();
    self.acquired_pointers[thread_context.thread_id][idx].store(pointer, Ordering::Release);
  }

  fn release(&self, idx: usize) {
    let thread_context = ThreadContext::current();
    self.acquired_pointers[thread_context.thread_id][idx].store(ptr::null_mut(), Ordering::Release);
  }

  fn retire(&self, ptr: *mut T) {
    self.with_retired_pointers(|retired_pointers| {
      retired_pointers.push_back(ptr as usize);

      if retired_pointers.len() > self.config.scan_threshold {
        self.scan(retired_pointers);
      }
    })
  }

}

unsafe impl<T> Sync for SimpleHazardPointerManager<T> {}
unsafe impl<T> Send for SimpleHazardPointerManager<T> {}



#[cfg(test)]
mod tests {
  use std::sync::atomic::Ordering;
  use std::ptr;
  use std::sync::Barrier;
  use std::sync::Arc;
  use std::thread;
  use std::mem::transmute;

  use ::collection::hazard_pointer_manager::HazardPointerManager;
  use ::thread::thread_context::ThreadContext;
  use super::SimpleHazardPointerManager;
  use super::Config;

  #[test]
  fn test_init() {
    let manager: SimpleHazardPointerManager<u32> = SimpleHazardPointerManager::new(Config { 
      thread_num: 10, 
      pointer_num: 4,
      scan_threshold: 5,
      id: 1
    }, None);
    assert_eq!(11, manager.acquired_pointers.len());
    assert_eq!(4, manager.acquired_pointers[0].len());
    manager.with_retired_pointers(|retired_pointers| {
      assert_eq!(0, retired_pointers.len());
    });
  }

  #[test]
  fn test_acquire_release() {
    let manager: SimpleHazardPointerManager<u32> = SimpleHazardPointerManager::new(Config { 
      thread_num: 10, 
      pointer_num: 4,
      scan_threshold: 5,
      id: 1
    }, None);

    let thread_id = 3;
    ThreadContext::set_current(ThreadContext::new(thread_id));

    let ptr = &mut 12u32 as *mut u32;
    manager.acquire(1, ptr);

    assert_eq!(ptr, manager.acquired_pointers[thread_id][1].load(Ordering::Relaxed));

    manager.release(1);
    assert_eq!(ptr::null_mut(), manager.acquired_pointers[thread_id][1].load(Ordering::Relaxed));
  }

  mod retire_test {
    use std::sync::atomic::Ordering;
    use std::sync::atomic::AtomicBool;
    use std::ptr;
    use std::sync::Barrier;
    use std::sync::Arc;
    use std::thread;
    use std::mem::transmute;

    use ::collection::hazard_pointer_manager::HazardPointerManager;
    use ::thread::thread_context::ThreadContext;
    use super::SimpleHazardPointerManager;
    use super::Config;

    static flags: [AtomicBool;3] = [AtomicBool::new(false), AtomicBool::new(false), AtomicBool::new(false)];

    fn free_and_set(p: *mut usize) {
      let v = unsafe { *p };
      flags[v-1].store(true, Ordering::Relaxed);
    }

    #[test]
    fn test_retire() {
      let manager: Arc<SimpleHazardPointerManager<usize>> = Arc::new(SimpleHazardPointerManager::new(Config { 
        thread_num: 2, 
        pointer_num: 2,
        scan_threshold: 1,
        id: 1
      }, Some(Box::new(free_and_set))));

      let data1 = Box::into_raw(Box::new(1usize)) as usize;
      let data2 = Box::into_raw(Box::new(2usize)) as usize;
      let data3 = Box::into_raw(Box::new(3usize)) as usize;

      let barrier1 = Arc::new(Barrier::new(2));
      let barrier2 = Arc::new(Barrier::new(2));
      let barrier3 = Arc::new(Barrier::new(2));

      let thread1 = {
        let barrier1_clone = barrier1.clone();
        let barrier2_clone = barrier2.clone();
        let barrier3_clone = barrier3.clone();
        let manager_clone = manager.clone();
        thread::spawn(move || {
          ThreadContext::set_current(ThreadContext::new(1));
          let p1 = unsafe { transmute::<usize, *mut usize>(data1) };
          manager_clone.acquire(0, p1);
          barrier1_clone.wait();
          barrier2_clone.wait();
          manager_clone.release(0);
          barrier3_clone.wait();
        })
      };

      let thread2 = {
        let barrier1_clone = barrier1.clone();
        let barrier2_clone = barrier2.clone();
        let barrier3_clone = barrier3.clone();
        let manager_clone = manager.clone();
        thread::spawn(move || {
          ThreadContext::set_current(ThreadContext::new(2));
          let p1 = unsafe { transmute::<usize, *mut usize>(data1) };
          let p2 = unsafe { transmute::<usize, *mut usize>(data2) };
          let p3 = unsafe { transmute::<usize, *mut usize>(data3) };
          barrier1_clone.wait();
          manager_clone.retire(p1);
          manager_clone.retire(p2);
          assert!(!flags[0].load(Ordering::Relaxed));
          assert!(flags[1].load(Ordering::Relaxed));
          barrier2_clone.wait();
          barrier3_clone.wait();  
          manager_clone.retire(p3);
          assert!(flags[0].load(Ordering::Relaxed));
          assert!(flags[1].load(Ordering::Relaxed));
          assert!(flags[2].load(Ordering::Relaxed));
        })
      };

      assert!(thread2.join().is_ok());
    }
  }
}
