use std::vec::Vec;
use std::collections::HashSet;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::ptr;
use std::boxed::Box;
use std::collections::HashMap;
use std::cell::RefCell;
use std::mem::transmute;


use ::thread::thread_context::ThreadContext;
use super::hazard_pointer_manager::HazardPointerManager;
use super::hazard_pointer_manager::RetiredPointer;
use super::hazard_pointer_manager::is_null;


type AcquiredPointers = Box<[AtomicUsize]>;
type RetiredPointers = Vec<RetiredPointer>;
type Id = u32;

thread_local! {
  static RETIRED_POINTERS: RefCell<HashMap<Id, RetiredPointers>> = RefCell::new(HashMap::new()); 
}

#[derive(Debug)]
pub struct Config {
    pub thread_num: usize,
    pub pointer_num: usize,
    // Maximum number of pointers acquired per thread.
    pub scan_threshold: usize,
    // The threshold to trigger scan.
    pub id: Id                        // Hazard pointer manager id.
}

/// A simple implementation of hazard_pointer_manager.
///
/// This implementation exists only for demo and a serious implementation should cooperate with
/// thread manager.
///
pub struct SimpleHazardPointerManager {
    //TODO: Replace pointers with another more efficient type to avoid false sharing
    acquired_pointers: Box<[AcquiredPointers]>,
    config: Config
}

impl SimpleHazardPointerManager {
    pub fn new(config: Config) -> SimpleHazardPointerManager {
        let acquired_pointers = (0 ... config.thread_num).map(|_| {
            (0..config.pointer_num)
                .map(|_| AtomicUsize::default())
                .collect::<Vec<AtomicUsize>>()
                .into_boxed_slice()
        })
            .collect::<Vec<AcquiredPointers>>()
            .into_boxed_slice();

        SimpleHazardPointerManager {
            acquired_pointers: acquired_pointers,
            config: config
        }
    }

    fn with_retired_pointers<F, V>(&self, f: F) -> V
        where F: FnOnce(&mut RetiredPointers) -> V {
        RETIRED_POINTERS.with(|cell| {
            let mut map = cell.borrow_mut();
            if !map.contains_key(&self.config.id) {
                map.insert(self.config.id, Vec::new());
            }

            f(map.get_mut(&self.config.id).unwrap())
        })
    }

    fn scan(&self, retired_pointer: &mut RetiredPointers) {
        let thread_id = ThreadContext::current().thread_id;
        let acquired_pointers = self.acquired_pointers
            .iter()
            .flat_map(|v| {
                v.iter()
                    .map(|p| p.load(Ordering::Acquire))
                    .filter(|&p| !is_null(p))
            }).collect::<HashSet<usize>>();


        retired_pointer.retain(|p| acquired_pointers.contains(&p.ptr));
    }
}

impl HazardPointerManager for SimpleHazardPointerManager {
    fn acquire(&self, idx: usize, pointer: usize) {
        let thread_context = ThreadContext::current();
        self.acquired_pointers[thread_context.thread_id][idx].store(pointer, Ordering::Release);
    }

    fn release(&self, idx: usize) {
        let thread_context = ThreadContext::current();
        self.acquired_pointers[thread_context.thread_id][idx].store(0usize, Ordering::Release);
    }

    fn retire(&self, ptr: RetiredPointer) {
        self.with_retired_pointers(|retired_pointers| {
            retired_pointers.push(ptr);

            if retired_pointers.len() > self.config.scan_threshold {
                self.scan(retired_pointers);
            }
        })
    }
}

unsafe impl Sync for SimpleHazardPointerManager {}

unsafe impl Send for SimpleHazardPointerManager {}


#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::ptr;
    use std::sync::Barrier;
    use std::sync::Arc;
    use std::thread;
    use std::mem::transmute;

    use ::collection::hazard_pointer_manager::HazardPointerManager;
    use ::collection::hazard_pointer_manager::is_null;
    use ::thread::thread_context::ThreadContext;
    use super::SimpleHazardPointerManager;
    use super::Config;

    #[test]
    fn test_init() {
        let manager: SimpleHazardPointerManager = SimpleHazardPointerManager::new(Config {
            thread_num: 10,
            pointer_num: 4,
            scan_threshold: 5,
            id: 1
        });
        assert_eq!(11, manager.acquired_pointers.len());
        assert_eq!(4, manager.acquired_pointers[0].len());
        manager.with_retired_pointers(|retired_pointers| {
            assert_eq!(0, retired_pointers.len());
        });
    }

    #[test]
    fn test_acquire_release() {
        let manager: SimpleHazardPointerManager = SimpleHazardPointerManager::new(Config {
            thread_num: 10,
            pointer_num: 4,
            scan_threshold: 5,
            id: 1
        });

        let thread_id = 3;
        ThreadContext::set_current(ThreadContext::new(thread_id));

        let ptr = &mut 12u32 as *mut u32 as usize;
        manager.acquire(1, ptr);

        assert_eq!(ptr, manager.acquired_pointers[thread_id][1].load(Ordering::Relaxed));

        manager.release(1);
        assert!(is_null(manager.acquired_pointers[thread_id][1].load(Ordering::Relaxed)));
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
        use ::collection::hazard_pointer_manager::RetiredPointer;
        use ::thread::thread_context::ThreadContext;
        use super::SimpleHazardPointerManager;
        use super::Config;

        static flags: [AtomicBool; 3] = [AtomicBool::new(false), AtomicBool::new(false), AtomicBool::new(false)];

        fn free_and_set(p: usize) {
            let p = unsafe { transmute::<usize, *mut usize>(p) };
            let v = unsafe { *p };
            flags[v - 1].store(true, Ordering::Relaxed);
        }

        #[test]
        fn test_retire() {
            let manager: Arc<SimpleHazardPointerManager> = Arc::new(SimpleHazardPointerManager::new(Config {
                thread_num: 2,
                pointer_num: 2,
                scan_threshold: 1,
                id: 1
            }));

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
                    manager_clone.acquire_ptr(0, p1);
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
                    let p1 = RetiredPointer { ptr: data1, deleter: Box::new(free_and_set) };
                    let p2 = RetiredPointer { ptr: data2, deleter: Box::new(free_and_set) };
                    let p3 = RetiredPointer { ptr: data3, deleter: Box::new(free_and_set) };
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
