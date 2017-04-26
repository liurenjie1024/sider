use std::sync::atomic::AtomicPtr;
use std::hash::Hash;
use std::hash::Hasher;
use std::cmp::Eq;
use std::marker::Sync;
use std::marker::Send;
use std::cell::RefCell;
use std::boxed::Box;
use std::default::Default;
use std::ops::DerefMut;
use std::collections::hash_map::RandomState;
use std::collections::hash_map::DefaultHasher;
use std::sync::atomic::Ordering;
use std::ptr;
use std::hash::BuildHasher;
use super::hazard_pointer_manager::HazardPointerManager;
use super::simple_hazard_pointer::SimpleHazardPointerManager;


type HashUnit = u64;

pub struct Node<K, V> {
    next: AtomicPtr<Node<K, V>>,
    hash_code: HashUnit,
    key: K,
    value: V
}



pub struct ConcurrentHashMap<K, V, M=SimpleHazardPointerManager<Node<K, V>>> {
    head: AtomicPtr<Node<K, V>>,
    memory_manager: M,
    hasher_builder: RandomState
}


impl<K, V, M> ConcurrentHashMap<K, V, M>
    where K: Hash + Eq,
          M: HazardPointerManager<Node<K, V>>
          
{
    pub fn new(m: M) -> ConcurrentHashMap<K, V, M> {
        ConcurrentHashMap { 
            head: AtomicPtr::default(),
            memory_manager: m,
            hasher_builder: RandomState::new()
        }
    }

    pub fn delete(&self, key: &K) {
        self.remove(key, |_| {})
    }

    pub fn remove<F, R>(&self, key: &K, func: F) -> R 
        where F: for<'a> Fn(Option<&'a V>) -> R
    {
        let hash_code = self.make_hash(key);

        let pre = self.find(key, hash_code);
        let cur_ptr = pre.load(Ordering::Relaxed);

        if cur_ptr.is_null() {
            func(None)
        } else {
            let cur = unsafe { &mut *cur_ptr };
            let result = func(Some(&cur.value));
            pre.store(cur.next.load(Ordering::Relaxed), Ordering::Release);
            cur.next.store(ptr::null_mut(), Ordering::Release);
            self.memory_manager.retire(cur_ptr);
            result
        }
    }

    pub fn put(&self, key: K, value: V) {
        self.insert(key, value, |_| {})
    }

    pub fn insert<F, R>(&self, key: K, value: V, func: F) -> R
        where F: for<'a> Fn(Option<&'a V>) -> R
    {
        let hash_code = self.make_hash(&key);

        let pre = self.find(&key, hash_code);
        let cur_ptr = pre.load(Ordering::Relaxed);

        let new_node_ptr = Box::into_raw(Box::new(Node { next: AtomicPtr::default(), 
            hash_code: hash_code,
            key: key,
            value: value
        }));

        if cur_ptr.is_null() {
            pre.store(new_node_ptr, Ordering::Release);
            func(None)
        } else {
            let cur = unsafe { &mut *cur_ptr };
            let result = func(Some(&cur.value));
            let new_node = unsafe { &mut *new_node_ptr };
            new_node.next.store(cur.next.load(Ordering::Relaxed), Ordering::Relaxed);
            pre.store(new_node_ptr, Ordering::Release);
            cur.next.store(ptr::null_mut(), Ordering::Release);
            self.memory_manager.retire(cur_ptr);
            result
        }
    }

    pub fn get<F, R>(&self, key: &K, func: F) -> R
        where F: for<'a> Fn(Option<&'a V>) -> R
    {
        let hash_code = self.make_hash(key);
        unsafe {
            loop {
                let mut pre = &self.head;
                let mut pre_ptr = ptr::null_mut();

                // Loop invariant: 
                // 1. self.memory_manager.acquire(0, pre_ptr) or pre_ptr == null
                // 2. pre == head or pre = &*pre_ptr.next
                loop {
                    let cur_ptr = pre.load(Ordering::Acquire);
                    self.memory_manager.acquire(1, cur_ptr);

                    if cur_ptr != pre.load(Ordering::Acquire) {
                        self.memory_manager.release(0);
                        self.memory_manager.release(1);
                        break;
                    }

                    if cur_ptr.is_null() {
                        self.memory_manager.release(0);
                        return func(None);
                    } else {
                        let cur = &*cur_ptr;

                        if cur.hash_code==hash_code && cur.key.eq(key) {
                            let result = func(Some(&cur.value));
                            self.memory_manager.release(0);
                            self.memory_manager.release(1);
                            return result;
                        } else {
                            pre_ptr = cur_ptr;
                            self.memory_manager.acquire(0, pre_ptr);
                            self.memory_manager.release(1);
                            pre = &(&*pre_ptr).next;
                        }
                    }
                }
            }
        }
    }

    // This method is only used in the single writer thread.
    fn find(&self, key: &K, hash_code: HashUnit) -> &AtomicPtr<Node<K, V>> {
        unsafe {
            let mut pre = &self.head;
            let mut cur_ptr: *mut Node<K, V> = ptr::null_mut();
            
            loop {
                let cur_ptr = pre.load(Ordering::Relaxed);
                if !cur_ptr.is_null() {
                    let cur = &*cur_ptr;
                    if hash_code==cur.hash_code && key.eq(&cur.key) {
                        return pre;
                    } else {
                        pre = &cur.next;
                    }
                } else {
                    return pre
                }
            }
        }
    }


    fn make_hash(&self, key: &K) -> HashUnit {
        let mut hasher = self.hasher_builder.build_hasher();
        key.hash::<DefaultHasher>(&mut hasher);
        hasher.finish()
    }
}

unsafe impl<K: Sync + Send, V: Sync + Send> Sync for ConcurrentHashMap<K, V> {}
unsafe impl<K: Sync + Send, V: Sync + Send> Send for ConcurrentHashMap<K, V> {}


#[cfg(test)]
mod tests {
    use test::Bencher;

    use std::sync::Arc;
    use std::sync::Barrier;
    use std::thread;
    use std::vec::Vec;


    use super::ConcurrentHashMap;
    use super::Node;
    use ::collection::simple_hazard_pointer::SimpleHazardPointerManager;
    use ::collection::simple_hazard_pointer::Config;
    use ::thread::thread_context::ThreadContext;

    type Str = Box<[u8]>;
    type Map = ConcurrentHashMap<Str, u32, SimpleHazardPointerManager<Node<Str, u32>>>;


    #[test]
    fn test_make_hash() {
        let map = Arc::new(make_map());


        let hash1 = {
            let map = map.clone();
            thread::spawn(move || {
                let raw_key = "abcdefg";
                map.make_hash(&make_key(raw_key))
            }).join().unwrap()
        };

        let hash2 = {
            let map = map.clone();
            thread::spawn(move || {
                let raw_key = "abcdefg";
                map.make_hash(&make_key(raw_key))
            }).join().unwrap()
        };

        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_insert() {
        let thread_id = 3;
        ThreadContext::set_current(ThreadContext::new(thread_id));

        let map = make_map();

        let raw_key = "abcdefg";

        let key1 = make_key(raw_key);
        let key2 = make_key(raw_key);

        map.put(key1, 1);

        let value = map.get(&key2, |opt_value| match opt_value {
            Some(&v) => v,
            None => 0,
        });

        assert_eq!(1, value);
    }

    #[test]
    fn test_remove() {
        let thread_id = 3;
        ThreadContext::set_current(ThreadContext::new(thread_id));

        let map = make_map();

        let raw_key = "abcdefg";

        let key1 = make_key(raw_key);
        let key2 = make_key(raw_key);


        let result = map.remove(&key1, |opt_value| match opt_value {
            Some(_) => true,
            None => false,
        });
        assert_eq!(false, result);

        map.put(key1, 1);
        let result = map.remove(&key2, |opt_value| match opt_value {
            Some(&v) => v,
            None => 0,
        });
        assert_eq!(1, result);

        let result = map.remove(&key2, |opt_value| match opt_value {
            Some(_) => true,
            None => false,
        });
        assert_eq!(false, result);
    }

    #[test]
    fn test_concurrent_get() {
        let map = Arc::new(make_map());

        let barrier1 = Arc::new(Barrier::new(2));
        let barrier2 = Arc::new(Barrier::new(2));
        let barrier3 = Arc::new(Barrier::new(2));

        let raw_key = "abcdefg";

        let thread1 = {
            let barrier1 = barrier1.clone();
            let barrier2 = barrier2.clone();
            let barrier3 = barrier3.clone();
            let map = map.clone();
            let key1 = make_key(raw_key);
            let key2 = make_key(raw_key);

            thread::spawn(move || {
                ThreadContext::set_current(ThreadContext::new(1));
                map.put(key1, 1);
                barrier1.wait();
                barrier2.wait();
                map.delete(&key2);
                barrier3.wait();
            });
        };

        let thread2 = {
            let barrier1 = barrier1.clone();
            let barrier2 = barrier2.clone();
            let barrier3 = barrier3.clone();
            let map = map.clone();
            let key1 = make_key(raw_key);

            thread::spawn(move || {
                ThreadContext::set_current(ThreadContext::new(1));
                barrier1.wait();
                let result = map.get(&key1, |r| match r {
                    Some(x) => *x,
                    None => 0,
                });
                assert_eq!(1, result);
                barrier2.wait();
                barrier3.wait();
                
                let result = map.get(&key1, |r| match r {
                    Some(&x) => x,
                    None => 0,
                });
                assert_eq!(0, result);
            })
        };

        assert!(thread2.join().is_ok());
    }

    #[bench]
    fn bench_get_only(b: &mut Bencher) {
        let map = Arc::new(make_map());

        ThreadContext::set_current(ThreadContext::new(1));
        let keys = ["abcde", "bcdef", "cdefg", "defgh", "efghi", "fghijk", "jklmn"];


        let mut idx: usize = 0;
        for k in &keys {
            idx = idx + 1;
            map.put(make_key(k), idx as u32);
        }

        let strs = keys.iter().map(|x| make_key(x)).collect::<Vec<Str>>();
        

        b.iter(move || {
            ThreadContext::set_current(ThreadContext::new(2));
            idx = (idx+1)%strs.len();
            map.get(&strs[idx], |r| {*r.unwrap()})
        });
    }

    fn make_map() -> Map {
        Map::new (
            SimpleHazardPointerManager::new (
                Config {
                    thread_num: 100, 
                    pointer_num: 2,
                    scan_threshold: 5,
                    id: 1
                }, None
            )
        ) 
    }

    fn make_key(raw_key: &str) -> Str {
        raw_key.to_string().into_bytes().into_boxed_slice()
    }
}