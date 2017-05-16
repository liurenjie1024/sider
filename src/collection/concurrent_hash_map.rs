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
use std::vec::Vec;
use std::mem::transmute;
use super::hazard_pointer_manager::HazardPointerManager;
use super::hazard_pointer_manager::RetiredPointer;
use super::simple_hazard_pointer::SimpleHazardPointerManager;


type HashUnit = u64;


const REVERSED_BITS: [u8; 256] =
    [0x0, 0x80, 0x40, 0xC0, 0x20, 0xA0, 0x60, 0xE0, 0x10, 0x90, 0x50, 0xD0, 0x30, 0xB0, 0x70,
     0xF0, 0x8, 0x88, 0x48, 0xC8, 0x28, 0xA8, 0x68, 0xE8, 0x18, 0x98, 0x58, 0xD8, 0x38, 0xB8,
     0x78, 0xF8, 0x4, 0x84, 0x44, 0xC4, 0x24, 0xA4, 0x64, 0xE4, 0x14, 0x94, 0x54, 0xD4, 0x34,
     0xB4, 0x74, 0xF4, 0xC, 0x8C, 0x4C, 0xCC, 0x2C, 0xAC, 0x6C, 0xEC, 0x1C, 0x9C, 0x5C, 0xDC,
     0x3C, 0xBC, 0x7C, 0xFC, 0x2, 0x82, 0x42, 0xC2, 0x22, 0xA2, 0x62, 0xE2, 0x12, 0x92, 0x52,
     0xD2, 0x32, 0xB2, 0x72, 0xF2, 0xA, 0x8A, 0x4A, 0xCA, 0x2A, 0xAA, 0x6A, 0xEA, 0x1A, 0x9A,
     0x5A, 0xDA, 0x3A, 0xBA, 0x7A, 0xFA, 0x6, 0x86, 0x46, 0xC6, 0x26, 0xA6, 0x66, 0xE6, 0x16,
     0x96, 0x56, 0xD6, 0x36, 0xB6, 0x76, 0xF6, 0xE, 0x8E, 0x4E, 0xCE, 0x2E, 0xAE, 0x6E, 0xEE,
     0x1E, 0x9E, 0x5E, 0xDE, 0x3E, 0xBE, 0x7E, 0xFE, 0x1, 0x81, 0x41, 0xC1, 0x21, 0xA1, 0x61,
     0xE1, 0x11, 0x91, 0x51, 0xD1, 0x31, 0xB1, 0x71, 0xF1, 0x9, 0x89, 0x49, 0xC9, 0x29, 0xA9,
     0x69, 0xE9, 0x19, 0x99, 0x59, 0xD9, 0x39, 0xB9, 0x79, 0xF9, 0x5, 0x85, 0x45, 0xC5, 0x25,
     0xA5, 0x65, 0xE5, 0x15, 0x95, 0x55, 0xD5, 0x35, 0xB5, 0x75, 0xF5, 0xD, 0x8D, 0x4D, 0xCD,
     0x2D, 0xAD, 0x6D, 0xED, 0x1D, 0x9D, 0x5D, 0xDD, 0x3D, 0xBD, 0x7D, 0xFD, 0x3, 0x83, 0x43,
     0xC3, 0x23, 0xA3, 0x63, 0xE3, 0x13, 0x93, 0x53, 0xD3, 0x33, 0xB3, 0x73, 0xF3, 0xB, 0x8B,
     0x4B, 0xCB, 0x2B, 0xAB, 0x6B, 0xEB, 0x1B, 0x9B, 0x5B, 0xDB, 0x3B, 0xBB, 0x7B, 0xFB, 0x7,
     0x87, 0x47, 0xC7, 0x27, 0xA7, 0x67, 0xE7, 0x17, 0x97, 0x57, 0xD7, 0x37, 0xB7, 0x77, 0xF7,
     0xF, 0x8F, 0x4F, 0xCF, 0x2F, 0xAF, 0x6F, 0xEF, 0x1F, 0x9F, 0x5F, 0xDF, 0x3F, 0xBF, 0x7F, 0xFF];

struct Node<K, V> {
    next: AtomicPtr<Node<K, V>>,
    split_order_key: HashUnit, // split order hash code
    key: K,
    value: V,
}

impl<K, V> Node<K, V> {
    fn make_sentinel_node(bucket_pos: usize) -> Box<Node<K, V>>
        where K: Default,
              V: Default
    {
        Box::new(Node {
                     next: AtomicPtr::default(),
                     split_order_key: reverse_bits(bucket_pos as u64),
                     key: K::default(),
                     value: V::default(),
                 })
    }
}


struct Bucket<K, V> {
    head: AtomicPtr<Node<K, V>>,
}

impl<K, V> Bucket<K, V> {
    fn new() -> Bucket<K, V> {
        Bucket { head: AtomicPtr::default() }
    }
}

type Buckets<K, V> = Box<[Bucket<K, V>]>;


/// A single writer, multi reader concurrent hash map.
pub struct ConcurrentHashMap<K, V, M = SimpleHazardPointerManager> {
    memory_manager: M,
    hasher_builder: RandomState,
    buckets: AtomicPtr<Buckets<K, V>>,
}


impl<K, V, M> ConcurrentHashMap<K, V, M>
    where K: Hash + Eq + Default,
          V: Default,
          M: HazardPointerManager
{
    pub fn new(m: M, capacity: usize) -> ConcurrentHashMap<K, V, M> {
        let table_size = table_size_for(capacity);
        let buckets: Box<Buckets<K, V>> = Box::new((0..table_size)
                                                       .map(|_| Bucket::new())
                                                       .collect::<Vec<Bucket<K, V>>>()
                                                       .into_boxed_slice());
        let head = Box::into_raw(Node::make_sentinel_node(0usize));
        buckets[0].head.store(head, Ordering::Relaxed);

        ConcurrentHashMap {
            memory_manager: m,
            hasher_builder: RandomState::new(),
            buckets: AtomicPtr::new(Box::into_raw(buckets)),
        }
    }

    pub fn delete(&self, key: &K) -> bool {
        self.remove(key, |v| v.is_some())
    }

    pub fn remove<F, R>(&self, key: &K, func: F) -> R
        where F: for<'a> Fn(Option<&'a V>) -> R
    {
        let hash_code = self.make_hash(key);
        let split_order_key = make_split_order_key(hash_code);

        let (pre, found) = self.find(key, hash_code, split_order_key);
        let cur_ptr = pre.load(Ordering::Relaxed);

        if !found {
            func(None)
        } else {
            let cur = unsafe { &mut *cur_ptr };
            let result = func(Some(&cur.value));
            pre.store(cur.next.load(Ordering::Relaxed), Ordering::Release);
            cur.next.store(ptr::null_mut(), Ordering::Release);
            self.memory_manager.retire_ptr(cur_ptr);
            result
        }
    }

    pub fn put(&self, key: K, value: V) {
        self.insert(key, move |_| (value, ()))
    }

    pub fn insert<F, R>(&self, key: K, func: F) -> R
        where F: for<'a> FnOnce(Option<&'a V>) -> (V, R)
    {
        let hash_code = self.make_hash(&key);
        let split_order_key = make_split_order_key(hash_code);
        self.insert_sentinel_node(hash_code);

        let (pre, found) = self.find(&key, hash_code, split_order_key);
        let cur_ptr = pre.load(Ordering::Relaxed);

        let new_node_ptr = Box::into_raw(Box::new(Node {
                                                      next: AtomicPtr::default(),
                                                      split_order_key: split_order_key,
                                                      key: key,
                                                      value: V::default(),
                                                  }));

        if !found {
            let (value, result) = func(None);
            unsafe {
                (*new_node_ptr).value = value;
            }
            pre.store(new_node_ptr, Ordering::Release);
            result
        } else {
            let cur = unsafe { &mut *cur_ptr };
            let (value, result) = func(Some(&cur.value));
            let new_node = unsafe { &mut *new_node_ptr };
            new_node.value = value;
            new_node.next.store(cur.next.load(Ordering::Relaxed), Ordering::Relaxed);
            pre.store(new_node_ptr, Ordering::Release);
            cur.next.store(ptr::null_mut(), Ordering::Release);
            self.memory_manager.retire_ptr(cur_ptr);
            result
        }
    }

    pub fn get<F, R>(&self, key: &K, func: F) -> R
        where F: for<'a> Fn(Option<&'a V>) -> R
    {
        let hash_code = self.make_hash(key);
        let split_order_key = make_split_order_key(hash_code);
        unsafe {
            loop {
                let mut pre = &self.bucket_of(hash_code).head;
                let mut pre_ptr = ptr::null_mut();

                // Loop invariant:
                // 1. self.memory_manager.acquire(0, pre_ptr) or pre_ptr == null
                // 2. pre == head or pre = &*pre_ptr.next
                loop {
                    let cur_ptr = pre.load(Ordering::Acquire);
                    self.memory_manager.acquire_ptr(1, cur_ptr);

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

                        if cur.split_order_key == split_order_key && cur.key.eq(key) {
                            let result = func(Some(&cur.value));
                            self.memory_manager.release(0);
                            self.memory_manager.release(1);
                            return result;
                        } else if cur.split_order_key > split_order_key {
                            let result = func(None);
                            self.memory_manager.release(0);
                            self.memory_manager.release(1);
                            return result;
                        } else {
                            pre_ptr = cur_ptr;
                            self.memory_manager.acquire_ptr(0, pre_ptr);
                            self.memory_manager.release(1);
                            pre = &(&*pre_ptr).next;
                        }
                    }
                }
            }
        }
    }

    fn insert_sentinel_node(&self, hash_code: HashUnit) {
        let bucket = self.bucket_of(hash_code);
        let bucket_pos = self.get_bucket_pos(hash_code);
        if bucket.head.load(Ordering::Relaxed).is_null() {
            let parent = self.get_parent(bucket_pos);
            self.insert_sentinel_node(parent as u64);
            let node = Node::make_sentinel_node(bucket_pos);
            let (pre, found) =
                self.find_from_node(&node.key,
                                    node.split_order_key,
                                    &self.bucket_of(parent as u64).head);
            assert!(!found);
            let next = pre.load(Ordering::Relaxed);
            node.next.store(next, Ordering::Release);
            pre.store(Box::into_raw(node), Ordering::Release);
        }
    }

    fn bucket_of(&self, hash_code: HashUnit) -> &Bucket<K, V> {
        unsafe {
            let buckets = &*(self.buckets.load(Ordering::Relaxed));
            &buckets[(buckets.len() - 1) & (hash_code as usize)]
        }
    }

    fn get_bucket_size(&self) -> usize {
        unsafe { (*self.buckets.load(Ordering::Relaxed)).len() }
    }

    fn get_bucket_pos(&self, hash_code: HashUnit) -> usize {
        (self.get_bucket_size()-1) & (hash_code as usize)
    }

    // Find the nearest bucket sentinel node that's before this bucket.
    // This is an optimization to GET_PARENT algorithm which simply unset the msb.
    // Let's assume that the length of bucket is 32, there are two cases:
    // * bucket_pos=10110, e.g. the ith bit is set, get_parent simply unset ith bit and returns
    // 00110
    // * bucket_pos=00110, e.g. the ith bit is unset, get_parent should return 11010
    fn get_parent(&self, bucket_pos: usize) -> usize {
        let only_msb = reserve_only_msb(bucket_pos as u64) as usize;
        let right_mask = only_msb - 1;
        let left_mask = !right_mask;

        (right_mask & bucket_pos) | (right_mask & (self.get_bucket_size() - bucket_pos) & !only_msb)
    }

    // This method is only used in the single writer thread.
    fn find(&self,
            key: &K,
            hash_code: HashUnit,
            split_order_key: HashUnit)
            -> (&AtomicPtr<Node<K, V>>, bool) {
        unsafe {
            let head = &self.bucket_of(hash_code).head;
            self.find_from_node(key, split_order_key, head)
        }
    }

    fn find_from_node<'a, 'b>(&'a self,
                              key: &'b K,
                              split_order_key: HashUnit,
                              head: &'a AtomicPtr<Node<K, V>>)
                              -> (&'a AtomicPtr<Node<K, V>>, bool) {
        unsafe {
            let mut pre = head;
            let mut cur_ptr: *mut Node<K, V> = ptr::null_mut();

            loop {
                let cur_ptr = pre.load(Ordering::Relaxed);
                if !cur_ptr.is_null() {
                    let cur = &*cur_ptr;
                    if cur.split_order_key > split_order_key {
                        return (pre, false);
                    }
                    if split_order_key == cur.split_order_key && key.eq(&cur.key) {
                        return (pre, true);
                    } else {
                        pre = &cur.next;
                    }
                } else {
                    return (pre, false);
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


fn reverse_bits(hash_code: u64) -> u64 {
    let mut ret = 0u64;
    for x in 0..4 {
        let left_shifts = 8 * (7 - x);
        let right_shifts = 8 * x;
        let left = REVERSED_BITS[((hash_code >> left_shifts) & 0xFF) as usize] as u64;
        let right = REVERSED_BITS[((hash_code >> right_shifts) & 0xFF) as usize] as u64;
        ret |= right << left_shifts;
        ret |= left << right_shifts;
    }
    ret
}

fn make_split_order_key(hash_code: u64) -> u64 {
    const MASK: u64 = 1u64 << 63;
    reverse_bits(hash_code | MASK)
}

// Reserve only most significant bit.
fn reserve_only_msb(hash_code: u64) -> u64 {
    let mut ret = hash_code;
    ret |= ret >> 1;
    ret |= ret >> 2;
    ret |= ret >> 4;
    ret |= ret >> 8;
    ret |= ret >> 16;
    ret |= ret >> 32;

    ret & !(ret >> 1)
}

fn table_size_for(capacity: usize) -> usize {
    const MAX_CAPACITY: usize = usize::max_value() - (usize::max_value() >> 1);

    if capacity < MAX_CAPACITY {
        let mut ret = capacity - 1;
        ret |= ret >> 1;
        ret |= ret >> 2;
        ret |= ret >> 4;
        ret |= ret >> 8;
        ret |= ret >> 16;
        ret |= ret >> 32;

        ret + 1
    } else {
        MAX_CAPACITY
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
    use std::sync::atomic::AtomicU32;
    use std::sync::atomic::Ordering;


    use super::ConcurrentHashMap;
    use super::Node;
    use collection::simple_hazard_pointer::SimpleHazardPointerManager;
    use collection::simple_hazard_pointer::Config;
    use thread::thread_context::ThreadContext;

    type Str = Box<[u8]>;
    type Map = ConcurrentHashMap<Str, u32, SimpleHazardPointerManager>;


    #[test]
    fn test_make_hash() {
        let map = Arc::new(make_map());


        let hash1 = {
            let map = map.clone();
            thread::spawn(move || {
                              let raw_key = "abcdefg";
                              map.make_hash(&make_key(raw_key))
                          })
                    .join()
                    .unwrap()
        };

        let hash2 = {
            let map = map.clone();
            thread::spawn(move || {
                              let raw_key = "abcdefg";
                              map.make_hash(&make_key(raw_key))
                          })
                    .join()
                    .unwrap()
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


        let result = map.delete(&key1);
        assert_eq!(false, result);

        map.put(key1, 1);
        let result = map.remove(&key2, |opt_value| match opt_value {
            Some(&v) => v,
            None => 0,
        });
        assert_eq!(1, result);

        let result = map.delete(&key2);
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
                   idx = (idx + 1) % strs.len();
                   map.get(&strs[idx], |r| *r.unwrap())
               });
    }

    #[bench]
    fn bench_reverse_bits(b: &mut Bencher) {
        b.iter(|| super::reverse_bits(0x5Fu64));
    }

    #[test]
    fn test_reverse_bits() {
        assert_eq!(0xFA00000000000000u64, super::reverse_bits(0x5Fu64));
        assert_eq!(0x5Fu64, super::reverse_bits(0xFA00000000000000u64));
    }

    fn make_map() -> Map {
        static CONFIG_ID: AtomicU32 = AtomicU32::new(1);

        Map::new(SimpleHazardPointerManager::new(Config {
            thread_num: 100,
            pointer_num: 2,
            scan_threshold: 5,
            id: CONFIG_ID.fetch_add(1, Ordering::SeqCst),
        }), 1024)
    }

    fn make_key(raw_key: &str) -> Str {
        raw_key.to_string().into_bytes().into_boxed_slice()
    }
}
