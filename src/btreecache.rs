use std::collections::BTreeMap;
use priority_queue::PriorityQueue;
use std::cmp::Reverse;
use std::hash::Hash;

pub struct BTreeCache<K: Ord + Hash, V> {
    pub cache: BTreeMap<K, V>,
    pqueue: PriorityQueue<K, Reverse<u64>>,
    ticker: u64,
}

impl<K: Ord + Hash + Clone, V> BTreeCache<K, V> {
    pub fn new() -> BTreeCache<K, V> {
        BTreeCache {
            cache: BTreeMap::new(),
            pqueue: PriorityQueue::new(),
            ticker: 0,
        }
    }

    pub fn insert(&mut self, key: K, value: V) {
        self.cache.insert(key.clone(), value);
        self.pqueue.push(key, Reverse(self.ticker));
        self.ticker += 1;
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        if self.cache.contains_key(key) {
            self.pqueue.change_priority(key, Reverse(self.ticker));
            self.ticker += 1;
        }
        self.cache.get_mut(key)
    }

    pub fn remove_oldest(&mut self) -> Option<(K, V)> {
        self.pqueue.pop().map(|(key, _)| {
            let value = self.cache.remove(&key).unwrap();
            (key, value)
        })
    }

    pub fn len(&self) -> usize {
        self.cache.len()
    }
}
