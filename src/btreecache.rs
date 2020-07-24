use std::collections::BTreeMap;
use priority_queue::PriorityQueue;
use std::cmp::Reverse;

pub struct<K, V> BTreeCache<K, V> {
    cache: BTreeMap<K, V>,
    pqueue: PriorityQueue<K, Reverse<u64>>,
    ticker: u64,
}

impl<K: Ord, V> BTreeCache<K, V> {
    pub fn new() -> BTreeCache<K, V> {
        BTreeCache {
            cache: BTreeMap::new(),
            pqueue: PriorityQueue::new(),
            ticker: 0,
        }
    }

    pub fn insert(&mut self, key: K, value: V) {
        self.cache.insert(key, value);
        self.pqueue.insert(key, Reverse(self.ticker));
        self.ticker += 1;
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.cache.get_mut(key).map(|_| {
            self.pqueue.change_priority(key, Reverse(self.ticker));
            self.ticker += 1;
        })
    }

    pub fn remove_oldest(&mut self) -> Option<(K, V)> {
        self.pqueue.pop().and_then(|(key, _)| self.cache.remove(key))
    }

    pub fn len(&self) -> usize {
        self.cache.len()
    }
}
