use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

pub struct CoinLruCache<K, V> {
    cache: Arc<Mutex<LruCache<K, V>>>,
}

impl<K: Eq + std::hash::Hash + Clone, V> CoinLruCache<K, V> {
    pub fn new(capacity: usize) -> Self {
        Self { cache: Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(capacity).unwrap()))) }
    }

    pub fn insert(&self, key: K, value: V) {
        let mut cache = self.cache.lock().unwrap();
        cache.put(key, value);
    }

    pub fn get(&self, key: &K) -> Option<V>
    where
        V: Clone,
    {
        let mut cache = self.cache.lock().unwrap();
        cache.get(key).cloned()
    }

    pub fn contains_key(&self, key: &K) -> bool {
        let cache = self.cache.lock().unwrap();
        cache.contains(key)
    }

    pub fn remove(&self, key: &K) -> Option<V> {
        let mut cache = self.cache.lock().unwrap();
        cache.pop(key)
    }

    pub fn clear(&self) {
        let mut cache = self.cache.lock().unwrap();
        cache.clear();
    }

    pub fn len(&self) -> usize {
        let cache = self.cache.lock().unwrap();
        cache.len()
    }

    pub fn is_empty(&self) -> bool {
        let cache = self.cache.lock().unwrap();
        cache.is_empty()
    }
}

impl<K, V> Clone for CoinLruCache<K, V> {
    fn clone(&self) -> Self {
        Self { cache: Arc::clone(&self.cache) }
    }
}
