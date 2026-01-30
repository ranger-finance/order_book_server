pub mod lru_cache;

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use lru::LruCache;

use orderbook_normaliser::models::UnifiedOrderbook;

#[derive(Clone, Debug)]
pub struct OrderBookCache {
    cache: Arc<Mutex<LruCache<String, UnifiedOrderbook>>>,
}

impl OrderBookCache {
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self { cache: Arc::new(Mutex::new(LruCache::new(capacity))) }
    }

    pub fn get(&self, symbol: &str) -> Option<UnifiedOrderbook> {
        let mut cache = self.cache.lock().unwrap();
        cache.get(symbol).cloned()
    }

    pub fn put(&self, symbol: String, book: UnifiedOrderbook) {
        let mut cache = self.cache.lock().unwrap();
        cache.put(symbol, book);
    }

    pub fn remove(&self, symbol: &str) -> Option<UnifiedOrderbook> {
        let mut cache = self.cache.lock().unwrap();
        cache.pop(symbol)
    }

    pub fn len(&self) -> usize {
        let cache = self.cache.lock().unwrap();
        cache.len()
    }
}

impl Default for OrderBookCache {
    fn default() -> Self {
        Self::new(NonZeroUsize::new(1000).unwrap())
    }
}
