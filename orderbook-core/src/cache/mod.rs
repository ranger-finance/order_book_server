pub mod lru_cache;

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use lru::LruCache;

use crate::orderbook::types::Coin;
use crate::types::L2Book;

#[derive(Clone, Debug)]
pub struct OrderBookCache {
    cache: Arc<Mutex<LruCache<Coin, L2Book>>>,
}

impl OrderBookCache {
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self { cache: Arc::new(Mutex::new(LruCache::new(capacity))) }
    }

    pub fn get(&self, coin: &Coin) -> Option<L2Book> {
        let mut cache = self.cache.lock().unwrap();
        cache.get(coin).cloned()
    }

    pub fn put(&self, coin: Coin, book: L2Book) {
        let mut cache = self.cache.lock().unwrap();
        cache.put(coin, book);
    }

    pub fn remove(&self, coin: &Coin) -> Option<L2Book> {
        let mut cache = self.cache.lock().unwrap();
        cache.pop(coin)
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
