use crate::{
    cache::{OrderBookCache, lru_cache::CoinLruCache},
    redis::RedisPublisher,
};
use bb8_redis::redis::RedisError;
use orderbook_normaliser::models::{Exchange, UnifiedOrderbook};
use std::sync::Arc;

pub struct L2Emitter {
    cache: OrderBookCache,
    redis_publisher: Arc<RedisPublisher>,
    last_snapshot_block: CoinLruCache<String, u64>,
    block_height: u64,
    snapshot_interval: u64,
}

impl L2Emitter {
    pub fn new(cache: OrderBookCache, redis_publisher: Arc<RedisPublisher>, snapshot_interval: u64) -> Self {
        Self {
            cache,
            redis_publisher,
            last_snapshot_block: CoinLruCache::<String, u64>::new(1000),
            block_height: 0,
            snapshot_interval,
        }
    }

    pub fn new_with_default_interval(cache: OrderBookCache, redis_publisher: Arc<RedisPublisher>) -> Self {
        Self::new(cache, redis_publisher, 100)
    }

    pub async fn process_coin(
        &mut self,
        symbol: &str,
        book: &UnifiedOrderbook,
        block_height: u64,
    ) -> Result<(), RedisError> {
        self.block_height = block_height;

        let mut published = false;
        if self.should_emit_snapshot(symbol) {
            self.redis_publisher.publish_orderbook(Exchange::Hyperliquid, symbol, book).await?;
            self.last_snapshot_block.insert(symbol.to_string(), block_height);
            published = true;
        } else if let Some(prev_book) = self.cache.get(symbol) {
            if self.has_changes(&prev_book, book) {
                self.redis_publisher.publish_orderbook(Exchange::Hyperliquid, symbol, book).await?;
                published = true;
            }
        } else {
            self.redis_publisher.publish_orderbook(Exchange::Hyperliquid, symbol, book).await?;
            self.last_snapshot_block.insert(symbol.to_string(), block_height);
            published = true;
        }

        if published {
            self.redis_publisher.publish_update_notification(Exchange::Hyperliquid, symbol).await?;
        }

        self.cache.put(symbol.to_string(), book.clone());

        Ok(())
    }

    pub fn should_emit_snapshot(&self, symbol: &str) -> bool {
        let last_block = self.last_snapshot_block.get(&symbol.to_string()).unwrap_or(0);
        self.block_height - last_block >= self.snapshot_interval
    }

    pub fn has_changes(&self, old_book: &UnifiedOrderbook, new_book: &UnifiedOrderbook) -> bool {
        old_book.timestamp_ms != new_book.timestamp_ms
    }

    pub const fn block_height(&self) -> u64 {
        self.block_height
    }

    pub fn set_block_height(&mut self, height: u64) {
        self.block_height = height;
    }

    pub const fn snapshot_interval(&self) -> u64 {
        self.snapshot_interval
    }

    pub fn set_snapshot_interval(&mut self, interval: u64) {
        self.snapshot_interval = interval;
    }
}
