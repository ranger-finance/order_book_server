use crate::{
    cache::{OrderBookCache, lru_cache::CoinLruCache},
    redis::RedisPublisher,
};
use bb8_redis::redis::RedisError;
use orderbook_normaliser::models::{Exchange, UnifiedOrderbook};
use std::sync::Arc;
use std::time::Instant;

pub struct L2Emitter {
    cache: OrderBookCache,
    redis_publisher: Arc<RedisPublisher>,
    last_snapshot_block: CoinLruCache<String, u64>,
    block_height: u64,
    snapshot_interval: u64,
    streaming_mode: bool,
    snapshot_interval_ms: u64,
    last_emit_time: Instant,
}

impl L2Emitter {
    pub fn new(
        cache: OrderBookCache,
        redis_publisher: Arc<RedisPublisher>,
        snapshot_interval: u64,
        streaming_mode: bool,
        snapshot_interval_ms: Option<u64>,
    ) -> Self {
        let snapshot_interval_ms = snapshot_interval_ms.unwrap_or_else(|| if streaming_mode { 100 } else { 1000 });

        Self {
            cache,
            redis_publisher,
            last_snapshot_block: CoinLruCache::<String, u64>::new(1000),
            block_height: 0,
            snapshot_interval,
            streaming_mode,
            snapshot_interval_ms,
            last_emit_time: Instant::now(),
        }
    }

    pub fn new_with_default_interval(cache: OrderBookCache, redis_publisher: Arc<RedisPublisher>) -> Self {
        Self::new_simple(cache, redis_publisher, 100)
    }

    pub fn new_simple(cache: OrderBookCache, redis_publisher: Arc<RedisPublisher>, snapshot_interval: u64) -> Self {
        Self::new(cache, redis_publisher, snapshot_interval, false, None)
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
            self.update_emit_time();
            published = true;
        } else if let Some(prev_book) = self.cache.get(symbol) {
            if self.has_changes(&prev_book, book) {
                self.redis_publisher.publish_orderbook(Exchange::Hyperliquid, symbol, book).await?;
                published = true;
            }
        } else {
            self.redis_publisher.publish_orderbook(Exchange::Hyperliquid, symbol, book).await?;
            self.last_snapshot_block.insert(symbol.to_string(), block_height);
            self.update_emit_time();
            published = true;
        }

        if published {
            self.redis_publisher.publish_update_notification(Exchange::Hyperliquid, symbol).await?;
        }

        self.cache.put(symbol.to_string(), book.clone());

        Ok(())
    }

    pub async fn process_coin_incremental(
        &mut self,
        symbol: &str,
        book: &UnifiedOrderbook,
        block_height: u64,
    ) -> Result<(), RedisError> {
        self.block_height = block_height;

        if self.should_emit_for_coin(symbol) {
            let mut published = false;

            if let Some(prev_book) = self.cache.get(symbol) {
                if self.has_changes(&prev_book, book) {
                    self.redis_publisher.publish_orderbook(Exchange::Hyperliquid, symbol, book).await?;
                    published = true;
                }
            } else {
                self.redis_publisher.publish_orderbook(Exchange::Hyperliquid, symbol, book).await?;
                self.last_snapshot_block.insert(symbol.to_string(), block_height);
                self.update_emit_time();
                published = true;
            }

            if published {
                self.redis_publisher.publish_update_notification(Exchange::Hyperliquid, symbol).await?;
            }

            self.cache.put(symbol.to_string(), book.clone());
        }

        Ok(())
    }

    fn should_emit_for_coin(&self, symbol: &str) -> bool {
        if !self.streaming_mode {
            return false;
        }

        let elapsed_ms = self.last_emit_time.elapsed().as_millis() as u64;

        if elapsed_ms < self.snapshot_interval_ms {
            return false;
        }

        let last_block = self.last_snapshot_block.get(&symbol.to_string()).unwrap_or(0);
        let block_interval_passed = self.block_height - last_block >= self.snapshot_interval;

        block_interval_passed
    }

    pub fn should_emit_snapshot(&self, symbol: &str) -> bool {
        let last_block = self.last_snapshot_block.get(&symbol.to_string()).unwrap_or(0);
        let block_interval_passed = self.block_height - last_block >= self.snapshot_interval;

        if self.streaming_mode {
            let elapsed_ms = self.last_emit_time.elapsed().as_millis() as u64;
            let time_interval_passed = elapsed_ms >= self.snapshot_interval_ms;
            block_interval_passed || time_interval_passed
        } else {
            block_interval_passed
        }
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

    pub const fn streaming_mode(&self) -> bool {
        self.streaming_mode
    }

    pub fn set_streaming_mode(&mut self, mode: bool) {
        self.streaming_mode = mode;
    }

    pub const fn snapshot_interval_ms(&self) -> u64 {
        self.snapshot_interval_ms
    }

    pub fn set_snapshot_interval_ms(&mut self, interval_ms: u64) {
        self.snapshot_interval_ms = interval_ms;
    }

    pub fn update_emit_time(&mut self) {
        self.last_emit_time = Instant::now();
    }
}
