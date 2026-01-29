use crate::{
    cache::{OrderBookCache, lru_cache::CoinLruCache},
    orderbook::Coin,
    redis::RedisPublisher,
    types::L2Book,
};
use bb8_redis::redis::RedisError;
use std::sync::Arc;
use std::time::Instant;

// #[derive(Debug, Clone, Serialize, Deserialize)]
// #[serde(rename_all = "camelCase")]
// pub struct L2DeltaMessage {
//     pub coin: String,
//     pub timestamp: u64,
//     pub bids: Vec<(String, String)>,
//     pub asks: Vec<(String, String)>,
//     pub source: String,
// }

pub struct L2Emitter {
    cache: OrderBookCache,
    redis_publisher: Arc<RedisPublisher>,
    last_snapshot_block: CoinLruCache<Coin, u64>,
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
        let snapshot_interval_ms = snapshot_interval_ms.unwrap_or_else(|| {
            if streaming_mode { 100 } else { 1000 }
        });

        Self {
            cache,
            redis_publisher,
            last_snapshot_block: CoinLruCache::<Coin, u64>::new(1000),
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

    pub fn new_simple(
        cache: OrderBookCache,
        redis_publisher: Arc<RedisPublisher>,
        snapshot_interval: u64,
    ) -> Self {
        Self::new(cache, redis_publisher, snapshot_interval, false, None)
    }

    pub async fn process_coin(&mut self, coin: &Coin, book: &L2Book, block_height: u64) -> Result<(), RedisError> {
        self.block_height = block_height;

        let mut published = false;
        if self.should_emit_snapshot(coin) {
            self.redis_publisher.publish_l2_book(coin, book).await?;
            self.last_snapshot_block.insert(coin.clone(), block_height);
            self.update_emit_time();
            published = true;
        } else if let Some(prev_book) = self.cache.get(coin) {
            if self.has_changes(&prev_book, book) {
                self.redis_publisher.publish_l2_book(coin, book).await?;
                published = true;
            }
        } else {
            self.redis_publisher.publish_l2_book(coin, book).await?;
            self.last_snapshot_block.insert(coin.clone(), block_height);
            self.update_emit_time();
            published = true;
        }

        if published {
            self.redis_publisher.publish_update_notification(&coin.value()).await?;
        }

        self.cache.put(coin.clone(), book.clone());

        Ok(())
    }

    pub async fn process_coin_incremental(&mut self, coin: &Coin, book: &L2Book, block_height: u64) -> Result<(), RedisError> {
        self.block_height = block_height;

        if self.should_emit_for_coin(coin) {
            let mut published = false;

            if let Some(prev_book) = self.cache.get(coin) {
                if self.has_changes(&prev_book, book) {
                    self.redis_publisher.publish_l2_book(coin, book).await?;
                    published = true;
                }
            } else {
                self.redis_publisher.publish_l2_book(coin, book).await?;
                self.last_snapshot_block.insert(coin.clone(), block_height);
                self.update_emit_time();
                published = true;
            }

            if published {
                self.redis_publisher.publish_update_notification(&coin.value()).await?;
            }

            self.cache.put(coin.clone(), book.clone());
        }

        Ok(())
    }

    fn should_emit_for_coin(&self, coin: &Coin) -> bool {
        if !self.streaming_mode {
            return false;
        }

        let elapsed_ms = self.last_emit_time.elapsed().as_millis() as u64;

        if elapsed_ms < self.snapshot_interval_ms {
            return false;
        }

        let last_block = self.last_snapshot_block.get(coin).unwrap_or(0);
        let block_interval_passed = self.block_height - last_block >= self.snapshot_interval;

        block_interval_passed
    }

    pub fn should_emit_snapshot(&self, coin: &Coin) -> bool {
        let last_block = self.last_snapshot_block.get(coin).unwrap_or(0);
        let block_interval_passed = self.block_height - last_block >= self.snapshot_interval;

        if self.streaming_mode {
            let elapsed_ms = self.last_emit_time.elapsed().as_millis() as u64;
            let time_interval_passed = elapsed_ms >= self.snapshot_interval_ms;
            block_interval_passed || time_interval_passed
        } else {
            block_interval_passed
        }
    }

    // pub fn compute_delta(&self, coin: &Coin, old_book: &L2Book, new_book: &L2Book) -> L2DeltaMessage {
    //     let (bids_delta, asks_delta) = self.diff_levels(&old_book.levels, &new_book.levels);
    //
    //     L2DeltaMessage {
    //         coin: coin.value(),
    //         timestamp: new_book.time,
    //         bids: bids_delta,
    //         asks: asks_delta,
    //         source: "orderbook".to_string(),
    //     }
    // }

    pub fn has_changes(&self, old_book: &L2Book, new_book: &L2Book) -> bool {
        old_book.time != new_book.time
    }

    // pub fn diff_levels(
    //     &self,
    //     old_levels: &[Vec<Level>; 2],
    //     new_levels: &[Vec<Level>; 2],
    // ) -> (Vec<(String, String)>, Vec<(String, String)>) {
    //     let bids_delta = self.diff_level_list(&old_levels[0], &new_levels[0]);
    //     let asks_delta = self.diff_level_list(&old_levels[1], &new_levels[1]);
    //
    //     (bids_delta, asks_delta)
    // }

    // fn diff_level_list(&self, old_levels: &[Level], new_levels: &[Level]) -> Vec<(String, String)> {
    //     let mut deltas = Vec::new();
    //
    //     let max_len = old_levels.len().max(new_levels.len());
    //     for i in 0..max_len {
    //         let old_level = old_levels.get(i);
    //         let new_level = new_levels.get(i);
    //
    //         match (old_level, new_level) {
    //             (Some(old), Some(new)) => {
    //                 if old.px != new.px || old.sz != new.sz || old.n != new.n {
    //                     deltas.push((new.px.clone(), new.sz.clone()));
    //                 }
    //             }
    //             (None, Some(new)) => {
    //                 deltas.push((new.px.clone(), new.sz.clone()));
    //             }
    //             (Some(old), None) => {
    //                 deltas.push((old.px.clone(), "0".to_string()));
    //             }
    //             (None, None) => {}
    //         }
    //     }
    //
    //     deltas
    // }

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
