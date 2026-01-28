use crate::{
    cache::{OrderBookCache, lru_cache::CoinLruCache},
    orderbook::Coin,
    redis::RedisPublisher,
    types::{L2Book, Level},
};
use bb8_redis::redis::RedisError;
use std::sync::Arc;

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
}

impl L2Emitter {
    pub fn new(cache: OrderBookCache, redis_publisher: Arc<RedisPublisher>, snapshot_interval: u64) -> Self {
        Self {
            cache,
            redis_publisher,
            last_snapshot_block: CoinLruCache::<Coin, u64>::new(1000),
            block_height: 0,
            snapshot_interval,
        }
    }

    pub fn new_with_default_interval(cache: OrderBookCache, redis_publisher: Arc<RedisPublisher>) -> Self {
        Self::new(cache, redis_publisher, 100)
    }

    pub async fn process_coin(&mut self, coin: &Coin, book: &L2Book, block_height: u64) -> Result<(), RedisError> {
        self.block_height = block_height;

        if self.should_emit_snapshot(coin) {
            self.redis_publisher.publish_l2_book(coin, book).await?;
            self.last_snapshot_block.insert(coin.clone(), block_height);
        } else if let Some(prev_book) = self.cache.get(coin) {
            if self.has_changes(&prev_book, book) {
                // let _delta = self.compute_delta(coin, &prev_book, book);
                self.redis_publisher.publish_l2_book(coin, book).await?;
            }
        } else {
            self.redis_publisher.publish_l2_book(coin, book).await?;
            self.last_snapshot_block.insert(coin.clone(), block_height);
        }

        self.cache.put(coin.clone(), book.clone());

        Ok(())
    }

    pub fn should_emit_snapshot(&self, coin: &Coin) -> bool {
        let last_block = self.last_snapshot_block.get(coin).unwrap_or(0);
        self.block_height - last_block >= self.snapshot_interval
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
        old_book.block != new_book.block || old_book.time != new_book.time
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
}
