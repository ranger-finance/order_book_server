use crate::{
    cache::OrderBookCache,
    listener::utils::{BatchQueue, EventBatch},
    orderbook::multi_book::Snapshots,
    orderbook::Coin,
    prelude::*,
    state::OrderBookState,
    types::inner::InnerL4Order,
    types::{
        node_data::{Batch, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
        L2Book, Level,
    },
};
use log::{info, warn};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;

pub struct L2OrderBookBuilder {
    state: OrderBookState,
    fill_cache: BatchQueue<NodeDataFill>,
    status_cache: BatchQueue<NodeDataOrderStatus>,
    diff_cache: BatchQueue<NodeDataOrderDiff>,
    ignore_spot: bool,
    cache: OrderBookCache,
}

impl L2OrderBookBuilder {
    pub fn new() -> Self {
        Self {
            state: OrderBookState::new(false),
            fill_cache: BatchQueue::new(),
            status_cache: BatchQueue::new(),
            diff_cache: BatchQueue::new(),
            ignore_spot: false,
            cache: OrderBookCache::new(NonZeroUsize::new(1000).unwrap()),
        }
    }

    pub fn with_ignore_spot(ignore_spot: bool) -> Self {
        Self {
            state: OrderBookState::new(ignore_spot),
            fill_cache: BatchQueue::new(),
            status_cache: BatchQueue::new(),
            diff_cache: BatchQueue::new(),
            ignore_spot,
            cache: OrderBookCache::new(NonZeroUsize::new(1000).unwrap()),
        }
    }

    pub fn consume_batch(&mut self, batch: EventBatch) -> Result<()> {
        match batch {
            EventBatch::Orders(batch) => {
                self.status_cache.push(batch);
            }
            EventBatch::BookDiffs(batch) => {
                self.diff_cache.push(batch);
            }
            EventBatch::Fills(batch) => {
                self.fill_cache.push(batch);
            }
        }
        self.try_apply_updates()?;
        Ok(())
    }

    pub fn init_from_snapshot(&mut self, snapshot: Snapshots<InnerL4Order>, height: u64, time: u64) {
        info!("Initializing order book from snapshot at height {}", height);
        self.state = OrderBookState::from_snapshot(snapshot, height, time, true, self.ignore_spot);
        let mut retry = false;
        while let Some((order_statuses, order_diffs)) = self.find_matching_blocks() {
            if self.state.apply_updates(order_statuses, order_diffs).is_err() {
                warn!(
                    "Failed to apply cached updates to initial book (likely missing older updates). Will wait for next synchronization."
                );
                retry = true;
                break;
            }
        }
        if !retry {
            info!("Order book initialized and ready for streaming");
        }
    }

    fn try_apply_updates(&mut self) -> Result<()> {
        if let Some((status_batch, diff_batch)) = self.find_matching_blocks() {
            self.state.apply_updates(status_batch, diff_batch)?;
        }
        Ok(())
    }

    fn find_matching_blocks(&mut self) -> Option<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)> {
        while let Some(t) = self.diff_cache.front() {
            if let Some(s) = self.status_cache.front() {
                match t.block_number().cmp(&s.block_number()) {
                    Ordering::Less => {
                        self.diff_cache.pop_front();
                    }
                    Ordering::Equal => {
                        return self.status_cache.pop_front().and_then(|t| self.diff_cache.pop_front().map(|s| (t, s)));
                    }
                    Ordering::Greater => {
                        self.status_cache.pop_front();
                    }
                }
            } else {
                break;
            }
        }
        None
    }

    pub fn try_build_snapshot(&mut self) -> Option<HashMap<Coin, L2Book>> {
        if let Some((time, l2_snapshots)) = self.state.l2_snapshots(false) {
            let mut result = HashMap::new();
            for (coin, snapshots) in l2_snapshots.as_ref() {
                if coin.is_spot() && self.ignore_spot {
                    continue;
                }
                if let Some((_, inner_snapshot)) = snapshots.iter().next() {
                    let serialized_levels =
                        inner_snapshot.as_ref().clone().map(|levels| levels.into_iter().map(Level::from).collect());
                    let l2_book = L2Book::from_l2_snapshot(coin.value(), serialized_levels, time);
                    result.insert(coin.clone(), l2_book);
                }
            }
            if !result.is_empty() {
                for (coin, book) in &result {
                    self.cache.put(coin.clone(), book.clone());
                }
                return Some(result);
            }
        }
        None
    }

    pub fn is_ready(&self) -> bool {
        true
    }

    pub fn height(&self) -> Option<u64> {
        Some(self.state.height())
    }

    pub fn universe(&self) -> HashSet<Coin> {
        self.state.compute_universe()
    }
}

impl Default for L2OrderBookBuilder {
    fn default() -> Self {
        Self::new()
    }
}
