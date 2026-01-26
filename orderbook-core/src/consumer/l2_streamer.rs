use crate::{orderbook::Coin, types::L2Book, types::Level};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct L2Delta {
    pub coin: String,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
    pub timestamp: u64,
    pub sequence: u64,
    pub is_delta: bool,
}

pub struct L2OrderBookStreamer {
    last_snapshot: Option<HashMap<Coin, L2Book>>,
    sequence: u64,
}

impl L2OrderBookStreamer {
    pub fn new() -> Self {
        Self { last_snapshot: None, sequence: 0 }
    }

    pub fn compute_delta(&mut self, new_snapshot: &HashMap<Coin, L2Book>) -> Vec<L2Delta> {
        let mut deltas = Vec::new();
        self.sequence += 1;

        for (coin, new_book) in new_snapshot {
            let coin_str = coin.clone();
            let timestamp = new_book.time;
            let sequence = self.sequence;

            if let Some(old_book) = self.last_snapshot.as_ref().and_then(|s| s.get(&coin_str)) {
                let (bids_delta, asks_delta) = self.compute_level_deltas(old_book, new_book);

                if !bids_delta.is_empty() || !asks_delta.is_empty() {
                    deltas.push(L2Delta {
                        coin: coin.value().clone(),
                        bids: bids_delta,
                        asks: asks_delta,
                        timestamp,
                        sequence,
                        is_delta: true,
                    });
                }
            } else {
                let bids = self.copy_levels(&new_book.levels[0]);
                let asks = self.copy_levels(&new_book.levels[1]);
                deltas.push(L2Delta { coin: coin.value().clone(), bids, asks, timestamp, sequence, is_delta: false });
            }
        }

        deltas
    }

    fn compute_level_deltas(&self, old_book: &L2Book, new_book: &L2Book) -> (Vec<Level>, Vec<Level>) {
        let old_bids = &old_book.levels[0];
        let old_asks = &old_book.levels[1];
        let new_bids = &new_book.levels[0];
        let new_asks = &new_book.levels[1];

        let bids_delta = self.find_changed_levels(old_bids, new_bids);
        let asks_delta = self.find_changed_levels(old_asks, new_asks);

        (bids_delta, asks_delta)
    }

    fn find_changed_levels(&self, old_levels: &[Level], new_levels: &[Level]) -> Vec<Level> {
        let mut changed = Vec::new();

        let max_len = old_levels.len().max(new_levels.len());
        for i in 0..max_len {
            let old_level = old_levels.get(i);
            let new_level = new_levels.get(i);

            match (old_level, new_level) {
                (Some(old), Some(new)) => {
                    if old.px != new.px || old.sz != new.sz || old.n != new.n {
                        changed.push(new.clone());
                    }
                }
                (None, Some(new)) => {
                    changed.push(new.clone());
                }
                (Some(_), None) => {
                    changed.push(Level::new("0".to_string(), "0".to_string(), 0));
                }
                (None, None) => {}
            }
        }

        changed
    }

    fn copy_levels(&self, levels: &[Level]) -> Vec<Level> {
        levels.to_vec()
    }

    pub fn update_snapshot(&mut self, snapshot: HashMap<Coin, L2Book>) {
        self.last_snapshot = Some(snapshot);
    }
}

impl Default for L2OrderBookStreamer {
    fn default() -> Self {
        Self::new()
    }
}
