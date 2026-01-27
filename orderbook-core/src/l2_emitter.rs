use crate::{
    cache::OrderBookCache,
    orderbook::Coin,
    types::{amqp::L2Message, L2Book, Level},
};

pub struct L2Emitter {
    cache: OrderBookCache,
    sequence: u64,
    block_height: u64,
    snapshot_interval: u64,
    last_snapshot_block: u64,
}

impl L2Emitter {
    pub fn new(cache: OrderBookCache, snapshot_interval: u64) -> Self {
        Self { cache, sequence: 0, block_height: 0, snapshot_interval, last_snapshot_block: 0 }
    }

    pub fn new_with_default_interval(cache: OrderBookCache) -> Self {
        Self::new(cache, 100)
    }

    pub fn process_new_book(&mut self, coin: &Coin, book: &L2Book, block_height: u64) -> Vec<L2Message> {
        self.block_height = block_height;
        let mut messages = Vec::new();

        if self.should_emit_snapshot() {
            let snapshot = self.create_snapshot_message(coin, book);
            messages.push(snapshot);
            self.last_snapshot_block = block_height;
            self.cache.put(coin.clone(), book.clone());
        } else if let Some(prev_book) = self.cache.get(coin) {
            if let Some(delta) = self.compute_delta_if_changed(coin, &prev_book, book) {
                messages.push(delta);
            }
        } else {
            let snapshot = self.create_snapshot_message(coin, book);
            messages.push(snapshot);
            self.last_snapshot_block = block_height;
            self.cache.put(coin.clone(), book.clone());
        }

        messages
    }

    pub fn should_emit_snapshot(&self) -> bool {
        self.block_height - self.last_snapshot_block >= self.snapshot_interval
    }

    pub fn compute_delta(&mut self, coin: &Coin, old_book: &L2Book, new_book: &L2Book) -> L2Message {
        self.sequence += 1;

        let (bids_delta, asks_delta) = self.diff_levels(&old_book.levels, &new_book.levels);

        let from_sequence = self.sequence.saturating_sub(1);

        L2Message::Delta(crate::types::amqp::L2DeltaMessage {
            coin: coin.value(),
            timestamp: new_book.time,
            sequence: self.sequence,
            bids: bids_delta,
            asks: asks_delta,
            from_sequence,
            source: "orderbook".to_string(),
        })
    }

    pub fn compute_delta_if_changed(&mut self, coin: &Coin, old_book: &L2Book, new_book: &L2Book) -> Option<L2Message> {
        let (bids_delta, asks_delta) = self.diff_levels(&old_book.levels, &new_book.levels);

        if bids_delta.is_empty() && asks_delta.is_empty() {
            None
        } else {
            Some(self.compute_delta(coin, old_book, new_book))
        }
    }

    pub fn diff_levels(
        &self,
        old_levels: &[Vec<Level>; 2],
        new_levels: &[Vec<Level>; 2],
    ) -> (Vec<(String, String)>, Vec<(String, String)>) {
        let bids_delta = self.diff_level_list(&old_levels[0], &new_levels[0]);
        let asks_delta = self.diff_level_list(&old_levels[1], &new_levels[1]);

        (bids_delta, asks_delta)
    }

    fn diff_level_list(&self, old_levels: &[Level], new_levels: &[Level]) -> Vec<(String, String)> {
        let mut deltas = Vec::new();

        let max_len = old_levels.len().max(new_levels.len());
        for i in 0..max_len {
            let old_level = old_levels.get(i);
            let new_level = new_levels.get(i);

            match (old_level, new_level) {
                (Some(old), Some(new)) => {
                    if old.px != new.px || old.sz != new.sz || old.n != new.n {
                        deltas.push((new.px.clone(), new.sz.clone()));
                    }
                }
                (None, Some(new)) => {
                    deltas.push((new.px.clone(), new.sz.clone()));
                }
                (Some(old), None) => {
                    deltas.push((old.px.clone(), "0".to_string()));
                }
                (None, None) => {}
            }
        }

        deltas
    }

    fn create_snapshot_message(&mut self, coin: &Coin, book: &L2Book) -> L2Message {
        self.sequence += 1;

        let bids: Vec<(String, String)> = book.levels[0].iter().map(|l| (l.px.clone(), l.sz.clone())).collect();
        let asks: Vec<(String, String)> = book.levels[1].iter().map(|l| (l.px.clone(), l.sz.clone())).collect();

        L2Message::Snapshot(crate::types::amqp::L2SnapshotMessage {
            coin: coin.value(),
            timestamp: book.time,
            sequence: self.sequence,
            bids,
            asks,
            source: "orderbook".to_string(),
        })
    }

    pub const fn sequence(&self) -> u64 {
        self.sequence
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

impl Default for L2Emitter {
    fn default() -> Self {
        Self::new_with_default_interval(OrderBookCache::default())
    }
}
