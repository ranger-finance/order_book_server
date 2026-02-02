use crate::{
    listener::{L2SnapshotParams, L2Snapshots},
    orderbook::{
        Snapshot,
        multi_book::{OrderBooks, Snapshots},
        types::InnerOrder,
    },
    prelude::*,
    types::{
        inner::InnerLevel,
        node_data::{Batch, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
    },
};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use reqwest::Client;
use serde_json::json;
use std::collections::VecDeque;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

pub async fn process_rmp_file(dir: &Path) -> Result<PathBuf> {
    let output_path = dir.join("out.json");
    let payload = json!({
        "type": "fileSnapshot",
        "request": {
            "type": "l4Snapshots",
            "includeUsers": true,
            "includeTriggerOrders": false
        },
        "outPath": output_path,
        "includeHeightInOutput": true
    });

    let client = Client::new();
    client
        .post("http://localhost:3001/info")
        .header("Content-Type", "application/json")
        .json(&payload)
        .send()
        .await?
        .error_for_status()?;

    Ok(output_path)
}

pub fn validate_snapshot_consistency<O: Clone + PartialEq + Debug>(
    snapshot: &Snapshots<O>,
    expected: Snapshots<O>,
    ignore_spot: bool,
) -> Result<()> {
    let mut snapshot_map: HashMap<_, _> =
        expected.value().into_iter().filter(|(c, _)| !c.is_spot() || !ignore_spot).collect();

    for (coin, book) in snapshot.as_ref() {
        if ignore_spot && coin.is_spot() {
            continue;
        }
        let book1 = book.as_ref();
        if let Some(book2) = snapshot_map.remove(coin) {
            for (orders1, orders2) in book1.as_ref().iter().zip(book2.as_ref()) {
                for (order1, order2) in orders1.iter().zip(orders2.iter()) {
                    if *order1 != *order2 {
                        return Err(
                            format!("Orders do not match, expected: {:?} received: {:?}", *order2, *order1).into()
                        );
                    }
                }
            }
        } else if !book1[0].is_empty() || !book1[1].is_empty() {
            return Err(format!("Missing {} book", coin.value()).into());
        }
    }
    if !snapshot_map.is_empty() {
        return Err("Extra orderbooks detected".to_string().into());
    }
    Ok(())
}

impl L2SnapshotParams {
    pub const fn new(n_sig_figs: Option<u32>, mantissa: Option<u64>) -> Self {
        Self { n_sig_figs, mantissa }
    }
}

pub fn compute_l2_snapshots<O: InnerOrder + Send + Sync>(order_books: &OrderBooks<O>) -> L2Snapshots {
    compute_l2_snapshots_with_max_levels(order_books, 50)
}

pub fn compute_l2_snapshots_with_max_levels<O: InnerOrder + Send + Sync>(
    order_books: &OrderBooks<O>,
    max_levels: usize,
) -> L2Snapshots {
    L2Snapshots(
        order_books
            .as_ref()
            .par_iter()
            .map(|(coin, order_book)| {
                let mut entries = Vec::new();
                let snapshot = order_book.to_l2_snapshot(Some(max_levels), None, None);
                entries.push((L2SnapshotParams { n_sig_figs: None, mantissa: None }, snapshot));
                let mut add_new_snapshot = |n_sig_figs: Option<u32>, mantissa: Option<u64>, idx: usize| {
                    if let Some((_, last_snapshot)) = &entries.get(entries.len() - idx) {
                        let snapshot = last_snapshot.to_l2_snapshot(Some(max_levels), n_sig_figs, mantissa);
                        entries.push((L2SnapshotParams { n_sig_figs, mantissa }, snapshot));
                    }
                };
                for n_sig_figs in (2..=5).rev() {
                    if n_sig_figs == 5 {
                        for mantissa in [None, Some(2), Some(5)] {
                            if mantissa == Some(5) {
                                add_new_snapshot(Some(n_sig_figs), mantissa, 2);
                            } else {
                                add_new_snapshot(Some(n_sig_figs), mantissa, 1);
                            }
                        }
                    } else {
                        add_new_snapshot(Some(n_sig_figs), None, 1);
                    }
                }
                (coin.clone(), entries.into_iter().collect::<HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>())
            })
            .collect(),
    )
}

pub enum EventBatch {
    Orders(Batch<NodeDataOrderStatus>),
    BookDiffs(Batch<NodeDataOrderDiff>),
    Fills(Batch<NodeDataFill>),
}

pub struct BatchQueue<T> {
    deque: VecDeque<Batch<T>>,
    last_ts: Option<u64>,
}

impl<T> BatchQueue<T> {
    pub const fn new() -> Self {
        Self { deque: VecDeque::new(), last_ts: None }
    }

    pub fn push(&mut self, block: Batch<T>) -> bool {
        if let Some(last_ts) = self.last_ts {
            if last_ts >= block.block_number() {
                return false;
            }
        }
        self.last_ts = Some(block.block_number());
        self.deque.push_back(block);
        true
    }

    pub fn pop_front(&mut self) -> Option<Batch<T>> {
        self.deque.pop_front()
    }

    pub fn front(&self) -> Option<&Batch<T>> {
        self.deque.front()
    }
}

impl<T> Default for BatchQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orderbook::{Coin, Side};
    use crate::types::inner::InnerL4Order;
    use alloy::primitives::Address;

    fn create_test_orderbook() -> OrderBooks<InnerL4Order> {
        let mut order_books = OrderBooks::new();
        let coin = Coin::new("TEST");

        for i in 0..10 {
            let bid = InnerL4Order {
                user: Address::new([0; 20]),
                coin: coin.clone(),
                side: Side::Bid,
                limit_px: crate::orderbook::Px::new(1000 + i * 10),
                sz: crate::orderbook::Sz::new(100),
                oid: i,
                timestamp: 0,
                trigger_condition: String::new(),
                is_trigger: false,
                trigger_px: String::new(),
                is_position_tpsl: false,
                reduce_only: false,
                order_type: String::new(),
                tif: None,
                cloid: None,
            };
            order_books.add_order(bid);
        }

        for i in 0..10 {
            let ask = InnerL4Order {
                user: Address::new([0; 20]),
                coin: coin.clone(),
                side: Side::Ask,
                limit_px: crate::orderbook::Px::new(2000 + i * 10),
                sz: crate::orderbook::Sz::new(100),
                oid: 10 + i,
                timestamp: 0,
                trigger_condition: String::new(),
                is_trigger: false,
                trigger_px: String::new(),
                is_position_tpsl: false,
                reduce_only: false,
                order_type: String::new(),
                tif: None,
                cloid: None,
            };
            order_books.add_order(ask);
        }

        order_books
    }

    #[test]
    fn test_compute_l2_snapshots_default() {
        let order_books = create_test_orderbook();
        let l2_snapshots = compute_l2_snapshots(&order_books);

        let coin = Coin::new("TEST");
        assert!(l2_snapshots.as_ref().contains_key(&coin));

        let params_map = l2_snapshots.as_ref().get(&coin).unwrap();
        let raw_params = L2SnapshotParams { n_sig_figs: None, mantissa: None };
        let snapshot = params_map.get(&raw_params).unwrap();

        assert_eq!(snapshot.0[0].len(), 10); // 10 bids
        assert_eq!(snapshot.0[1].len(), 10); // 10 asks
    }

    #[test]
    fn test_compute_l2_snapshots_with_max_levels() {
        let order_books = create_test_orderbook();
        let max_levels = 3;
        let l2_snapshots = compute_l2_snapshots_with_max_levels(&order_books, max_levels);

        let coin = Coin::new("TEST");
        assert!(l2_snapshots.as_ref().contains_key(&coin));

        let params_map = l2_snapshots.as_ref().get(&coin).unwrap();
        let raw_params = L2SnapshotParams { n_sig_figs: None, mantissa: None };
        let snapshot = params_map.get(&raw_params).unwrap();

        assert_eq!(snapshot.0[0].len(), max_levels);
        assert_eq!(snapshot.0[1].len(), max_levels);
    }

    #[test]
    fn test_compute_l2_snapshots_default_wrapper() {
        let order_books = create_test_orderbook();
        let l2_snapshots_default = compute_l2_snapshots(&order_books);
        let l2_snapshots_50 = compute_l2_snapshots_with_max_levels(&order_books, 50);

        let coin = Coin::new("TEST");
        let params_map_default = l2_snapshots_default.as_ref().get(&coin).unwrap();
        let params_map_50 = l2_snapshots_50.as_ref().get(&coin).unwrap();
        let raw_params = L2SnapshotParams { n_sig_figs: None, mantissa: None };
        let snapshot_default = params_map_default.get(&raw_params).unwrap();
        let snapshot_50 = params_map_50.get(&raw_params).unwrap();

        assert_eq!(snapshot_default.0[0].len(), snapshot_50.0[0].len());
        assert_eq!(snapshot_default.0[1].len(), snapshot_50.0[1].len());
    }
}
