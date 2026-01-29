use log::{error, info};

use crate::{
    listener::{utils::compute_l2_snapshots, L2Snapshots, TimedSnapshots},
    orderbook::{
        multi_book::{OrderBooks, Snapshots},
        Coin, InnerOrder, Oid,
    },
    prelude::*,
    types::{
        inner::{InnerL4Order, InnerOrderDiff},
        node_data::{Batch, NodeDataOrderDiff, NodeDataOrderStatus},
    },
};
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Clone)]
pub struct OrderBookState {
    order_book: OrderBooks<InnerL4Order>,
    height: u64,
    time: u64,
    snapped: bool,
    ignore_spot: bool,
}

impl OrderBookState {
    pub fn new(ignore_spot: bool) -> Self {
        Self { order_book: OrderBooks::new(), height: 0, time: 0, snapped: false, ignore_spot }
    }

    pub fn from_snapshot(
        snapshot: Snapshots<InnerL4Order>,
        height: u64,
        time: u64,
        ignore_triggers: bool,
        ignore_spot: bool,
    ) -> Self {
        Self {
            ignore_spot,
            time,
            height,
            order_book: OrderBooks::from_snapshots(snapshot, ignore_triggers),
            snapped: false,
        }
    }

    pub const fn height(&self) -> u64 {
        self.height
    }

    // forcibly take snapshot - (time, height, snapshot)
    pub fn compute_snapshot(&self) -> TimedSnapshots {
        TimedSnapshots { time: self.time, height: self.height, snapshot: self.order_book.to_snapshots_par() }
    }

    // (time, snapshot)
    pub fn l2_snapshots(&mut self, prevent_future_snaps: bool) -> Option<(u64, L2Snapshots)> {
        if self.snapped {
            None
        } else {
            self.snapped = prevent_future_snaps || self.snapped;
            Some((self.time, compute_l2_snapshots(&self.order_book)))
        }
    }

    pub fn compute_universe(&self) -> HashSet<Coin> {
        self.order_book.as_ref().keys().cloned().collect()
    }

    pub fn apply_updates(
        &mut self,
        order_statuses: Batch<NodeDataOrderStatus>,
        order_diffs: Batch<NodeDataOrderDiff>,
    ) -> Result<()> {
        let height = order_statuses.block_number();
        let time = order_statuses.block_time();
        assert_eq!(order_statuses.block_number(), order_diffs.block_number());
        if self.height == 0 && height > 1 {
            // Startup situation - we missed some blocks before starting
            self.height = height - 1;
            info!("Starting from block {}", height);
        } else if height > self.height + 1 {
            // Blocks are out of order
            self.height = height;
            return Err(format!("Expecting block {}, got block {}", self.height + 1, height).into());
        } else if height <= self.height {
            info!("Already at block {}, ignoring block {}", self.height, height);
            // This is not an error in case we started caching long before a snapshot is fetched
            return Ok(());
        }
        let mut diffs = order_diffs.events().into_iter().collect::<VecDeque<_>>();
        let mut order_map = order_statuses
            .events()
            .into_iter()
            .filter_map(|order_status| {
                if order_status.is_inserted_into_book() {
                    Some((Oid::new(order_status.order.oid), order_status))
                } else {
                    None
                }
            })
            .collect::<HashMap<_, _>>();

        // Apply diffs
        while let Some(diff) = diffs.pop_front() {
            let oid = diff.oid();
            let coin = diff.coin();
            if coin.is_spot() && self.ignore_spot {
                continue;
            }
            let inner_diff = diff.diff().try_into()?;
            match inner_diff {
                InnerOrderDiff::New { sz } => {
                    if let Some(order) = order_map.remove(&oid) {
                        let time = order.time.and_utc().timestamp_millis();
                        let mut inner_order: InnerL4Order = order.try_into()?;
                        inner_order.modify_sz(sz);
                        // must replace time with time of entering book, which is the timestamp of the order status update
                        #[allow(clippy::unwrap_used)]
                        inner_order.convert_trigger(time.try_into().unwrap());
                        self.order_book.add_order(inner_order);
                    } else {
                        error!("Unable to find order opening status {:?}", diff);
                    }
                }
                InnerOrderDiff::Update { new_sz, .. } => {
                    if !self.order_book.modify_sz(oid, coin, new_sz) {
                        error!("Unable to find order on the book {:?}", diff);
                    }
                }
                InnerOrderDiff::Remove => {
                    if !self.order_book.cancel_order(oid, coin) {
                        error!("Unable to find order on the book {:?}", diff);
                    }
                }
            }
        }

        self.height += 1;
        self.time = time;
        self.snapped = false;
        info!("Block height now at {}", self.height);

        Ok(())
    }

    pub fn apply_single_update(
        &mut self,
        order_status: Batch<NodeDataOrderStatus>,
        order_diff: Batch<NodeDataOrderDiff>,
    ) -> Result<()> {
        let height = order_status.block_number();
        let time = order_status.block_time();

        assert_eq!(order_status.block_number(), order_diff.block_number());

        if height > self.height + 1 {
            return Err(format!("Expecting block {}, got block {} (too far ahead)", self.height + 1, height).into());
        } else if height == self.height {
            self.apply_updates_internal(order_status, order_diff, false)?;
        } else if height == self.height + 1 {
            self.apply_updates_internal(order_status, order_diff, true)?;
            self.height = height;
            self.time = time;
        } else {
            info!("Already at block {}, ignoring block {}", self.height, height);
        }

        Ok(())
    }

    fn apply_updates_internal(
        &mut self,
        order_statuses: Batch<NodeDataOrderStatus>,
        order_diffs: Batch<NodeDataOrderDiff>,
        increment_height: bool,
    ) -> Result<()> {
        let mut diffs = order_diffs.events().into_iter().collect::<VecDeque<_>>();
        let mut order_map = order_statuses
            .events()
            .into_iter()
            .filter_map(|order_status| {
                if order_status.is_inserted_into_book() {
                    Some((Oid::new(order_status.order.oid), order_status))
                } else {
                    None
                }
            })
            .collect::<HashMap<_, _>>();

        while let Some(diff) = diffs.pop_front() {
            let oid = diff.oid();
            let coin = diff.coin();
            if coin.is_spot() && self.ignore_spot {
                continue;
            }
            let inner_diff = diff.diff().try_into()?;
            match inner_diff {
                InnerOrderDiff::New { sz } => {
                    if let Some(order) = order_map.remove(&oid) {
                        let time = order.time.and_utc().timestamp_millis();
                        let mut inner_order: InnerL4Order = order.try_into()?;
                        inner_order.modify_sz(sz);
                        #[allow(clippy::unwrap_used)]
                        inner_order.convert_trigger(time.try_into().unwrap());
                        self.order_book.add_order(inner_order);
                    } else {
                        error!("Unable to find order opening status {:?}", diff);
                    }
                }
                InnerOrderDiff::Update { new_sz, .. } => {
                    if !self.order_book.modify_sz(oid, coin, new_sz) {
                        error!("Unable to find order on the book {:?}", diff);
                    }
                }
                InnerOrderDiff::Remove => {
                    if !self.order_book.cancel_order(oid, coin) {
                        error!("Unable to find order on the book {:?}", diff);
                    }
                }
            }
        }

        if increment_height {
            self.snapped = false;
            info!("Block height now at {}", self.height);
        }

        Ok(())
    }
}
