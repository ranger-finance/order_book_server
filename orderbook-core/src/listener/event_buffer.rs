use crate::orderbook::{Coin, Oid};
use crate::types::node_data::{NodeDataOrderDiff, NodeDataOrderStatus};
use chrono::NaiveDateTime;
use std::collections::HashMap;
use std::time::Instant;

#[derive(Clone, Debug)]
pub struct OrderStatusWrapper {
    pub status: NodeDataOrderStatus,
    pub block_number: u64,
    pub block_time: NaiveDateTime,
    pub local_time: NaiveDateTime,
}

#[derive(Clone, Debug)]
pub struct OrderDiffWrapper {
    pub diff: NodeDataOrderDiff,
    pub block_number: u64,
    pub block_time: NaiveDateTime,
    pub local_time: NaiveDateTime,
}

pub struct EventBuffer {
    order_status_buffer: HashMap<(u64, Oid, Coin), OrderStatusWrapper>,
    order_diff_buffer: HashMap<(u64, Oid, Coin), OrderDiffWrapper>,
    buffer_window_ms: u64,
    last_process_time: Instant,
}

impl EventBuffer {
    pub fn new(buffer_window_ms: u64) -> Self {
        Self {
            order_status_buffer: HashMap::new(),
            order_diff_buffer: HashMap::new(),
            buffer_window_ms,
            last_process_time: Instant::now(),
        }
    }

    pub fn add_order_status(&mut self, block_number: u64, block_time: NaiveDateTime, status: NodeDataOrderStatus) {
        let key = (block_number, Oid::new(status.order.oid), Coin::new(&status.order.coin));
        let wrapper = OrderStatusWrapper { status, block_number, block_time, local_time: block_time.clone() };
        self.order_status_buffer.insert(key, wrapper);
    }

    pub fn add_order_diff(&mut self, block_number: u64, block_time: NaiveDateTime, diff: NodeDataOrderDiff) {
        let key = (block_number, diff.oid(), diff.coin());
        let wrapper = OrderDiffWrapper { diff, block_number, block_time, local_time: block_time.clone() };
        self.order_diff_buffer.insert(key, wrapper);
    }

    pub fn try_match_events(&mut self) -> Vec<(OrderStatusWrapper, OrderDiffWrapper)> {
        let mut matched = Vec::new();
        let mut to_remove = Vec::new();

        for (key, status) in &self.order_status_buffer {
            if let Some(diff) = self.order_diff_buffer.get(key) {
                matched.push((status.clone(), diff.clone()));
                to_remove.push(key.clone());
            }
        }

        for key in to_remove {
            self.order_status_buffer.remove(&key);
            self.order_diff_buffer.remove(&key);
        }

        matched
    }

    pub fn flush_old_events(&mut self) {
        let now = Instant::now();
        if now.duration_since(self.last_process_time).as_millis() > self.buffer_window_ms as u128 {
            self.order_status_buffer.clear();
            self.order_diff_buffer.clear();
            self.last_process_time = now;
        }
    }

    pub fn status_count(&self) -> usize {
        self.order_status_buffer.len()
    }

    pub fn diff_count(&self) -> usize {
        self.order_diff_buffer.len()
    }
}
