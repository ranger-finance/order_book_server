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

    pub fn flush_old_events(&mut self, current_height: Option<u64>) {
        let now = Instant::now();
        let time_window_expired =
            now.duration_since(self.last_process_time).as_millis() > self.buffer_window_ms as u128;

        if time_window_expired {
            let status_before = self.order_status_buffer.len();
            let diff_before = self.order_diff_buffer.len();
            let mut status_to_remove = Vec::new();
            let mut diff_to_remove = Vec::new();

            for (key, status) in &self.order_status_buffer {
                let should_keep = if let Some(h) = current_height { status.block_number >= h } else { true };

                if !should_keep {
                    status_to_remove.push(key.clone());
                }
            }

            for (key, diff) in &self.order_diff_buffer {
                let should_keep = if let Some(h) = current_height { diff.block_number >= h } else { true };

                if !should_keep {
                    diff_to_remove.push(key.clone());
                }
            }

            let status_removed_count = status_to_remove.len();
            let diff_removed_count = diff_to_remove.len();

            for key in status_to_remove {
                self.order_status_buffer.remove(&key);
            }

            for key in diff_to_remove {
                self.order_diff_buffer.remove(&key);
            }

            let status_after = self.order_status_buffer.len();
            let diff_after = self.order_diff_buffer.len();

            if status_removed_count > 0 || diff_removed_count > 0 {
                log::info!("Pruned event buffer: status {} -> {} (removed {}), diff {} -> {} (removed {}), current_height: {:?}",
                          status_before, status_after, status_removed_count,
                          diff_before, diff_after, diff_removed_count,
                          current_height);
            }

            self.last_process_time = now;
        }
    }

    pub fn status_count(&self) -> usize {
        self.order_status_buffer.len()
    }

    pub fn diff_count(&self) -> usize {
        self.order_diff_buffer.len()
    }

    pub fn get_events_in_range(
        &self,
        start_height: u64,
        end_height: u64,
    ) -> Vec<(OrderStatusWrapper, OrderDiffWrapper)> {
        let mut matched = Vec::new();

        log::debug!(
            "get_events_in_range: looking for events in [{}, {}], buffer has {} statuses, {} diffs",
            start_height,
            end_height,
            self.order_status_buffer.len(),
            self.order_diff_buffer.len()
        );

        for (key, status) in &self.order_status_buffer {
            let block_number = status.block_number;
            if block_number >= start_height && block_number <= end_height {
                if let Some(diff) = self.order_diff_buffer.get(key) {
                    matched.push((status.clone(), diff.clone()));
                }
            }
        }

        log::debug!("get_events_in_range: returning {} matched events", matched.len());
        matched
    }
}
