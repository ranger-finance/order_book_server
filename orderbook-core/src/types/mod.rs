use alloy::primitives::Address;
use orderbook_normaliser::models::{Exchange, PriceLevel, UnifiedOrderbook};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    orderbook::types::Side,
    types::node_data::{NodeDataOrderDiff, NodeDataOrderStatus},
};

pub mod inner;
pub mod node_data;

#[derive(Debug, Serialize, Deserialize)]
pub struct Trade {
    pub coin: String,
    side: Side,
    px: String,
    sz: String,
    hash: String,
    time: u64,
    tid: u64,
    users: [Address; 2],
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Level {
    pub px: String,
    pub sz: String,
    pub n: usize,
}

impl Level {
    pub const fn new(px: String, sz: String, n: usize) -> Self {
        Self { px, sz, n }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L2Book {
    coin: String,
    pub time: u64,
    pub levels: [Vec<Level>; 2],
}

#[derive(Debug, Serialize, Deserialize)]
pub enum L4Book {
    Snapshot { coin: String, time: u64, height: u64, levels: [Vec<L4Order>; 2] },
    Updates(L4BookUpdates),
}

impl L2Book {
    pub const fn from_l2_snapshot(coin: String, snapshot: [Vec<Level>; 2], time: u64) -> Self {
        Self { coin, time, levels: snapshot }
    }

    pub fn to_unified(&self, symbol: &str) -> Result<UnifiedOrderbook, Box<dyn std::error::Error + Send + Sync>> {
        let bids: Vec<PriceLevel> = self.levels[0]
            .iter()
            .map::<Result<PriceLevel, rust_decimal::Error>, _>(|level: &Level| {
                let price = level.px.parse::<Decimal>()?;
                let size = level.sz.parse::<Decimal>()?;
                Ok(PriceLevel::new(price, size))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let asks: Vec<PriceLevel> = self.levels[1]
            .iter()
            .map::<Result<PriceLevel, rust_decimal::Error>, _>(|level: &Level| {
                let price = level.px.parse::<Decimal>()?;
                let size = level.sz.parse::<Decimal>()?;
                Ok(PriceLevel::new(price, size))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let timestamp_ms = self.time as i64;

        Ok(UnifiedOrderbook::from_levels(Exchange::Hyperliquid, symbol.to_string(), bids, asks, timestamp_ms))
    }
}

impl Trade {}

#[derive(Debug, Serialize, Deserialize)]
pub struct L4BookUpdates {
    pub time: u64,
    pub height: u64,
    pub order_statuses: Vec<NodeDataOrderStatus>,
    pub book_diffs: Vec<NodeDataOrderDiff>,
}

impl L4BookUpdates {
    pub const fn new(time: u64, height: u64) -> Self {
        Self { time, height, order_statuses: Vec::new(), book_diffs: Vec::new() }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct L4Order {
    // when serializing, this field is found outside of this struct
    // when deserializing, we move it into this struct
    pub user: Option<Address>,
    pub coin: String,
    pub side: Side,
    pub limit_px: String,
    pub sz: String,
    pub oid: u64,
    pub timestamp: u64,
    pub trigger_condition: String,
    pub is_trigger: bool,
    pub trigger_px: String,
    pub is_position_tpsl: bool,
    pub reduce_only: bool,
    pub order_type: String,
    pub tif: Option<String>,
    pub cloid: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum OrderDiff {
    #[serde(rename_all = "camelCase")]
    New {
        sz: String,
    },
    #[serde(rename_all = "camelCase")]
    Update {
        orig_sz: String,
        new_sz: String,
    },
    Remove,
}
