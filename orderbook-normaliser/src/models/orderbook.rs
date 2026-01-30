use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;

/// Represents a single price level in the orderbook (used for serialization)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PriceLevel {
    pub price: Decimal,
    pub size: Decimal,
}

impl PriceLevel {
    pub fn new(price: Decimal, size: Decimal) -> Self {
        Self { price, size }
    }
}

/// The source exchange for the orderbook data
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Exchange {
    Hyperliquid,
    Drift,
}

/// Serialize BTreeMap as Vec<PriceLevel> in ascending order (for asks)
fn serialize_asks<S>(map: &BTreeMap<Decimal, Decimal>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let levels: Vec<PriceLevel> = map.iter().map(|(price, size)| PriceLevel::new(*price, *size)).collect();
    levels.serialize(serializer)
}

/// Serialize BTreeMap as Vec<PriceLevel> in descending order (for bids - best bid first)
fn serialize_bids<S>(map: &BTreeMap<Decimal, Decimal>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let levels: Vec<PriceLevel> = map
        .iter()
        .rev() // Reverse to get descending order (highest price first)
        .map(|(price, size)| PriceLevel::new(*price, *size))
        .collect();
    levels.serialize(serializer)
}

/// Deserialize Vec<PriceLevel> into BTreeMap
fn deserialize_levels<'de, D>(deserializer: D) -> Result<BTreeMap<Decimal, Decimal>, D::Error>
where
    D: Deserializer<'de>,
{
    let levels: Vec<PriceLevel> = Vec::deserialize(deserializer)?;
    Ok(levels.into_iter().map(|l| (l.price, l.size)).collect())
}

/// Unified orderbook structure that normalizes data from all exchanges
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnifiedOrderbook {
    /// Source exchange
    pub exchange: Exchange,
    /// Trading pair/market symbol (e.g., "BTC-PERP", "SOL-PERP")
    pub symbol: String,
    /// Bid levels: price -> size (BTreeMap keeps prices sorted ascending, serialized descending)
    #[serde(serialize_with = "serialize_bids", deserialize_with = "deserialize_levels")]
    pub bids: BTreeMap<Decimal, Decimal>,
    /// Ask levels: price -> size (BTreeMap keeps prices sorted ascending)
    #[serde(serialize_with = "serialize_asks", deserialize_with = "deserialize_levels")]
    pub asks: BTreeMap<Decimal, Decimal>,
    /// Timestamp in milliseconds since Unix epoch
    pub timestamp_ms: i64,
    /// Sequence number for ordering (if available)
    pub sequence: Option<u64>,
}

impl UnifiedOrderbook {
    pub fn new(
        exchange: Exchange,
        symbol: String,
        bids: BTreeMap<Decimal, Decimal>,
        asks: BTreeMap<Decimal, Decimal>,
        timestamp_ms: i64,
    ) -> Self {
        Self { exchange, symbol, bids, asks, timestamp_ms, sequence: None }
    }

    /// Create from Vec<PriceLevel> (convenience method for clients)
    pub fn from_levels(
        exchange: Exchange,
        symbol: String,
        bid_levels: Vec<PriceLevel>,
        ask_levels: Vec<PriceLevel>,
        timestamp_ms: i64,
    ) -> Self {
        let bids: BTreeMap<Decimal, Decimal> = bid_levels.into_iter().map(|l| (l.price, l.size)).collect();
        let asks: BTreeMap<Decimal, Decimal> = ask_levels.into_iter().map(|l| (l.price, l.size)).collect();
        Self::new(exchange, symbol, bids, asks, timestamp_ms)
    }

    /// Get the best bid price (highest)
    pub fn best_bid(&self) -> Option<PriceLevel> {
        self.bids.iter().next_back().map(|(p, s)| PriceLevel::new(*p, *s))
    }

    /// Get the best ask price (lowest)
    pub fn best_ask(&self) -> Option<PriceLevel> {
        self.asks.iter().next().map(|(p, s)| PriceLevel::new(*p, *s))
    }

    /// Calculate the spread
    pub fn spread(&self) -> Option<Decimal> {
        match (self.best_ask(), self.best_bid()) {
            (Some(ask), Some(bid)) => Some(ask.price - bid.price),
            _ => None,
        }
    }

    /// Calculate the mid price
    pub fn mid_price(&self) -> Option<Decimal> {
        match (self.best_ask(), self.best_bid()) {
            (Some(ask), Some(bid)) => Some((ask.price + bid.price) / Decimal::TWO),
            _ => None,
        }
    }

    /// Iterate bids in descending order (best bid first)
    pub fn bids_desc(&self) -> impl Iterator<Item = PriceLevel> + '_ {
        self.bids.iter().rev().map(|(p, s)| PriceLevel::new(*p, *s))
    }

    /// Iterate asks in ascending order (best ask first)
    pub fn asks_asc(&self) -> impl Iterator<Item = PriceLevel> + '_ {
        self.asks.iter().map(|(p, s)| PriceLevel::new(*p, *s))
    }
}
