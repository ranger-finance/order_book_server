use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::orderbook::Exchange;

/// A single bucket in the liquidity distribution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiquidityBucket {
    /// Lower bound of the price bucket
    pub price_low: Decimal,
    /// Upper bound of the price bucket
    pub price_high: Decimal,
    /// Total liquidity (size) in this bucket
    pub liquidity: Decimal,
    /// Whether this is on the bid or ask side
    pub is_bid: bool,
}

/// Oracle price data point for the curve
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OraclePoint {
    /// Price level
    pub price: Decimal,
    /// Cumulative depth at this price
    pub cumulative_depth: Decimal,
}

/// Discretized oracle curve data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OracleCurve {
    /// Points on the bid side (sorted by price descending)
    pub bid_curve: Vec<OraclePoint>,
    /// Points on the ask side (sorted by price ascending)
    pub ask_curve: Vec<OraclePoint>,
    /// Reference/mid price
    pub reference_price: Decimal,
}

/// Complete analysis result for an orderbook
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderbookAnalysis {
    /// Source exchange
    pub exchange: Exchange,
    /// Trading symbol
    pub symbol: String,
    /// Liquidity distribution buckets
    pub liquidity_distribution: Vec<LiquidityBucket>,
    /// Discretized oracle curve for overlay
    pub oracle_curve: OracleCurve,
    /// Timestamp of analysis
    pub timestamp_ms: i64,
    /// Summary statistics
    pub stats: AnalysisStats,
}

/// Summary statistics for the orderbook
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalysisStats {
    /// Best bid price
    pub best_bid: Option<Decimal>,
    /// Best ask price
    pub best_ask: Option<Decimal>,
    /// Spread
    pub spread: Option<Decimal>,
    /// Spread as percentage of mid price
    pub spread_bps: Option<Decimal>,
    /// Mid price
    pub mid_price: Option<Decimal>,
    /// Total bid liquidity
    pub total_bid_liquidity: Decimal,
    /// Total ask liquidity
    pub total_ask_liquidity: Decimal,
    /// Imbalance ratio (bid_liquidity / ask_liquidity)
    pub imbalance_ratio: Option<Decimal>,
}

/// Combined message sent to frontend containing orderbook + analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderbookUpdate {
    /// The raw unified orderbook
    pub orderbook: super::orderbook::UnifiedOrderbook,
    /// Analysis results
    pub analysis: OrderbookAnalysis,
}
