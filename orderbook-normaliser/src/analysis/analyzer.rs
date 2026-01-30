use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use crate::models::{AnalysisStats, LiquidityBucket, OracleCurve, OraclePoint, OrderbookAnalysis, UnifiedOrderbook};

/// Configuration for the analysis
#[derive(Debug, Clone)]
pub struct AnalysisConfig {
    /// Number of price buckets on each side (bid/ask)
    pub num_buckets: usize,
    /// Percentage range from mid price to analyze (e.g., 0.05 = 5%)
    pub price_range_pct: Decimal,
    /// Number of points for oracle curve discretization
    pub oracle_curve_points: usize,
}

impl Default for AnalysisConfig {
    fn default() -> Self {
        Self {
            num_buckets: 20,
            price_range_pct: dec!(0.05), // 5% range
            oracle_curve_points: 50,
        }
    }
}

/// Analyzes an orderbook and produces liquidity distribution and oracle curve
pub struct OrderbookAnalyzer {
    config: AnalysisConfig,
}

impl OrderbookAnalyzer {
    pub fn new(config: AnalysisConfig) -> Self {
        Self { config }
    }

    pub fn with_default_config() -> Self {
        Self::new(AnalysisConfig::default())
    }

    /// Analyze an orderbook and produce analysis results
    pub fn analyze(&self, orderbook: &UnifiedOrderbook) -> OrderbookAnalysis {
        let stats = self.calculate_stats(orderbook);

        let mid_price = stats.mid_price.unwrap_or(Decimal::ZERO);

        let liquidity_distribution = if mid_price > Decimal::ZERO {
            self.calculate_liquidity_distribution(orderbook, mid_price)
        } else {
            vec![]
        };

        let oracle_curve = if mid_price > Decimal::ZERO {
            self.discretize_oracle_curve(orderbook, mid_price)
        } else {
            OracleCurve { bid_curve: vec![], ask_curve: vec![], reference_price: Decimal::ZERO }
        };

        OrderbookAnalysis {
            exchange: orderbook.exchange,
            symbol: orderbook.symbol.clone(),
            liquidity_distribution,
            oracle_curve,
            timestamp_ms: orderbook.timestamp_ms,
            stats,
        }
    }

    /// Calculate summary statistics for the orderbook
    fn calculate_stats(&self, orderbook: &UnifiedOrderbook) -> AnalysisStats {
        let best_bid = orderbook.best_bid().map(|l| l.price);
        let best_ask = orderbook.best_ask().map(|l| l.price);
        let spread = orderbook.spread();
        let mid_price = orderbook.mid_price();

        let spread_bps = match (spread, mid_price) {
            (Some(s), Some(m)) if m > Decimal::ZERO => {
                Some((s / m) * dec!(10000)) // basis points
            }
            _ => None,
        };

        let total_bid_liquidity: Decimal = orderbook.bids.values().copied().sum();
        let total_ask_liquidity: Decimal = orderbook.asks.values().copied().sum();

        let imbalance_ratio =
            if total_ask_liquidity > Decimal::ZERO { Some(total_bid_liquidity / total_ask_liquidity) } else { None };

        AnalysisStats {
            best_bid,
            best_ask,
            spread,
            spread_bps,
            mid_price,
            total_bid_liquidity,
            total_ask_liquidity,
            imbalance_ratio,
        }
    }

    /// Calculate liquidity distribution in price buckets
    fn calculate_liquidity_distribution(
        &self,
        orderbook: &UnifiedOrderbook,
        mid_price: Decimal,
    ) -> Vec<LiquidityBucket> {
        let mut buckets = Vec::new();

        let price_range = mid_price * self.config.price_range_pct;
        let bucket_size = price_range / Decimal::from(self.config.num_buckets);

        // Bid side buckets (from mid price going down)
        for i in 0..self.config.num_buckets {
            let bucket_idx = Decimal::from(i);
            let price_high = mid_price - (bucket_size * bucket_idx);
            let price_low = price_high - bucket_size;

            let liquidity: Decimal = orderbook
                .bids
                .iter()
                .filter(|(price, _)| **price <= price_high && **price > price_low)
                .map(|(_, size)| *size)
                .sum();

            buckets.push(LiquidityBucket { price_low, price_high, liquidity, is_bid: true });
        }

        // Ask side buckets (from mid price going up)
        for i in 0..self.config.num_buckets {
            let bucket_idx = Decimal::from(i);
            let price_low = mid_price + (bucket_size * bucket_idx);
            let price_high = price_low + bucket_size;

            let liquidity: Decimal = orderbook
                .asks
                .iter()
                .filter(|(price, _)| **price >= price_low && **price < price_high)
                .map(|(_, size)| *size)
                .sum();

            buckets.push(LiquidityBucket { price_low, price_high, liquidity, is_bid: false });
        }

        buckets
    }

    /// Discretize the oracle/depth curve for overlay visualization
    /// This creates cumulative depth at each price point
    fn discretize_oracle_curve(&self, orderbook: &UnifiedOrderbook, mid_price: Decimal) -> OracleCurve {
        let price_range = mid_price * self.config.price_range_pct;
        let step = price_range / Decimal::from(self.config.oracle_curve_points);

        // Bid curve - cumulative depth going down from mid price
        let mut bid_curve = Vec::new();

        for i in 0..self.config.oracle_curve_points {
            let price = mid_price - (step * Decimal::from(i));

            // Cumulative liquidity from mid_price down to this price
            let cumulative_depth: Decimal =
                orderbook.bids.iter().filter(|(p, _)| **p <= mid_price && **p > price).map(|(_, size)| *size).sum();

            bid_curve.push(OraclePoint { price, cumulative_depth });
        }

        // Ask curve - cumulative depth going up from mid price
        let mut ask_curve = Vec::new();

        for i in 0..self.config.oracle_curve_points {
            let price = mid_price + (step * Decimal::from(i));

            // Cumulative liquidity from mid_price up to this price
            let cumulative_depth: Decimal =
                orderbook.asks.iter().filter(|(p, _)| **p >= mid_price && **p < price).map(|(_, size)| *size).sum();

            ask_curve.push(OraclePoint { price, cumulative_depth });
        }

        OracleCurve { bid_curve, ask_curve, reference_price: mid_price }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{Exchange, PriceLevel};

    fn create_test_orderbook() -> UnifiedOrderbook {
        let bids = vec![
            PriceLevel::new(dec!(100.0), dec!(10.0)),
            PriceLevel::new(dec!(99.5), dec!(20.0)),
            PriceLevel::new(dec!(99.0), dec!(30.0)),
            PriceLevel::new(dec!(98.0), dec!(50.0)),
        ];

        let asks = vec![
            PriceLevel::new(dec!(100.5), dec!(15.0)),
            PriceLevel::new(dec!(101.0), dec!(25.0)),
            PriceLevel::new(dec!(101.5), dec!(35.0)),
            PriceLevel::new(dec!(102.0), dec!(45.0)),
        ];

        UnifiedOrderbook::from_levels(Exchange::Hyperliquid, "BTC-PERP".to_string(), bids, asks, 1234567890)
    }

    #[test]
    fn test_calculate_stats() {
        let orderbook = create_test_orderbook();
        let analyzer = OrderbookAnalyzer::with_default_config();
        let stats = analyzer.calculate_stats(&orderbook);

        assert_eq!(stats.best_bid, Some(dec!(100.0)));
        assert_eq!(stats.best_ask, Some(dec!(100.5)));
        assert_eq!(stats.spread, Some(dec!(0.5)));
        assert_eq!(stats.mid_price, Some(dec!(100.25)));
        assert_eq!(stats.total_bid_liquidity, dec!(110.0));
        assert_eq!(stats.total_ask_liquidity, dec!(120.0));
    }

    #[test]
    fn test_analyze() {
        let orderbook = create_test_orderbook();
        let analyzer = OrderbookAnalyzer::with_default_config();
        let analysis = analyzer.analyze(&orderbook);

        assert!(!analysis.liquidity_distribution.is_empty());
        assert!(!analysis.oracle_curve.bid_curve.is_empty());
        assert!(!analysis.oracle_curve.ask_curve.is_empty());
    }
}
