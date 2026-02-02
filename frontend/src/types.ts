// Types matching the Rust backend

export interface PriceLevel {
  price: string;
  size: string;
}

export type Exchange = "hyperliquid" | "drift";

export type Token = "SOL" | "BTC" | "ETH";

export const AVAILABLE_TOKENS: Token[] = ["SOL", "BTC", "ETH"];

export interface UnifiedOrderbook {
  exchange: Exchange;
  symbol: string;
  bids: PriceLevel[];
  asks: PriceLevel[];
  timestamp_ms: number;
  sequence?: number;
}

export interface LiquidityBucket {
  price_low: string;
  price_high: string;
  liquidity: string;
  is_bid: boolean;
}

export interface OraclePoint {
  price: string;
  cumulative_depth: string;
}

export interface OracleCurve {
  bid_curve: OraclePoint[];
  ask_curve: OraclePoint[];
  reference_price: string;
}

export interface AnalysisStats {
  best_bid: string | null;
  best_ask: string | null;
  spread: string | null;
  spread_bps: string | null;
  mid_price: string | null;
  total_bid_liquidity: string;
  total_ask_liquidity: string;
  imbalance_ratio: string | null;
}

export interface OrderbookAnalysis {
  exchange: Exchange;
  symbol: string;
  liquidity_distribution: LiquidityBucket[];
  oracle_curve: OracleCurve;
  timestamp_ms: number;
  stats: AnalysisStats;
}

export interface OrderbookUpdate {
  orderbook: UnifiedOrderbook;
  analysis: OrderbookAnalysis;
}
