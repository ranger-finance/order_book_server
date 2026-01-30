# Orderbook Normaliser Types

A Rust library providing unified orderbook types for normalizing exchange-specific orderbook data into a common format.

## Features

- UnifiedOrderbook struct with Decimal precision
- Exchange enum (Hyperliquid, Drift)
- PriceLevel types
- Orderbook analysis utilities
- Serde serialization support

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
orderbook_normaliser = { path = "../orderbook-normaliser" }
```

```rust
use orderbook_normaliser::models::{UnifiedOrderbook, Exchange, PriceLevel};
use rust_decimal::Decimal;

// Create a unified orderbook
let orderbook = UnifiedOrderbook::new(
    Exchange::Hyperliquid,
    "BTC-PERP".to_string(),
    bids,
    asks,
    timestamp_ms,
);
```

## Architecture

This is a types library only, providing unified data structures for orderbook normalization and analysis.

## Dependencies

- `serde` - Serialization/deserialization
- `rust_decimal` - High-precision decimal math
- `thiserror` - Error handling
