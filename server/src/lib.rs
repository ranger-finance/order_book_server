#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
pub mod websocket;

pub use orderbook_core::{L4Book, Level, OrderBookStream, StreamConfig, StreamEvent, Trade, UnifiedOrderbook};
pub use websocket::run_websocket_server;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
