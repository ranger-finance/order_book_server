#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
pub mod websocket;

pub use orderbook_core::{OrderBookStream, StreamConfig, StreamEvent, Level, L2Book, L4Book, Trade};
pub use orderbook_core::publisher::{AmqpPublisher, shared_publisher, SharedAmqpPublisher};
pub use websocket::run_websocket_server;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
