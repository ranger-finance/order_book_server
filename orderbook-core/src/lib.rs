//! Core orderbook streaming library for Hyperliquid node data.
//!
//! This crate provides:
//! - Real-time L2/L4 orderbook streaming from hl-node file data
//! - Orderbook state management and snapshot computation
//! - Type definitions for orderbook data
//!
//! # Example
//! ```rust,no_run
//! use orderbook_core::{OrderBookStream, StreamConfig, StreamEvent};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//!     let config = StreamConfig::default();
//!     let stream = OrderBookStream::new(config).await?;
//!
//!     let mut receiver = stream.subscribe();
//!     stream.start().await?;
//!
//!     while let Ok(event) = receiver.recv().await {
//!         match event {
//!             StreamEvent::L2Snapshot { coin, bids, asks, .. } => {
//!                 println!("L2 update for {coin}: {} bids, {} asks", bids.len(), asks.len());
//!             }
//!             StreamEvent::Ready => {
//!                 println!("Stream ready!");
//!             }
//!             _ => {}
//!         }
//!     }
//!
//!     Ok(())
//! }
//! ```

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod cache;
pub mod config;
pub mod l2_emitter;
pub mod listener;
pub mod orderbook;
mod prelude;
pub mod redis;
mod state;
mod stream;
pub mod types;

pub const HL_NODE: &str = "hl-node";

// Re-export main interface
pub use stream::{OrderBookStream, StreamConfig, StreamEvent};

// Re-export L2Emitter for external use
pub use l2_emitter::L2Emitter;

// Re-export Redis modules
pub use redis::{RedisConfig, RedisPublisher};

// Re-export types for consumers who need them
pub use types::{
    node_data::{Batch, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
    Fill, L2Book, L4Book, L4BookUpdates, L4Order, Level, OrderDiff, Trade,
};

pub use orderbook::{Coin, Px, Side, Sz};

// Internal re-exports for the server crate
#[doc(hidden)]
pub mod internal {
    pub use crate::listener::*;
    pub use crate::orderbook::*;
    pub use crate::state::*;
    pub use crate::types::inner::*;
}
