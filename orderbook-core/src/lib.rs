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

pub mod listener;
pub mod orderbook;
pub mod publisher;
mod prelude;
mod state;
mod stream;
pub mod types;
pub mod consumer;

pub const HL_NODE: &str = "hl-node";

// Re-export main interface
pub use stream::{
    OrderBookStream,
    StreamConfig,
    StreamEvent,
};

// Re-export types for consumers who need them
pub use types::{
    Fill,
    L4Order,
    OrderDiff,
    Trade,
    Level,
    L2Book,
    L4Book,
    L4BookUpdates,
    node_data::{Batch, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
};

pub use orderbook::{Coin, Px, Sz, Side};

// Internal re-exports for the server crate
#[doc(hidden)]
pub mod internal {
    pub use crate::listener::*;
    pub use crate::state::*;
    pub use crate::orderbook::*;
    pub use crate::types::inner::*;
}
