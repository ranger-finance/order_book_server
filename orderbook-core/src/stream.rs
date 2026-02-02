use crate::{
    listener::{OrderBookListener, hl_listen},
    types::node_data::{Batch, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
};
use orderbook_normaliser::models::UnifiedOrderbook;
use std::{collections::HashSet, path::PathBuf, sync::Arc};
use tokio::sync::{Mutex, broadcast};

/// Events emitted by the orderbook stream
#[derive(Debug, Clone)]
pub enum StreamEvent {
    /// Unified orderbook snapshot for a symbol
    OrderbookSnapshot(UnifiedOrderbook),

    /// L4 book updates (order-level)
    L4Update {
        coin: String,
        time: u64,
        height: u64,
        order_statuses: Vec<NodeDataOrderStatus>,
        book_diffs: Vec<NodeDataOrderDiff>,
    },
    /// Raw fill batch
    Fill { batch: Batch<NodeDataFill> },
    /// Stream is ready (initial snapshot available)
    Ready,
    /// Error occurred
    Error(String),
}

/// Configuration for the orderbook stream
#[derive(Debug, Clone)]
pub struct StreamConfig {
    /// Base directory for hl-node data
    pub data_dir: PathBuf,
    /// Ignore spot markets
    pub ignore_spot: bool,
    /// Number of L2 levels to include in snapshots
    pub l2_levels: usize,
    /// Significant figures for price bucketing (None = no bucketing)
    pub n_sig_figs: Option<u32>,
    /// Mantissa for price bucketing
    pub mantissa: Option<u64>,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from(dirs::home_dir().unwrap_or_default()),
            ignore_spot: true,
            l2_levels: 20,
            n_sig_figs: None,
            mantissa: None,
        }
    }
}

/// Main streaming interface for orderbook data
pub struct OrderBookStream {
    config: StreamConfig,
    listener: Arc<Mutex<OrderBookListener>>,
    event_tx: broadcast::Sender<StreamEvent>,
}

impl OrderBookStream {
    /// Create a new orderbook stream
    pub async fn new(config: StreamConfig) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let (event_tx, _) = broadcast::channel(1024);

        let listener = OrderBookListener::new(None, config.ignore_spot, None, None, config.l2_levels);
        let listener = Arc::new(Mutex::new(listener));

        Ok(Self { config, listener, event_tx })
    }

    /// Start the stream (spawns background listener task)
    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = self.listener.clone();
        let data_dir = self.config.data_dir.clone();
        let event_tx = self.event_tx.clone();

        tokio::spawn(async move {
            if let Err(err) = hl_listen(listener, data_dir).await {
                log::error!("OrderBookStream listener error: {err}");
                let _unused = event_tx.send(StreamEvent::Error(err.to_string()));
            }
        });

        Ok(())
    }

    /// Subscribe to stream events
    pub fn subscribe(&self) -> broadcast::Receiver<StreamEvent> {
        self.event_tx.subscribe()
    }

    /// Check if the stream is ready (has received initial snapshot)
    pub async fn is_ready(&self) -> bool {
        self.listener.lock().await.is_ready()
    }

    /// Get the universe of available coins
    pub async fn get_universe(&self) -> HashSet<String> {
        self.listener.lock().await.universe().into_iter().map(|c| c.value()).collect()
    }
}
