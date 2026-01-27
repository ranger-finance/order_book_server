#![allow(unused_crate_dependencies)]
use std::net::Ipv4Addr;

use clap::Parser;
use tracing::{error, info};
use orderbook_core::listener::perform_cleanup;
use server::{Result, run_websocket_server};

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Args {
    /// Server address (e.g., 0.0.0.0)
    #[arg(long)]
    address: Ipv4Addr,

    /// Server port (e.g., 8000)
    #[arg(long)]
    port: u16,

    /// Compression level for WebSocket connections.
    /// Accepts values in the range `0..=9`.
    /// * `0` – compression disabled.
    /// * `1` – fastest compression, low compression ratio (default).
    /// * `9` – slowest compression, highest compression ratio.
    ///
    /// The level is passed to `flate2::Compression::new(level)`; see the
    /// documentation for <https://docs.rs/flate2/1.1.2/flate2/struct.Compression.html#method.new> for more info.
    #[arg(long)]
    websocket_compression_level: Option<u32>,

    /// Interval in hours between cleanup operations (optional)
    /// Cleanup removes data older than the retention period
    #[arg(long)]
    cleanup_interval: Option<u64>,

    /// Retention period in days for data cleanup (default: 7)
    #[arg(long, default_value = "7")]
    retention_days: u64,

    /// Base directory for node data (default: ./data)
    #[arg(long, default_value = "./data")]
    base_dir: std::path::PathBuf,

    /// AMQP URL for publishing L2 orderbook data (optional, defaults to LAVINMQ_URL env var)
    #[arg(long)]
    amqp_url: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    info!("Starting websocket_server");

    let args = Args::parse();

    let full_address = format!("{}:{}", args.address, args.port);
    println!("Running websocket server on {full_address}");

    let compression_level = args.websocket_compression_level.unwrap_or(/* Some compression */ 1);

    let amqp_url = args.amqp_url.or_else(|| std::env::var("LAVINMQ_URL").ok());

    if let Some(interval_hours) = args.cleanup_interval {
        info!("Cleanup enabled: running every {} hours with {} days retention", interval_hours, args.retention_days);
        info!("Initial cleanup before starting server...");

        let base_dir = args.base_dir.clone();
        let retention_days = args.retention_days as i64;

        perform_cleanup(base_dir.clone(), retention_days).await?;

        let cleanup_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(interval_hours * 3600));
            loop {
                interval.tick().await;
                info!("Running periodic cleanup...");
                if let Err(err) = perform_cleanup(base_dir.clone(), retention_days).await {
                    error!("Cleanup failed: {}", err);
                }
            }
        });

        tokio::pin!(cleanup_handle);

        tokio::select! {
            result = run_websocket_server(&full_address, true, compression_level, amqp_url.as_deref()) => {
                result?;
            }
            _ = &mut cleanup_handle => {
                return Err("Cleanup task exited unexpectedly".into());
            }
        }
    } else {
        run_websocket_server(&full_address, true, compression_level, amqp_url.as_deref()).await?;
    }

    Ok(())
}
