use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use lapin::{
    Connection, ConnectionProperties,
    options::{BasicConsumeOptions, BasicQosOptions, QueueDeclareOptions},
    types::{FieldTable, LongString},
};
use orderbook_core::consumer::{L2Delta, L2OrderBookBuilder, L2OrderBookStreamer};
use orderbook_core::listener::utils::EventBatch;
use orderbook_core::orderbook::Coin;
use orderbook_core::types::node_data::{Batch, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, broadcast};
use tokio::time::{Duration, interval};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Args {
    #[arg(long, default_value = "amqp://localhost:5672")]
    amqp_url: String,

    #[arg(long, default_value = "hl.node_data")]
    amqp_queue: String,

    #[arg(long, default_value = "8001")]
    websocket_port: u16,

    #[arg(long, default_value = "0.0.0.0")]
    address: Ipv4Addr,

    #[arg(long, default_value = "/opt/orderbook-server/snapshots")]
    snapshot_dir: PathBuf,

    #[arg(long, default_value = "5")]
    snapshot_interval: u64,

    #[arg(long, default_value = "false")]
    ignore_spot: String,
}

impl Args {
    fn ignore_spot(&self) -> bool {
        self.ignore_spot.to_lowercase() == "true"
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "method")]
#[serde(rename_all = "camelCase")]
enum ClientMessage {
    Subscribe { coin: String },
    Unsubscribe { coin: String },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "channel", content = "data")]
#[serde(rename_all = "camelCase")]
enum ServerResponse {
    SubscriptionResponse,
    L2Update(L2Delta),
    Error(String),
}

async fn handle_websocket_client<S>(
    ws_stream: WebSocketStream<S>,
    mut delta_rx: broadcast::Receiver<L2Delta>,
    mut universe_rx: broadcast::Receiver<HashSet<Coin>>,
    ignore_spot: bool,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
    let mut subscribed_coins: HashSet<String> = HashSet::new();

    loop {
        tokio::select! {
            result = delta_rx.recv() => {
                match result {
                    Ok(delta) => {
                        if subscribed_coins.contains(&delta.coin) {
                            let response = ServerResponse::L2Update(delta);
                            if let Ok(msg) = serde_json::to_string(&response) {
                                if let Err(err) = ws_sender.send(Message::Text(msg.into())).await {
                                    error!("Failed to send WebSocket message: {}", err);
                                    break;
                                }
                            }
                        }
                    }
                    Err(err) => {
                        warn!("Delta receiver error: {}", err);
                    }
                }
            }
            result = universe_rx.recv() => {
                if let Ok(universe) = result {
                    let valid_universe: HashSet<String> = universe
                        .into_iter()
                        .filter(|c| !ignore_spot || !c.is_spot())
                        .map(|c| c.value().to_string())
                        .collect();

                    subscribed_coins.retain(|coin| valid_universe.contains(coin));
                }
            }
            result = ws_receiver.next() => {
                match result {
                    Some(Ok(msg)) => {
                        if msg.is_text() {
                            if let Ok(text) = msg.to_text() {
                                info!("Client message: {}", text);
                                if let Ok(client_msg) = serde_json::from_str::<ClientMessage>(text) {
                                    match client_msg {
                                        ClientMessage::Subscribe { coin } => {
                                            subscribed_coins.insert(coin.clone());
                                            if let Ok(resp) = serde_json::to_string(&ServerResponse::SubscriptionResponse) {
                                                let _ = ws_sender.send(Message::Text(resp.into())).await;
                                            }
                                        }
                                        ClientMessage::Unsubscribe { coin } => {
                                            subscribed_coins.remove(&coin);
                                            if let Ok(resp) = serde_json::to_string(&ServerResponse::SubscriptionResponse) {
                                                let _ = ws_sender.send(Message::Text(resp.into())).await;
                                            }
                                        }
                                    }
                                }
                            }
                        } else if msg.is_close() {
                            info!("Client disconnected");
                            break;
                        }
                    }
                    Some(Err(err)) => {
                        error!("WebSocket error: {}", err);
                        break;
                    }
                    None => {
                        info!("Client connection closed");
                        break;
                    }
                }
            }
        }
    }
}

async fn write_snapshot_to_file(
    coin: &str,
    bids: Vec<orderbook_core::types::Level>,
    asks: Vec<orderbook_core::types::Level>,
    timestamp: u64,
    sequence: u64,
    snapshot_dir: &PathBuf,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let snapshot_data = serde_json::json!({
        "coin": coin,
        "bids": bids,
        "asks": asks,
        "timestamp": timestamp,
        "sequence": sequence
    });

    let file_path = snapshot_dir.join(format!("{}.json", coin));
    let json_string = serde_json::to_string_pretty(&snapshot_data)?;

    fs::write(&file_path, json_string).await?;
    println!("Wrote snapshot for {} to {:?}", coin, file_path);
    Ok(())
}

async fn snapshot_writer_task(
    builder: Arc<Mutex<L2OrderBookBuilder>>,
    snapshot_dir: PathBuf,
    snapshot_interval_secs: u64,
) {
    let mut timer = interval(Duration::from_secs(snapshot_interval_secs));
    let mut sequence_counter: u64 = 0;

    info!("Snapshot writer task started, writing to {:?} every {} seconds", snapshot_dir, snapshot_interval_secs);
    println!("Snapshot writer task started, writing to {:?} every {} seconds", snapshot_dir, snapshot_interval_secs);
    loop {
        timer.tick().await;

        let snapshots = {
            let mut builder = builder.lock().await;
            builder.try_build_snapshot()
        };

        if let Some(snapshots) = snapshots {
            if snapshots.is_empty() {
                continue;
            }

            if let Err(err) = fs::create_dir_all(&snapshot_dir).await {
                error!("Failed to create snapshot directory {:?}: {}", snapshot_dir, err);
                continue;
            }

            sequence_counter += 1;
            info!("Writing snapshot #{} for {} coins", sequence_counter, snapshots.len());
            println!("Writing snapshot #{} for {} coins", sequence_counter, snapshots.len());
            for (coin, l2_book) in &snapshots {
                let coin_string = coin.value().clone();
                let bids = l2_book.levels[0].clone();
                let asks = l2_book.levels[1].clone();
                let timestamp = l2_book.time;

                if let Err(err) =
                    write_snapshot_to_file(&coin_string, bids, asks, timestamp, sequence_counter, &snapshot_dir).await
                {
                    error!("Failed to write snapshot for {}: {}", coin_string, err);
                }
            }
        }
    }
}

async fn amqp_consumer_task(
    amqp_url: String,
    queue_name: String,
    batch_tx: tokio::sync::mpsc::UnboundedSender<EventBatch>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("Connecting to AMQP at {}", amqp_url);
    let conn = Connection::connect(&amqp_url, ConnectionProperties::default()).await?;
    let channel = conn.create_channel().await?;

    channel.basic_qos(10, BasicQosOptions::default()).await?;

    let mut queue_args = FieldTable::default();
    queue_args.insert("x-dead-letter-exchange".into(), LongString::from("dlx_exchange").into());
    queue_args.insert("x-dead-letter-routing-key".into(), LongString::from("dlx_key").into());
    channel.queue_declare(&queue_name, QueueDeclareOptions { durable: true, ..Default::default() }, queue_args).await?;

    info!("Consuming from AMQP queue: {}", queue_name);
    let mut consumer =
        channel.basic_consume(&queue_name, "", BasicConsumeOptions::default(), FieldTable::default()).await?;

    while let Some(delivery_result) = consumer.next().await {
        match delivery_result {
            Ok(delivery) => {
                if let Err(err) = process_amqp_delivery(&delivery, &batch_tx).await {
                    error!("Failed to process AMQP delivery: {}", err);
                }
            }
            Err(err) => {
                error!("AMQP consumer error: {}", err);
            }
        }
    }

    Ok(())
}

async fn process_amqp_delivery(
    delivery: &lapin::message::Delivery,
    batch_tx: &tokio::sync::mpsc::UnboundedSender<EventBatch>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use lapin::options::{BasicAckOptions, BasicNackOptions};

    let text = std::str::from_utf8(&delivery.data)?;

    let batch_result = serde_json::from_str::<Batch<NodeDataFill>>(text)
        .map(|b| EventBatch::Fills(b))
        .or_else(|_| serde_json::from_str::<Batch<NodeDataOrderStatus>>(text).map(EventBatch::Orders))
        .or_else(|_| serde_json::from_str::<Batch<NodeDataOrderDiff>>(text).map(EventBatch::BookDiffs));

    let event_batch = match batch_result {
        Ok(batch) => batch,
        Err(_) => {
            warn!("Failed to parse AMQP message: unknown format");
            if let Err(err) = delivery.nack(BasicNackOptions { requeue: false, ..Default::default() }).await {
                error!("Failed to nack AMQP message: {}", err);
            };
            return Ok(());
        }
    };

    match batch_tx.send(event_batch) {
        Ok(_) => {
            if let Err(err) = delivery.ack(BasicAckOptions::default()).await {
                error!("Failed to ack AMQP message: {}", err);
            }
        }
        Err(err) => {
            error!("Failed to send batch to channel: {}", err);
            if let Err(err) = delivery.nack(BasicNackOptions { requeue: false, ..Default::default() }).await {
                error!("Failed to nack AMQP message: {}", err);
            }
        }
    }

    Ok(())
}

async fn broadcast_snapshots(
    builder: Arc<Mutex<L2OrderBookBuilder>>,
    streamer: Arc<Mutex<L2OrderBookStreamer>>,
    delta_tx: broadcast::Sender<L2Delta>,
    universe_tx: broadcast::Sender<HashSet<Coin>>,
) {
    let mut timer = interval(Duration::from_millis(100));
    loop {
        timer.tick().await;

        let new_snapshot = {
            let mut builder = builder.lock().await;
            builder.try_build_snapshot()
        };

        if let Some(snapshot) = new_snapshot {
            let mut streamer = streamer.lock().await;
            let deltas = streamer.compute_delta(&snapshot);
            streamer.update_snapshot(snapshot);

            for delta in deltas {
                let _unused = delta_tx.send(delta);
            }

            let universe = {
                let builder = builder.lock().await;
                builder.universe()
            };
            let _unused = universe_tx.send(universe);
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();

    info!("Starting l2_orderbook_consumer");

    let args = Args::parse();

    let ignore_spot = args.ignore_spot();
    let (batch_tx, mut batch_rx) = tokio::sync::mpsc::unbounded_channel();
    let (delta_tx, _) = broadcast::channel::<L2Delta>(100);
    let (universe_tx, _) = broadcast::channel::<HashSet<Coin>>(10);

    info!("Starting L2 Order Book Consumer");
    info!("AMQP URL: {}", args.amqp_url);
    info!("WebSocket port: {}", args.websocket_port);
    info!("Snapshot directory: {:?}", args.snapshot_dir);
    info!("Snapshot interval: {} seconds", args.snapshot_interval);

    let builder = L2OrderBookBuilder::with_ignore_spot(ignore_spot);
    let streamer = Arc::new(Mutex::new(L2OrderBookStreamer::new()));
    let builder = Arc::new(Mutex::new(builder));

    info!("Starting with empty orderbook state, building from queue events");

    let amqp_url = args.amqp_url.clone();
    let amqp_queue = args.amqp_queue.clone();
    let builder_clone = Arc::clone(&builder);
    let builder_clone2 = Arc::clone(&builder);
    let builder_clone3 = Arc::clone(&builder);
    let streamer_clone = Arc::clone(&streamer);
    let delta_tx_for_amqp = delta_tx.clone();
    let universe_tx_for_snapshots = universe_tx.clone();

    let snapshot_dir = args.snapshot_dir.clone();
    let snapshot_interval = args.snapshot_interval;

    tokio::spawn(async move {
        if let Err(err) = amqp_consumer_task(amqp_url, amqp_queue, batch_tx).await {
            error!("AMQP consumer task failed: {}", err);
        }
    });

    tokio::spawn(async move {
        while let Some(batch) = batch_rx.recv().await {
            let mut builder = builder_clone.lock().await;
            if let Err(err) = builder.consume_batch(batch) {
                error!("Failed to consume batch: {}", err);
            }
        }
    });

    tokio::spawn(broadcast_snapshots(builder_clone2, streamer_clone, delta_tx_for_amqp, universe_tx_for_snapshots));

    tokio::spawn(snapshot_writer_task(builder_clone3, snapshot_dir, snapshot_interval));

    let addr = format!("{}:{}", args.address, args.websocket_port);
    let listener = TcpListener::bind(&addr).await?;
    info!("WebSocket server running on ws://{}", addr);

    while let Ok((stream, addr)) = listener.accept().await {
        info!("New WebSocket connection from {}", addr);
        let delta_rx = delta_tx.subscribe();
        let universe_rx = universe_tx.subscribe();

        tokio::spawn(async move {
            let ws_stream = match tokio_tungstenite::accept_async(stream).await {
                Ok(s) => s,
                Err(err) => {
                    error!("WebSocket handshake failed: {}", err);
                    return;
                }
            };

            handle_websocket_client(ws_stream, delta_rx, universe_rx, ignore_spot).await;
        });
    }

    Ok(())
}
