#![allow(unused_crate_dependencies)]
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use lapin::{
    Connection, ConnectionProperties,
    options::{BasicConsumeOptions, BasicQosOptions, QueueDeclareOptions},
    types::{FieldTable, LongString},
};

use orderbook_core::types::Level;
use orderbook_core::types::amqp::{L2DeltaMessage, L2SnapshotMessage};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::net::Ipv4Addr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, broadcast};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

const SNAPSHOT_INTERVAL_SECONDS: u64 = 5;
const SNAPSHOT_DIR: &str = "./snapshot";

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Args {
    #[arg(long, default_value = "amqp://localhost:5672")]
    amqp_url: String,

    #[arg(long, default_value = "hl.l2.snapshot")]
    snapshot_queue: String,

    #[arg(long, default_value = "hl.l2.delta")]
    delta_queue: String,

    #[arg(long, default_value = "8002")]
    websocket_port: u16,

    #[arg(long, default_value = "0.0.0.0")]
    address: Ipv4Addr,

    #[arg(long, default_value = "false")]
    ignore_spot: String,
}

impl Args {
    fn ignore_spot(&self) -> bool {
        self.ignore_spot.to_lowercase() == "true"
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    L2Update(ThinL2Update),
    Snapshot(L2SnapshotMessage),
    Error(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThinL2Update {
    pub coin: String,
    pub timestamp: u64,
    pub sequence: u64,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
    pub is_snapshot: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderBookSnapshot {
    pub coin: String,
    pub timestamp: u64,
    pub sequence: u64,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

#[derive(Debug, Clone)]
pub struct L2OrderBook {
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
    pub sequence: u64,
    pub timestamp: u64,
    pub coin: String,
}

impl L2OrderBook {
    pub fn new(coin: String) -> Self {
        Self { coin, bids: Vec::new(), asks: Vec::new(), sequence: 0, timestamp: 0 }
    }

    pub fn apply_snapshot(&mut self, snapshot: &L2SnapshotMessage) -> Result<(), String> {
        self.bids = self.convert_levels(&snapshot.bids);
        self.asks = self.convert_levels(&snapshot.asks);
        self.sequence = snapshot.sequence;
        self.timestamp = snapshot.timestamp;
        self.coin = snapshot.coin.clone();
        Ok(())
    }

    pub fn apply_delta(&mut self, delta: &L2DeltaMessage) -> Result<(), String> {
        // Throw away the delta as we missed some updates or are ahead
        if delta.from_sequence != self.sequence {
            return Err(format!("Sequence mismatch: expected {}, got {}", self.sequence, delta.from_sequence));
        }

        let bids = delta.bids.clone();
        let asks = delta.asks.clone();

        if !bids.is_empty() {
            Self::update_levels(&mut self.bids, &bids);
        }
        if !asks.is_empty() {
            Self::update_levels(&mut self.asks, &asks);
        }

        self.sequence = delta.sequence;
        self.timestamp = delta.timestamp;
        Ok(())
    }

    fn update_levels(levels: &mut Vec<Level>, new_levels: &[(String, String)]) {
        for (px, sz) in new_levels {
            let updated_level = Level::new(px.clone(), sz.clone(), 1);
            if sz == "0" {
                levels.retain(|l| l.px != *px);
            } else {
                if let Some(pos) = levels.iter().position(|l| l.px == *px) {
                    levels[pos] = updated_level;
                } else {
                    levels.push(updated_level);
                }
            }
        }
    }

    fn convert_levels(&self, levels: &[(String, String)]) -> Vec<Level> {
        levels.iter().map(|(px, sz)| Level::new(px.clone(), sz.clone(), 1)).collect()
    }

    pub fn to_thin_update(&self, is_snapshot: bool) -> ThinL2Update {
        ThinL2Update {
            coin: self.coin.clone(),
            timestamp: self.timestamp,
            sequence: self.sequence,
            bids: self.bids.clone(),
            asks: self.asks.clone(),
            is_snapshot,
        }
    }
}

async fn handle_websocket_client<S>(
    ws_stream: WebSocketStream<S>,
    mut update_rx: broadcast::Receiver<ThinL2Update>,
    mut universe_rx: broadcast::Receiver<HashSet<String>>,
    _ignore_spot: bool,
    orderbooks: Arc<Mutex<HashMap<String, L2OrderBook>>>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
    let mut subscribed_coins: HashSet<String> = HashSet::new();

    loop {
        tokio::select! {
            result = update_rx.recv() => {
                match result {
                    Ok(update) => {
                        if subscribed_coins.contains(&update.coin) {
                            let response = ServerResponse::L2Update(update);
                            if let Ok(msg) = serde_json::to_string(&response) {
                                if let Err(err) = ws_sender.send(Message::Text(msg.into())).await {
                                    error!("Failed to send WebSocket message: {}", err);
                                    break;
                                }
                            }
                        }
                    }
                    Err(err) => {
                        warn!("Update receiver error: {}", err);
                    }
                }
            }
            result = universe_rx.recv() => {
                if let Ok(universe) = result {
                    subscribed_coins.retain(|coin| universe.contains(coin));
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
                                            if subscribed_coins.insert(coin.clone()) {
                                                let orderbooks = orderbooks.lock().await;
                                                if let Some(book) = orderbooks.get(&coin) {
                                                    let response = ServerResponse::L2Update(book.to_thin_update(true));
                                                    if let Ok(msg) = serde_json::to_string(&response) {
                                                        let _unused = ws_sender.send(Message::Text(msg.into())).await;
                                                    }
                                                }
                                            }
                                            if let Ok(resp) = serde_json::to_string(&ServerResponse::SubscriptionResponse) {
                                                let _unused = ws_sender.send(Message::Text(resp.into())).await;
                                            }
                                        }
                                        ClientMessage::Unsubscribe { coin } => {
                                            subscribed_coins.remove(&coin);
                                            if let Ok(resp) = serde_json::to_string(&ServerResponse::SubscriptionResponse) {
                                                let _unused = ws_sender.send(Message::Text(resp.into())).await;
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

async fn process_snapshot(
    snapshot: L2SnapshotMessage,
    orderbooks: Arc<Mutex<HashMap<String, L2OrderBook>>>,
    update_tx: broadcast::Sender<ThinL2Update>,
    universe_tx: broadcast::Sender<HashSet<String>>,
    ignore_spot: bool,
) {
    let coin = snapshot.coin.clone();

    if ignore_spot && (coin.starts_with('@') || coin == "PURR/USDC") {
        return;
    }

    let mut books = orderbooks.lock().await;
    let book = books.entry(coin.clone()).or_insert_with(|| L2OrderBook::new(coin.clone()));

    if let Err(err) = book.apply_snapshot(&snapshot) {
        error!("Failed to apply snapshot for {}: {}", coin, err);
        return;
    }

    let update = book.to_thin_update(true);
    drop(books);

    let _unused = update_tx.send(update);

    let universe = {
        let books = orderbooks.lock().await;
        books.keys().cloned().collect()
    };
    let _unused = universe_tx.send(universe);
}

async fn process_delta(
    delta: L2DeltaMessage,
    orderbooks: Arc<Mutex<HashMap<String, L2OrderBook>>>,
    update_tx: broadcast::Sender<ThinL2Update>,
) {
    let coin = delta.coin.clone();

    let mut books = orderbooks.lock().await;
    let book = books.entry(coin.clone()).or_insert_with(|| {
        warn!("Received delta for unknown coin {}, requesting snapshot", coin);
        L2OrderBook::new(coin.clone())
    });

    match book.apply_delta(&delta) {
        Ok(_) => {
            let update = book.to_thin_update(false);
            drop(books);
            let _unused = update_tx.send(update);
        }
        Err(_err) => {
            // warn!("Failed to apply delta for {}: {}, requesting snapshot", coin, err);
        }
    }
}

async fn amqp_snapshot_consumer_task(
    amqp_url: String,
    queue_name: String,
    orderbooks: Arc<Mutex<HashMap<String, L2OrderBook>>>,
    update_tx: broadcast::Sender<ThinL2Update>,
    universe_tx: broadcast::Sender<HashSet<String>>,
    ignore_spot: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use lapin::options::{BasicAckOptions, BasicNackOptions};

    info!("Connecting to AMQP at {} for snapshot queue {}", amqp_url, queue_name);
    let conn = Connection::connect(&amqp_url, ConnectionProperties::default()).await?;
    let channel = conn.create_channel().await?;

    channel.basic_qos(10, BasicQosOptions::default()).await?;

    let mut queue_args = FieldTable::default();
    queue_args.insert("x-dead-letter-exchange".into(), LongString::from("dlx_exchange").into());
    queue_args.insert("x-dead-letter-routing-key".into(), LongString::from("dlx_key").into());
    channel.queue_declare(&queue_name, QueueDeclareOptions { durable: true, ..Default::default() }, queue_args).await?;

    info!("Consuming from AMQP snapshot queue: {}", queue_name);
    let mut consumer =
        channel.basic_consume(&queue_name, "", BasicConsumeOptions::default(), FieldTable::default()).await?;

    while let Some(delivery_result) = consumer.next().await {
        match delivery_result {
            Ok(delivery) => {
                let text = match std::str::from_utf8(&delivery.data) {
                    Ok(t) => t,
                    Err(err) => {
                        error!("Failed to parse AMQP message as UTF-8: {}", err);
                        let _unused = delivery.nack(BasicNackOptions { requeue: false, ..Default::default() }).await;
                        continue;
                    }
                };

                match serde_json::from_str::<L2SnapshotMessage>(text) {
                    Ok(snapshot) => {
                        process_snapshot(
                            snapshot,
                            Arc::clone(&orderbooks),
                            update_tx.clone(),
                            universe_tx.clone(),
                            ignore_spot,
                        )
                        .await;
                        if let Err(err) = delivery.ack(BasicAckOptions::default()).await {
                            error!("Failed to ack AMQP message: {}", err);
                        }
                    }
                    Err(err) => {
                        error!("Failed to parse snapshot message: {}", err);
                        if let Err(err) = delivery.nack(BasicNackOptions { requeue: false, ..Default::default() }).await
                        {
                            error!("Failed to nack AMQP message: {}", err);
                        }
                    }
                }
            }
            Err(err) => {
                error!("AMQP consumer error: {}", err);
            }
        }
    }

    Ok(())
}

async fn amqp_delta_consumer_task(
    amqp_url: String,
    queue_name: String,
    orderbooks: Arc<Mutex<HashMap<String, L2OrderBook>>>,
    update_tx: broadcast::Sender<ThinL2Update>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use lapin::options::{BasicAckOptions, BasicNackOptions};

    info!("Connecting to AMQP at {} for delta queue {}", amqp_url, queue_name);
    let conn = Connection::connect(&amqp_url, ConnectionProperties::default()).await?;
    let channel = conn.create_channel().await?;

    channel.basic_qos(10, BasicQosOptions::default()).await?;

    let mut queue_args = FieldTable::default();
    queue_args.insert("x-dead-letter-exchange".into(), LongString::from("dlx_exchange").into());
    queue_args.insert("x-dead-letter-routing-key".into(), LongString::from("dlx_key").into());
    channel.queue_declare(&queue_name, QueueDeclareOptions { durable: true, ..Default::default() }, queue_args).await?;

    info!("Consuming from AMQP delta queue: {}", queue_name);
    let mut consumer =
        channel.basic_consume(&queue_name, "", BasicConsumeOptions::default(), FieldTable::default()).await?;

    while let Some(delivery_result) = consumer.next().await {
        match delivery_result {
            Ok(delivery) => {
                let text = match std::str::from_utf8(&delivery.data) {
                    Ok(t) => t,
                    Err(err) => {
                        error!("Failed to parse AMQP message as UTF-8: {}", err);
                        let _unused = delivery.nack(BasicNackOptions { requeue: false, ..Default::default() }).await;
                        continue;
                    }
                };

                match serde_json::from_str::<L2DeltaMessage>(text) {
                    Ok(delta) => {
                        process_delta(delta, Arc::clone(&orderbooks), update_tx.clone()).await;
                        if let Err(err) = delivery.ack(BasicAckOptions::default()).await {
                            error!("Failed to ack AMQP message: {}", err);
                        }
                    }
                    Err(err) => {
                        error!("Failed to parse delta message: {}", err);
                        if let Err(err) = delivery.nack(BasicNackOptions { requeue: false, ..Default::default() }).await
                        {
                            error!("Failed to nack AMQP message: {}", err);
                        }
                    }
                }
            }
            Err(err) => {
                error!("AMQP consumer error: {}", err);
            }
        }
    }

    Ok(())
}

async fn snapshot_writer_task(orderbooks: Arc<Mutex<HashMap<String, L2OrderBook>>>) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(SNAPSHOT_INTERVAL_SECONDS));

    info!("Starting snapshot writer task, saving every {} seconds to {}", SNAPSHOT_INTERVAL_SECONDS, SNAPSHOT_DIR);

    if let Err(err) = fs::create_dir_all(SNAPSHOT_DIR) {
        error!("Failed to create snapshot directory '{}': {}", SNAPSHOT_DIR, err);
        return;
    }

    loop {
        interval.tick().await;

        let books = orderbooks.lock().await;
        for (coin, book) in books.iter() {
            let snapshot = OrderBookSnapshot {
                coin: coin.clone(),
                timestamp: book.timestamp,
                sequence: book.sequence,
                bids: book.bids.clone(),
                asks: book.asks.clone(),
            };

            let filename = format!("{}/{}.json", SNAPSHOT_DIR, coin);
            match serde_json::to_string_pretty(&snapshot) {
                Ok(json) => {
                    if let Err(err) = fs::write(&filename, json) {
                        error!("Failed to write snapshot for {}: {}", coin, err);
                    }
                }
                Err(err) => {
                    error!("Failed to serialize snapshot for {}: {}", coin, err);
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();

    info!("Starting l2_orderbook_consumer_thin");

    let args = Args::parse();

    let ignore_spot = args.ignore_spot();
    let (update_tx, _) = broadcast::channel::<ThinL2Update>(100);
    let (universe_tx, _) = broadcast::channel::<HashSet<String>>(10);

    info!("Starting L2 Order Book Thin Consumer");
    info!("AMQP URL: {}", args.amqp_url);
    info!("Snapshot Queue: {}", args.snapshot_queue);
    info!("Delta Queue: {}", args.delta_queue);
    info!("WebSocket port: {}", args.websocket_port);

    let orderbooks = Arc::new(Mutex::new(HashMap::<String, L2OrderBook>::new()));

    let amqp_url = args.amqp_url.clone();
    let amqp_url_clone = amqp_url.clone();
    let snapshot_queue = args.snapshot_queue.clone();
    let delta_queue = args.delta_queue.clone();
    let orderbooks_clone = Arc::clone(&orderbooks);
    let orderbooks_clone2 = Arc::clone(&orderbooks);
    let orderbooks_clone3 = Arc::clone(&orderbooks);
    let update_tx_clone = update_tx.clone();
    let update_tx_clone2 = update_tx.clone();
    let universe_tx_clone = universe_tx.clone();

    info!("Starting thin consumer - receiving pre-computed L2 snapshots and deltas");

    tokio::spawn(async move {
        if let Err(err) = amqp_snapshot_consumer_task(
            amqp_url,
            snapshot_queue,
            orderbooks_clone,
            update_tx_clone,
            universe_tx_clone,
            ignore_spot,
        )
        .await
        {
            error!("AMQP snapshot consumer task failed: {}", err);
        }
    });

    tokio::spawn(async move {
        if let Err(err) =
            amqp_delta_consumer_task(amqp_url_clone, delta_queue, orderbooks_clone2, update_tx_clone2).await
        {
            error!("AMQP delta consumer task failed: {}", err);
        }
    });

    tokio::spawn(async move {
        snapshot_writer_task(orderbooks_clone3).await;
    });

    let addr = format!("{}:{}", args.address, args.websocket_port);
    let listener = TcpListener::bind(&addr).await?;
    info!("WebSocket server running on ws://{}", addr);

    while let Ok((stream, addr)) = listener.accept().await {
        info!("New WebSocket connection from {}", addr);
        let update_rx = update_tx.subscribe();
        let universe_rx = universe_tx.subscribe();
        let orderbooks_for_client = Arc::clone(&orderbooks);

        tokio::spawn(async move {
            let ws_stream = match tokio_tungstenite::accept_async(stream).await {
                Ok(s) => s,
                Err(err) => {
                    error!("WebSocket handshake failed: {}", err);
                    return;
                }
            };

            handle_websocket_client(ws_stream, update_rx, universe_rx, ignore_spot, orderbooks_for_client).await;
        });
    }

    Ok(())
}
