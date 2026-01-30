use axum::{Router, response::IntoResponse, routing::get};
use futures_util::{SinkExt, StreamExt};
use log::{error, info};
use orderbook_core::{
    L2Book, L4Book, L4BookUpdates, L4Order, RedisConfig, RedisPublisher, Trade, UnifiedOrderbook,
    internal::{
        Coin, InnerLevel, InternalMessage, L2SnapshotParams, L2Snapshots, OrderBookListener, Snapshot, TimedSnapshots,
        hl_listen,
    },
    types::node_data::{Batch, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::select;
use tokio::{
    net::TcpListener,
    sync::{
        Mutex,
        broadcast::{Sender, channel},
    },
};
use yawc::{FrameView, OpCode, WebSocket};

const DEFAULT_LEVELS: usize = 20;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "method")]
#[serde(rename_all = "camelCase")]
enum ClientMessage {
    Subscribe { subscription: Subscription },
    Unsubscribe { subscription: Subscription },
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "camelCase")]
enum Subscription {
    #[serde(rename_all = "camelCase")]
    Trades { coin: String },
    #[serde(rename_all = "camelCase")]
    L2Book { coin: String, n_sig_figs: Option<u32>, n_levels: Option<usize>, mantissa: Option<u64> },
    #[serde(rename_all = "camelCase")]
    L4Book { coin: String },
}

impl Subscription {
    fn validate(&self, universe: &HashSet<String>) -> bool {
        match self {
            Self::Trades { coin } => universe.contains(coin),
            Self::L2Book { coin, n_sig_figs, n_levels, mantissa } => {
                if !universe.contains(coin) || coin.starts_with('@') {
                    info!("Invalid subscription: coin not found");
                    return false;
                }
                if *n_levels == Some(DEFAULT_LEVELS) {
                    info!("Invalid subscription: set n_levels to this by using null");
                    return false;
                }
                let n_levels = n_levels.unwrap_or(DEFAULT_LEVELS);
                if n_levels > 100 {
                    info!("Invalid subscription: n_levels too high");
                    return false;
                }
                if let Some(n_sig_figs) = *n_sig_figs {
                    if !(2..=5).contains(&n_sig_figs) {
                        info!("Invalid subscription: sig figs aren't set correctly");
                        return false;
                    }
                    if let Some(m) = *mantissa {
                        if n_sig_figs < 5 || (m != 5 && m != 2) {
                            return false;
                        }
                    }
                } else if mantissa.is_some() {
                    info!("Invalid subscription: mantissa can not be some if sig figs are not set");
                    return false;
                }
                info!("Valid subscription");
                true
            }
            Self::L4Book { coin } => {
                if !universe.contains(coin) || coin.starts_with('@') {
                    info!("Invalid subscription: coin not found");
                    return false;
                }
                info!("Valid subscription");
                true
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "channel", content = "data")]
#[serde(rename_all = "camelCase")]
enum ServerResponse {
    SubscriptionResponse(ClientMessage),
    L2Book(UnifiedOrderbook),
    L4Book(L4Book),
    Trades(Vec<Trade>),
    Error(String),
}

#[derive(Default)]
struct SubscriptionManager {
    subscriptions: HashSet<Subscription>,
}

impl SubscriptionManager {
    fn subscribe(&mut self, sub: Subscription) -> bool {
        self.subscriptions.insert(sub)
    }

    fn unsubscribe(&mut self, sub: Subscription) -> bool {
        self.subscriptions.remove(&sub)
    }

    const fn subscriptions(&self) -> &HashSet<Subscription> {
        &self.subscriptions
    }
}

pub async fn run_websocket_server(
    address: &str,
    ignore_spot: bool,
    compression_level: u32,
    redis_url: Option<&str>,
) -> super::Result<()> {
    let (internal_message_tx, _) = channel::<Arc<InternalMessage>>(100);

    let redis_publisher = if let Some(redis_url) = redis_url {
        info!("Initializing Redis publisher");
        let config = RedisConfig::from_url(redis_url.to_string());
        match config.create_pool().await {
            Ok(pool) => {
                info!("Redis connection pool created successfully");
                let publisher = RedisPublisher::new(Arc::new(pool), "orderbook".to_string());
                info!("Redis publisher initialized successfully");
                Some(Arc::new(publisher))
            }
            Err(err) => {
                error!("Failed to initialize Redis connection pool: {}", err);
                None
            }
        }
    } else {
        info!("No Redis URL provided, L2 snapshots and deltas will not be published");
        None
    };

    let home_dir = dirs::home_dir().ok_or("Could not find home directory")?;
    let listener = {
        let internal_message_tx = internal_message_tx.clone();
        OrderBookListener::new(Some(internal_message_tx), ignore_spot, redis_publisher)
    };
    let listener = Arc::new(Mutex::new(listener));

    {
        let listener = listener.clone();
        tokio::spawn(async move {
            if let Err(err) = hl_listen(listener, home_dir).await {
                error!("Listener fatal error: {err}");
                std::process::exit(1);
            }
        });
    }

    let websocket_opts =
        yawc::Options::default().with_compression_level(yawc::CompressionLevel::new(compression_level));
    let app = Router::new().route(
        "/ws",
        get({
            let internal_message_tx = internal_message_tx.clone();
            async move |ws_upgrade| {
                ws_handler(ws_upgrade, internal_message_tx.clone(), listener.clone(), ignore_spot, websocket_opts)
            }
        }),
    );

    let tcp_listener = TcpListener::bind(address).await?;
    info!("WebSocket server running at ws://{address}");

    if let Err(err) = axum::serve(tcp_listener, app.into_make_service()).await {
        error!("Server fatal error: {err}");
        std::process::exit(2);
    }

    Ok(())
}

fn ws_handler(
    incoming: yawc::IncomingUpgrade,
    internal_message_tx: Sender<Arc<InternalMessage>>,
    listener: Arc<Mutex<OrderBookListener>>,
    ignore_spot: bool,
    websocket_opts: yawc::Options,
) -> impl IntoResponse {
    let (resp, fut) = incoming.upgrade(websocket_opts).unwrap();
    tokio::spawn(async move {
        let ws = match fut.await {
            Ok(ok) => ok,
            Err(err) => {
                log::error!("failed to upgrade websocket connection: {err}");
                return;
            }
        };

        handle_socket(ws, internal_message_tx, listener, ignore_spot).await
    });

    resp
}

async fn handle_socket(
    mut socket: WebSocket,
    internal_message_tx: Sender<Arc<InternalMessage>>,
    listener: Arc<Mutex<OrderBookListener>>,
    ignore_spot: bool,
) {
    let mut internal_message_rx = internal_message_tx.subscribe();
    let is_ready = listener.lock().await.is_ready();
    let mut manager = SubscriptionManager::default();
    let mut universe = listener.lock().await.universe().into_iter().map(|c| c.value()).collect();
    if !is_ready {
        let msg = ServerResponse::Error("Order book not ready for streaming (waiting for snapshot)".to_string());
        send_socket_message(&mut socket, msg).await;
        return;
    }
    loop {
        select! {
            recv_result = internal_message_rx.recv() => {
                match recv_result {
                    Ok(msg) => {
                        match msg.as_ref() {
                            InternalMessage::Snapshot{ l2_snapshots, time } => {
                                universe = new_universe(l2_snapshots, ignore_spot);
                                for sub in manager.subscriptions() {
                                    send_ws_data_from_snapshot(&mut socket, sub, l2_snapshots.as_ref(), *time).await;
                                }
                            },
                            InternalMessage::Fills{ batch } => {
                                let mut trades = coin_to_trades(batch);
                                for sub in manager.subscriptions() {
                                    send_ws_data_from_trades(&mut socket, sub, &mut trades).await;
                                }
                            },
                            InternalMessage::L4BookUpdates{ diff_batch, status_batch } => {
                                let mut book_updates = coin_to_book_updates(diff_batch, status_batch);
                                for sub in manager.subscriptions() {
                                    send_ws_data_from_book_updates(&mut socket, sub, &mut book_updates).await;
                                }
                            },
                        }

                    }
                    Err(err) => {
                        error!("Receiver error: {err}");
                        return;
                    }
                }
            }

            msg = socket.next() => {
                if let Some(frame) = msg {
                    match frame.opcode {
                        OpCode::Text => {
                            let text = match std::str::from_utf8(&frame.payload) {
                                Ok(text) => text,
                                Err(err) => {
                                    log::warn!("unable to parse websocket content: {err}: {:?}", frame.payload.as_ref());
                                    return;
                                }
                            };

                            info!("Client message: {text}");

                            if let Ok(value) = serde_json::from_str::<ClientMessage>(text) {
                                receive_client_message(&mut socket, &mut manager, value, &universe, listener.clone()).await;
                            }
                            else {
                                let msg = ServerResponse::Error(format!("Error parsing JSON into valid websocket request: {text}"));
                                send_socket_message(&mut socket, msg).await;
                            }
                        }
                        OpCode::Close => {
                            info!("Client disconnected");
                            return;
                        }
                        _ => {}
                    }
                } else {
                    info!("Client connection closed");
                    return;
                }
            }
        }
    }
}

async fn receive_client_message(
    socket: &mut WebSocket,
    manager: &mut SubscriptionManager,
    client_message: ClientMessage,
    universe: &HashSet<String>,
    listener: Arc<Mutex<OrderBookListener>>,
) {
    let subscription = match &client_message {
        ClientMessage::Unsubscribe { subscription } | ClientMessage::Subscribe { subscription } => subscription.clone(),
    };
    let sub = serde_json::to_string(&subscription).unwrap_or_default();
    if !subscription.validate(universe) {
        let msg = ServerResponse::Error(format!("Invalid subscription: {sub}"));
        send_socket_message(socket, msg).await;
        return;
    }
    let (word, success) = match &client_message {
        ClientMessage::Subscribe { .. } => ("", manager.subscribe(subscription)),
        ClientMessage::Unsubscribe { .. } => ("un", manager.unsubscribe(subscription)),
    };
    if success {
        let snapshot_msg = if let ClientMessage::Subscribe { subscription } = &client_message {
            let msg = subscription.handle_immediate_snapshot(listener).await;
            match msg {
                Ok(msg) => msg,
                Err(err) => {
                    manager.unsubscribe(subscription.clone());
                    let msg = ServerResponse::Error(format!("Unable to grab order book snapshot: {err}"));
                    send_socket_message(socket, msg).await;
                    return;
                }
            }
        } else {
            None
        };
        let msg = ServerResponse::SubscriptionResponse(client_message);
        send_socket_message(socket, msg).await;
        if let Some(snapshot_msg) = snapshot_msg {
            send_socket_message(socket, snapshot_msg).await;
        }
    } else {
        let msg = ServerResponse::Error(format!("Already {word}subscribed: {sub}"));
        send_socket_message(socket, msg).await;
    }
}

async fn send_socket_message(socket: &mut WebSocket, msg: ServerResponse) {
    let msg = serde_json::to_string(&msg);
    match msg {
        Ok(msg) => {
            if let Err(err) = socket.send(FrameView::text(msg)).await {
                error!("Failed to send: {err}");
            }
        }
        Err(err) => {
            error!("Server response serialization error: {err}");
        }
    }
}

fn new_universe(l2_snapshots: &L2Snapshots, ignore_spot: bool) -> HashSet<String> {
    l2_snapshots
        .as_ref()
        .iter()
        .filter_map(|(c, _)| if !c.is_spot() || !ignore_spot { Some(c.clone().value()) } else { None })
        .collect()
}

async fn send_ws_data_from_snapshot(
    socket: &mut WebSocket,
    subscription: &Subscription,
    snapshot: &HashMap<Coin, HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>,
    time: u64,
) {
    if let Subscription::L2Book { coin, n_sig_figs, n_levels, mantissa } = subscription {
        let snapshot = snapshot.get(&Coin::new(coin));
        if let Some(snapshot) =
            snapshot.and_then(|snapshot| snapshot.get(&L2SnapshotParams::new(*n_sig_figs, *mantissa)))
        {
            let n_levels = n_levels.unwrap_or(DEFAULT_LEVELS);
            let snapshot = snapshot.truncate(n_levels);
            let levels = snapshot.export_inner_snapshot();
            let l2_book = L2Book::from_l2_snapshot(coin.clone(), levels, time);
            match l2_book.to_unified(coin) {
                Ok(unified_book) => {
                    let msg = ServerResponse::L2Book(unified_book);
                    send_socket_message(socket, msg).await;
                }
                Err(err) => {
                    error!("Failed to convert L2Book to UnifiedOrderbook: {}", err);
                }
            }
        } else {
            error!("Coin {coin} not found");
        }
    }
}

fn coin_to_trades(batch: &Batch<NodeDataFill>) -> HashMap<String, Vec<Trade>> {
    let mut fills = batch.clone().events();
    let mut trades = HashMap::new();
    while fills.len() >= 2 {
        let f2 = fills.pop();
        let f1 = fills.pop();
        if let Some(f1) = f1 {
            if let Some(f2) = f2 {
                let mut fills = HashMap::new();
                fills.insert(f1.1.side, f1);
                fills.insert(f2.1.side, f2);
                let trade = Trade::from_fills(fills);
                let coin = trade.coin.clone();
                trades.entry(coin).or_insert_with(Vec::new).push(trade);
            }
        }
    }
    for list in trades.values_mut() {
        list.reverse();
    }
    trades
}

fn coin_to_book_updates(
    diff_batch: &Batch<NodeDataOrderDiff>,
    status_batch: &Batch<NodeDataOrderStatus>,
) -> HashMap<String, L4BookUpdates> {
    let diffs = diff_batch.clone().events();
    let statuses = status_batch.clone().events();
    let time = diff_batch.block_time();
    let height = diff_batch.block_number();
    let mut updates = HashMap::new();
    for diff in &diffs {
        let coin = diff.coin().value();
        updates.entry(coin).or_insert_with(|| L4BookUpdates::new(time, height)).book_diffs.push(diff.clone());
    }
    for status in statuses {
        let coin = status.order.coin.clone();
        updates.entry(coin).or_insert_with(|| L4BookUpdates::new(time, height)).order_statuses.push(status);
    }
    updates
}

async fn send_ws_data_from_book_updates(
    socket: &mut WebSocket,
    subscription: &Subscription,
    book_updates: &mut HashMap<String, L4BookUpdates>,
) {
    if let Subscription::L4Book { coin } = subscription {
        if let Some(updates) = book_updates.remove(coin) {
            let msg = ServerResponse::L4Book(L4Book::Updates(updates));
            send_socket_message(socket, msg).await;
        }
    }
}

async fn send_ws_data_from_trades(
    socket: &mut WebSocket,
    subscription: &Subscription,
    trades: &mut HashMap<String, Vec<Trade>>,
) {
    if let Subscription::Trades { coin } = subscription {
        if let Some(trades) = trades.remove(coin) {
            let msg = ServerResponse::Trades(trades);
            send_socket_message(socket, msg).await;
        }
    }
}

impl Subscription {
    async fn handle_immediate_snapshot(
        &self,
        listener: Arc<Mutex<OrderBookListener>>,
    ) -> super::Result<Option<ServerResponse>> {
        if let Self::L4Book { coin } = self {
            let snapshot = listener.lock().await.compute_snapshot();
            if let Some(TimedSnapshots { time, height, snapshot }) = snapshot {
                let snapshot =
                    snapshot.value().into_iter().filter(|(c, _)| *c == Coin::new(coin)).collect::<Vec<_>>().pop();
                if let Some((coin, snapshot)) = snapshot {
                    let snapshot =
                        snapshot.as_ref().clone().map(|orders| orders.into_iter().map(L4Order::from).collect());
                    return Ok(Some(ServerResponse::L4Book(L4Book::Snapshot {
                        coin: coin.value(),
                        time,
                        height,
                        levels: snapshot,
                    })));
                }
            }
            return Err("Snapshot Failed".into());
        }
        Ok(None)
    }
}
