#![allow(unused_crate_dependencies)]
use clap::Parser;
use futures_util::{sink::SinkExt, stream::StreamExt};
use orderbook_core::{
    Exchange, redis::config::RedisConfig, redis::consumer::RedisConsumer,
};
use orderbook_normaliser::{OrderbookAnalyzer};
use orderbook_normaliser::models::OrderbookUpdate;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};
use tokio_tungstenite::tungstenite::protocol::Message;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    redis_url: Option<String>,

    #[arg(long, default_value = "8080")]
    port: u16,

    #[arg(long, default_value = "127.0.0.1")]
    address: String,
}

#[derive(Debug, Deserialize)]
struct ClientMessage {
    action: String,
    exchange: Option<String>,
    symbol: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
enum ServerMessage {
    #[serde(rename = "snapshot")]
    Snapshot { data: OrderbookUpdate },
    #[serde(rename = "subscribed")]
    Subscribed { exchange: Exchange, symbol: String },
    #[serde(rename = "unsubscribed")]
    Unsubscribed { exchange: Exchange, symbol: String },
    #[serde(rename = "error")]
    Error { message: String },
}

type ClientSender = mpsc::Sender<Message>;

#[derive(Clone)]
struct Client {
    id: String,
    symbols: Vec<String>,
    sender: ClientSender,
}

struct ClientManager {
    clients: HashMap<String, Client>,
    symbol_to_clients: HashMap<String, Vec<String>>,
}

impl ClientManager {
    fn new() -> Self {
        Self { clients: HashMap::new(), symbol_to_clients: HashMap::new() }
    }

    fn add_client(&mut self, id: String, sender: ClientSender) {
        self.clients.insert(id.clone(), Client { id: id.clone(), symbols: Vec::new(), sender });
    }

    fn remove_client(&mut self, id: &str) {
        if let Some(client) = self.clients.remove(id) {
            for symbol in client.symbols {
                if let Some(clients) = self.symbol_to_clients.get_mut(&symbol) {
                    clients.retain(|client_id| client_id != id);
                }
            }
        }
    }

    fn subscribe(&mut self, client_id: &str, symbol: String) -> bool {
        if let Some(client) = self.clients.get_mut(client_id) {
            if !client.symbols.contains(&symbol) {
                client.symbols.push(symbol.clone());
                self.symbol_to_clients.entry(symbol.clone()).or_insert_with(Vec::new).push(client_id.to_string());
            }
            true
        } else {
            false
        }
    }

    fn unsubscribe(&mut self, client_id: &str, symbol: &str) -> bool {
        if let Some(client) = self.clients.get_mut(client_id) {
            client.symbols.retain(|s| s != symbol);
            if let Some(clients) = self.symbol_to_clients.get_mut(symbol) {
                clients.retain(|id| id != client_id);
            }
            true
        } else {
            false
        }
    }

    fn get_clients_for_symbol(&self, symbol: &str) -> Vec<ClientSender> {
        self.symbol_to_clients
            .get(symbol)
            .map(|client_ids| {
                client_ids.iter().filter_map(|id| self.clients.get(id)).map(|client| client.sender.clone()).collect()
            })
            .unwrap_or_default()
    }

    fn get_client_sender(&self, client_id: &str) -> Option<ClientSender> {
        self.clients.get(client_id).map(|c| c.sender.clone())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let redis_url = args
        .redis_url
        .or_else(|| std::env::var("REDIS_URL").ok())
        .expect("Redis URL must be provided via --redis-url or REDIS_URL env var");
    let addr: SocketAddr = format!("{}:{}", args.address, args.port).parse()?;

    println!("Starting WebSocket server on {}", addr);
    println!("Connecting to Redis at {}", redis_url);

    let redis_config = RedisConfig::from_url(redis_url.clone());
    let pool = redis_config.create_pool().await?;
    let redis_consumer = Arc::new(RedisConsumer::new(pool, "orderbook".to_string(), redis_url.clone()).await);
    let client_manager = Arc::new(RwLock::new(ClientManager::new()));

    let mut update_rx = redis_consumer.subscribe_to_updates().await?;

    tokio::spawn({
        let consumer = Arc::clone(&redis_consumer);
        let manager = Arc::clone(&client_manager);
        let analyzer = OrderbookAnalyzer::with_default_config();

        async move {
            while let Some((exchange, symbol)) = update_rx.recv().await {
                if let Ok(Some(book)) = consumer.get_orderbook(exchange, &symbol).await {
                    let analysis = analyzer.analyze(&book);
                    let update = OrderbookUpdate { orderbook: book, analysis };
                    let response = ServerMessage::Snapshot { data: update };

                    let json = match serde_json::to_string(&response) {
                        Ok(j) => j,
                        Err(e) => {
                            eprintln!("Failed to serialize snapshot: {}", e);
                            continue;
                        }
                    };

                    let managers = manager.read().await;
                    for client in managers.get_clients_for_symbol(&symbol) {
                        drop(client.send(Message::Text(json.clone().into())).await);
                    }
                }
            }
        }
    });

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    println!("WebSocket server listening on {}", addr);

    let mut client_counter = 0;

    while let Ok((stream, peer_addr)) = listener.accept().await {
        println!("New connection from {}", peer_addr);

        let ws_stream = tokio_tungstenite::accept_async(stream).await?;
        let (mut write, mut read) = ws_stream.split();

        let (client_tx, mut client_rx) = mpsc::channel(100);
        let client_id = format!("client_{}", client_counter);
        client_counter += 1;
        {
            let mut manager = client_manager.write().await;
            manager.add_client(client_id.clone(), client_tx.clone());
        }

        let manager_for_task = Arc::clone(&client_manager);
        let consumer_for_task = Arc::clone(&redis_consumer);
        let analyzer_for_task = OrderbookAnalyzer::with_default_config();

        tokio::spawn(async move {
            let client_id = client_id.clone();
            let manager_for_task = Arc::clone(&manager_for_task);
            let consumer_for_task = Arc::clone(&consumer_for_task);
            loop {
                tokio::select! {
                    Some(msg) = client_rx.recv() => {
                        if write.send(msg).await.is_err() {
                            break;
                        }
                    }
                    Some(msg_result) = read.next() => {
                        match msg_result {
                            Ok(Message::Text(text)) => {
                                if let Ok(client_msg) = serde_json::from_str::<ClientMessage>(&text) {
                                    match client_msg.action.as_str() {
                                        "subscribe" => {
                                            if let (Some(exchange_str), Some(symbol)) = (client_msg.exchange, client_msg.symbol) {
                                                match Exchange::from_str(&exchange_str) {
                                                    Ok(exchange) => {
                                                        let mut manager = manager_for_task.write().await;
                                                        if manager.subscribe(&client_id, symbol.clone()) {
                                                            let response = ServerMessage::Subscribed {
                                                                exchange,
                                                                symbol: symbol.clone(),
                                                            };

                                                            if let Ok(json) = serde_json::to_string(&response) {
                                                                drop(write.send(Message::Text(json.into())).await);
                                                            }

                                                            if let Ok(Some(book)) = consumer_for_task.get_orderbook(exchange, &symbol).await {
                                                                let analysis = analyzer_for_task.analyze(&book);
                                                                let update = OrderbookUpdate { orderbook: book, analysis };
                                                                let response = ServerMessage::Snapshot {
                                                                    data: update,
                                                                };

                                                                if let Ok(json) = serde_json::to_string(&response) {
                                                                    if write.send(Message::Text(json.into())).await.is_err() {
                                                                        break;
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                    Err(e) => {
                                                        let response = ServerMessage::Error {
                                                            message: format!("Invalid exchange: {}", e),
                                                        };
                                                        if let Ok(json) = serde_json::to_string(&response) {
                                                            drop(write.send(Message::Text(json.into())).await);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        "unsubscribe" => {
                                            if let (Some(exchange_str), Some(symbol)) = (client_msg.exchange, client_msg.symbol) {
                                                match Exchange::from_str(&exchange_str) {
                                                    Ok(exchange) => {
                                                        let mut manager = manager_for_task.write().await;
                                                        if manager.unsubscribe(&client_id, &symbol) {
                                                            let response = ServerMessage::Unsubscribed {
                                                                exchange,
                                                                symbol,
                                                            };

                                                            if let Ok(json) = serde_json::to_string(&response) {
                                                                drop(write.send(Message::Text(json.into())).await);
                                                            }
                                                        }
                                                    }
                                                    Err(e) => {
                                                        let response = ServerMessage::Error {
                                                            message: format!("Invalid exchange: {}", e),
                                                        };
                                                        if let Ok(json) = serde_json::to_string(&response) {
                                                            drop(write.send(Message::Text(json.into())).await);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                            }
                            Ok(Message::Close(_)) => {
                                println!("Client {} disconnected", client_id);
                                break;
                            }
                            Err(e) => {
                                println!("WebSocket error for {}: {}", client_id, e);
                                break;
                            }
                            _ => {}
                        }
                    }
                    else => {
                        break;
                    }
                }
            }

            {
                let mut manager = manager_for_task.write().await;
                manager.remove_client(&client_id);
            }
        });
    }

    Ok(())
}
