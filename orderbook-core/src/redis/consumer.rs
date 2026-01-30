use std::io;

use bb8_redis::{
    RedisConnectionManager,
    bb8::{Pool, PooledConnection, RunError},
    redis::{AsyncCommands, Client, RedisError, RedisResult, cmd},
};
use futures_util::StreamExt;
use log::warn;
use orderbook_normaliser::models::{Exchange, UnifiedOrderbook};

pub struct RedisConsumer {
    pool: Pool<RedisConnectionManager>,
    key_prefix: String,
    redis_url: String,
}

impl RedisConsumer {
    pub async fn new(pool: Pool<RedisConnectionManager>, key_prefix: String, redis_url: String) -> Self {
        Self { pool, key_prefix, redis_url }
    }

    fn get_orderbook_key(&self, exchange: Exchange, symbol: &str) -> String {
        format!("{}:{}:{}", self.key_prefix, exchange.as_str(), symbol)
    }

    fn get_updates_channel(&self) -> String {
        format!("{}:updates", self.key_prefix)
    }

    pub async fn get_orderbook(&self, exchange: Exchange, symbol: &str) -> RedisResult<Option<UnifiedOrderbook>> {
        let key = self.get_orderbook_key(exchange, symbol);

        let mut conn: PooledConnection<'_, RedisConnectionManager> =
            self.pool.get().await.map_err(|e: RunError<RedisError>| {
                RedisError::from(io::Error::new(io::ErrorKind::Other, format!("Pool error: {}", e)))
            })?;

        let result: Option<String> = conn.get(key).await?;

        match result {
            Some(json) => {
                let deserialized: UnifiedOrderbook = serde_json::from_str(&json).map_err(|e| {
                    RedisError::from(io::Error::new(io::ErrorKind::Other, format!("Deserialization error: {}", e)))
                })?;
                Ok(Some(deserialized))
            }
            None => Ok(None),
        }
    }

    pub async fn subscribe_to_updates(&self) -> RedisResult<tokio::sync::mpsc::Receiver<(Exchange, String)>> {
        let updates_channel = self.get_updates_channel();
        let redis_url = self.redis_url.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(100);

        tokio::spawn(async move {
            loop {
                let client = Client::open(&*redis_url);
                let Some(client) = client.ok() else {
                    warn!("Failed to create Redis client, retrying...");
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    continue;
                };
                let conn_result = client.get_async_pubsub().await;
                let mut pubsub = match conn_result {
                    Ok(c) => c,
                    Err(e) => {
                        warn!("Failed to create pubsub connection: {}, retrying...", e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                        continue;
                    }
                };

                if let Err(e) = pubsub.subscribe(&updates_channel).await {
                    warn!("Failed to subscribe to channel: {}, retrying...", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    continue;
                }
                while let Some(msg) = pubsub.on_message().next().await {
                    let payload: String = match msg.get_payload() {
                        Ok(c) => c,
                        Err(e) => {
                            warn!("Failed to get message payload: {:?}", e);
                            continue;
                        }
                    };
                    if !payload.is_empty() {
                        if let Some((exchange, symbol)) = parse_exchange_symbol(&payload) {
                            if tx.send((exchange, symbol)).await.is_err() {
                                warn!("Failed to send update through channel");
                                break;
                            }
                        } else {
                            warn!("Failed to parse exchange:symbol from payload: {}", payload);
                        }
                    }
                }

                warn!("Pubsub stream ended, reconnecting...");
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        });

        Ok(rx)
    }

    pub async fn health_check(&self) -> bool {
        match self.pool.get().await {
            Ok(mut conn) => cmd("PING").query_async::<String>(&mut *conn).await.is_ok(),
            Err(_) => false,
        }
    }
}

fn parse_exchange_symbol(payload: &str) -> Option<(Exchange, String)> {
    let parts: Vec<&str> = payload.splitn(2, ':').collect();
    if parts.len() == 2 {
        let exchange = Exchange::from_str(parts[0]).ok()?;
        let symbol = parts[1].to_string();
        Some((exchange, symbol))
    } else {
        None
    }
}
