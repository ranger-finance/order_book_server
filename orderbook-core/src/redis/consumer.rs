use std::io;

use crate::types::L2Book;
use bb8_redis::{
    RedisConnectionManager,
    bb8::{Pool, PooledConnection, RunError},
    redis::{AsyncCommands, Client, RedisError, RedisResult, aio::PubSub, cmd},
};
use futures_util::StreamExt;
use log::warn;

pub struct RedisConsumer {
    pool: Pool<RedisConnectionManager>,
    key_prefix: String,
}

impl RedisConsumer {
    pub async fn new(pool: Pool<RedisConnectionManager>, key_prefix: String) -> Self {
        Self { pool, key_prefix }
    }

    fn get_orderbook_key(&self, coin: &str) -> String {
        format!("{}:orderbook:{}", self.key_prefix, coin)
    }

    fn get_updates_channel(&self) -> String {
        format!("{}:updates", self.key_prefix)
    }

    pub async fn get_l2_book(&self, coin: &str) -> RedisResult<Option<L2Book>> {
        let key = self.get_orderbook_key(coin);

        let mut conn: PooledConnection<'_, RedisConnectionManager> =
            self.pool.get().await.map_err(|e: RunError<RedisError>| {
                RedisError::from(io::Error::new(io::ErrorKind::Other, format!("Pool error: {}", e)))
            })?;

        let result: Option<String> = conn.get(key).await?;

        match result {
            Some(json) => {
                let deserialized: L2Book = serde_json::from_str(&json).map_err(|e| {
                    RedisError::from(io::Error::new(io::ErrorKind::Other, format!("Deserialization error: {}", e)))
                })?;
                Ok(Some(deserialized))
            }
            None => Ok(None),
        }
    }

    pub async fn subscribe_to_updates(&self) -> RedisResult<tokio::sync::mpsc::Receiver<String>> {
        let updates_channel = self.get_updates_channel();
        let (tx, rx) = tokio::sync::mpsc::channel(100);

        tokio::spawn(async move {
            loop {
                let client = Client::open("redis://127.0.0.1/");
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
                    let coin: String = match msg.get_payload() {
                        Ok(c) => c,
                        Err(e) => {
                            warn!("Failed to get message payload: {:?}", e);
                            continue;
                        }
                    };
                    if !coin.is_empty() {
                        if tx.send(coin).await.is_err() {
                            warn!("Failed to send update through channel");
                            break;
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
