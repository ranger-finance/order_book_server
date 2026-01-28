use crate::orderbook::Coin;
use crate::types::L2Book;
use bb8_redis::{
    RedisConnectionManager,
    bb8::{Pool, PooledConnection, RunError},
    redis::{AsyncCommands, RedisError, RedisResult, cmd},
};
use serde::Serialize;
use std::io;
use std::sync::Arc;

/// Redis publisher for orderbook snapshots
#[derive(Clone)]
pub struct RedisPublisher {
    pool: Arc<Pool<RedisConnectionManager>>,
    key_prefix: String,
}

impl RedisPublisher {
    pub fn new(pool: Arc<Pool<RedisConnectionManager>>, key_prefix: String) -> Self {
        Self { pool, key_prefix }
    }

    /// Get the Redis key for a coin's orderbook
    fn get_orderbook_key(&self, coin: &Coin) -> String {
        format!("{}:orderbook:{}", self.key_prefix, coin.value())
    }

    /// Publish an orderbook snapshot to Redis with TTL (default: 1 hour)
    pub async fn publish_l2_book(&self, coin: &Coin, book: &L2Book) -> RedisResult<()> {
        let key = self.get_orderbook_key(coin);
        let serialized = serde_json::to_string(book).map_err(|e| {
            RedisError::from(io::Error::new(io::ErrorKind::Other, format!("Serialization error: {}", e)))
        })?;

        let mut conn: PooledConnection<'_, RedisConnectionManager> =
            self.pool.get().await.map_err(|e: RunError<RedisError>| {
                RedisError::from(io::Error::new(io::ErrorKind::Other, format!("Pool error: {}", e)))
            })?;

        conn.set_ex::<_, _, ()>(key, serialized, 3600).await?;

        Ok(())
    }

    /// Get an orderbook from Redis (for testing/validation)
    pub async fn get_l2_book(&self, coin: &Coin) -> RedisResult<Option<L2Book>> {
        let key = self.get_orderbook_key(coin);

        let mut conn: PooledConnection<'_, RedisConnectionManager> =
            self.pool.get().await.map_err(|e: RunError<RedisError>| {
                RedisError::from(io::Error::new(io::ErrorKind::Other, format!("Pool error: {}", e)))
            })?;

        let result: Option<String> = conn.get(&key).await?;

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

    /// Check if Redis connection is healthy
    pub async fn health_check(&self) -> bool {
        match self.pool.get().await {
            Ok(mut conn) => cmd("PING").query_async::<String>(&mut *conn).await.is_ok(),
            Err(_) => false,
        }
    }
}
