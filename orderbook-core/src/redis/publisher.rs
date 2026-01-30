use bb8_redis::{
    RedisConnectionManager,
    bb8::{Pool, PooledConnection, RunError},
    redis::{AsyncCommands, RedisError, RedisResult, cmd},
};
use orderbook_normaliser::models::{Exchange, UnifiedOrderbook};
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

    /// Get the Redis key for an orderbook (includes exchange and symbol)
    fn get_orderbook_key(&self, exchange: Exchange, symbol: &str) -> String {
        format!("{}:orderbook:{}:{}", self.key_prefix, exchange.as_str(), symbol)
    }

    /// Publish a unified orderbook snapshot to Redis with TTL (default: 1 hour)
    pub async fn publish_orderbook(
        &self,
        exchange: Exchange,
        symbol: &str,
        book: &UnifiedOrderbook,
    ) -> RedisResult<()> {
        let key = self.get_orderbook_key(exchange, symbol);
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
    pub async fn get_orderbook(&self, exchange: Exchange, symbol: &str) -> RedisResult<Option<UnifiedOrderbook>> {
        let key = self.get_orderbook_key(exchange, symbol);

        let mut conn: PooledConnection<'_, RedisConnectionManager> =
            self.pool.get().await.map_err(|e: RunError<RedisError>| {
                RedisError::from(io::Error::new(io::ErrorKind::Other, format!("Pool error: {}", e)))
            })?;

        let result: Option<String> = conn.get(&key).await?;

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

    /// Publish an update notification to Redis pub/sub
    pub async fn publish_update_notification(&self, exchange: Exchange, symbol: &str) -> RedisResult<()> {
        let channel = format!("{}:updates", self.key_prefix);
        let message = format!("{}:{}", exchange.as_str(), symbol);

        let mut conn: PooledConnection<'_, RedisConnectionManager> =
            self.pool.get().await.map_err(|e: RunError<RedisError>| {
                RedisError::from(io::Error::new(io::ErrorKind::Other, format!("Pool error: {}", e)))
            })?;

        conn.publish::<_, _, ()>(channel, message).await?;

        Ok(())
    }

    /// Check if Redis connection is healthy
    pub async fn health_check(&self) -> bool {
        match self.pool.get().await {
            Ok(mut conn) => cmd("PING").query_async::<String>(&mut *conn).await.is_ok(),
            Err(_) => false,
        }
    }
}
