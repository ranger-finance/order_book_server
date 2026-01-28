use bb8_redis::{bb8::Pool, RedisConnectionManager};
use std::io;
use std::time::Duration;

/// Redis configuration
#[derive(Clone, Debug)]
pub struct RedisConfig {
    pub url: String,
    pub connection_timeout: Duration,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            url: "redis://localhost:6379".to_string(),
            connection_timeout: Duration::from_secs(10),
        }
    }
}

impl RedisConfig {
    pub fn from_url(url: String) -> Self {
        Self {
            url,
            ..Default::default()
        }
    }

    /// Create a Redis connection (simple, no pooling)
    pub async fn create_connection(&self) -> Result<bb8_redis::redis::aio::MultiplexedConnection, bb8_redis::redis::RedisError> {
        let client = bb8_redis::redis::Client::open(self.url.as_str())?;
        client.get_multiplexed_async_connection().await
    }

    /// Create a connection pool using bb8
    pub async fn create_pool(&self) -> Result<Pool<RedisConnectionManager>, bb8_redis::redis::RedisError> {
        let manager = RedisConnectionManager::new(self.url.as_str())
            .map_err(|e| {
                bb8_redis::redis::RedisError::from(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Failed to create Redis connection manager: {}", e),
                ))
            })?;
        
        let pool = Pool::builder()
            .max_size(20)
            .connection_timeout(self.connection_timeout)
            .build(manager)
            .await
            .map_err(|e| {
                bb8_redis::redis::RedisError::from(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Failed to build Redis connection pool: {}", e),
                ))
            })?;
        
        Ok(pool)
    }
}
