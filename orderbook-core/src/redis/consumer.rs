use bb8_redis::{RedisConnectionManager, bb8::Pool, redis::cmd};

pub struct RedisConsumer {
    pool: Pool<RedisConnectionManager>,
    key_prefix: String,
}

impl RedisConsumer {
    pub async fn new(pool: Pool<RedisConnectionManager>, key_prefix: String) -> Self {
        Self { pool, key_prefix }
    }

    pub async fn health_check(&self) -> bool {
        match self.pool.get().await {
            Ok(mut conn) => cmd("PING").query_async::<String>(&mut *conn).await.is_ok(),
            Err(_) => false,
        }
    }
}
