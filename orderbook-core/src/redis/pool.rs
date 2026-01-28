use crate::redis::config::RedisConfig;
use bb8_redis::{
    RedisConnectionManager,
    bb8::{Pool, State, PooledConnection, RunError},
    redis::cmd,
};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

pub struct RedisPool {
    pool: Pool<RedisConnectionManager>,
    metrics: Arc<PoolMetrics>,
}

#[derive(Debug, Default)]
pub struct PoolMetrics {
    total_acquisitions: AtomicU64,
    total_timeouts: AtomicU64,
    total_retries: AtomicU64,
}

impl PoolMetrics {
    pub fn total_acquisitions(&self) -> u64 {
        self.total_acquisitions.load(Ordering::Relaxed)
    }

    pub fn total_timeouts(&self) -> u64 {
        self.total_timeouts.load(Ordering::Relaxed)
    }

    pub fn total_retries(&self) -> u64 {
        self.total_retries.load(Ordering::Relaxed)
    }
}

impl RedisPool {
    pub async fn new(config: &RedisConfig) -> Result<Self, bb8_redis::redis::RedisError> {
        let pool = config.create_pool().await?;
        Ok(Self { pool, metrics: Arc::default() })
    }

    pub async fn get(
        &self,
    ) -> Result<PooledConnection<'_, RedisConnectionManager>, RunError<bb8_redis::redis::RedisError>> {
        self.metrics.total_acquisitions.fetch_add(1, Ordering::Relaxed);
        self.pool.get().await
    }

    pub async fn health_check(&self) -> bool {
        match self.pool.get().await {
            Ok(mut conn) => cmd("PING").query_async::<String>(&mut *conn).await.is_ok(),
            Err(_) => {
                self.metrics.total_timeouts.fetch_add(1, Ordering::Relaxed);
                false
            }
        }
    }

    pub fn increment_retries(&self) {
        self.metrics.total_retries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn state(&self) -> State {
        self.pool.state()
    }

    pub fn metrics(&self) -> &Arc<PoolMetrics> {
        &self.metrics
    }

    pub fn inner_pool(&self) -> &Pool<RedisConnectionManager> {
        &self.pool
    }
}

pub struct RetryPolicy {
    max_retries: usize,
    base_delay: Duration,
    max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self { max_retries: 3, base_delay: Duration::from_millis(100), max_delay: Duration::from_secs(5) }
    }
}

impl RetryPolicy {
    pub fn new(max_retries: usize, base_delay: Duration, max_delay: Duration) -> Self {
        Self { max_retries, base_delay, max_delay }
    }

    pub fn exponential_backoff(&self, attempt: usize) -> Duration {
        let delay_ms = self.base_delay.as_millis() as u64 * 2u64.pow(attempt.min(10) as u32);
        let delay = Duration::from_millis(delay_ms.min(self.max_delay.as_millis() as u64));
        delay
    }

    pub async fn execute<F, Fut, T, E>(&self, mut operation: F) -> Result<T, E>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let mut last_error = None;
        for attempt in 0..=self.max_retries {
            match operation().await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    last_error = Some(e);
                    if attempt < self.max_retries {
                        let delay = self.exponential_backoff(attempt);
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }
        Err(last_error.unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_retry_policy() {
        let policy = RetryPolicy::new(3, Duration::from_millis(100), Duration::from_secs(1));
        let delay_0 = policy.exponential_backoff(0);
        let delay_1 = policy.exponential_backoff(1);
        let delay_2 = policy.exponential_backoff(2);

        assert_eq!(delay_0, Duration::from_millis(100));
        assert_eq!(delay_1, Duration::from_millis(200));
        assert_eq!(delay_2, Duration::from_millis(400));
    }

    #[tokio::test]
    async fn test_retry_policy_max_delay() {
        let policy = RetryPolicy::new(10, Duration::from_millis(100), Duration::from_millis(300));
        let delay_10 = policy.exponential_backoff(10);
        assert_eq!(delay_10, Duration::from_millis(300));
    }

    #[tokio::test]
    async fn test_retry_execute_success() {
        let policy = RetryPolicy::default();
        let mut attempts = 0;
        let result = policy
            .execute(|| {
                attempts += 1;
                async { Ok::<_, ()>(42) }
            })
            .await
            .unwrap();
        assert_eq!(result, 42);
        assert_eq!(attempts, 1);
    }

    #[tokio::test]
    async fn test_retry_execute_failure() {
        let policy = RetryPolicy::new(2, Duration::from_millis(10), Duration::from_secs(1));
        let mut attempts = 0;
        let result = policy
            .execute(|| {
                attempts += 1;
                async { Err::<i32, ()>(()) }
            })
            .await;
        assert!(result.is_err());
        assert_eq!(attempts, 3);
    }
}
