use crate::consumer::L2Delta;
use crate::types::amqp::{L2DeltaMessage, L2SnapshotMessage};
use lapin::{
    options::{BasicPublishOptions, QueueDeclareOptions},
    types::{FieldTable, LongString},
    BasicProperties, Channel, Connection, ConnectionProperties,
};
use log::{error, info};
use serde::Serialize;

const DEFAULT_MAX_RETRY_ATTEMPTS: u32 = 3;
const DEFAULT_MAX_BACKOFF_MS: u64 = 30000;

struct SingleQueueAmqpPublisher {
    connection: Option<Connection>,
    channel: Option<Channel>,
    amqp_url: String,
    queue_name: String,
}

impl SingleQueueAmqpPublisher {
    async fn new(amqp_url: String, queue_name: String) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut publisher =
            Self { connection: None, channel: None, amqp_url: amqp_url.clone(), queue_name: queue_name.clone() };
        publisher.establish_connection().await?;
        Ok(publisher)
    }

    async fn establish_connection(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut attempts: u32 = 0;
        let max_attempts: u32 = 30;
        let mut last_err: Option<Box<dyn std::error::Error + Send + Sync>> = None;

        while attempts < max_attempts {
            attempts += 1;

            match Connection::connect(&self.amqp_url, ConnectionProperties::default()).await {
                Ok(conn) => match conn.create_channel().await {
                    Ok(channel) => {
                        let mut queue_args = FieldTable::default();
                        queue_args.insert("x-dead-letter-exchange".into(), LongString::from("dlx_exchange").into());
                        queue_args.insert("x-dead-letter-routing-key".into(), LongString::from("dlx_key").into());
                        match channel
                            .queue_declare(
                                &self.queue_name,
                                QueueDeclareOptions { durable: true, ..Default::default() },
                                queue_args,
                            )
                            .await
                        {
                            Ok(_) => {
                                info!("AMQP connection established to queue: {}", self.queue_name);
                                self.connection = Some(conn);
                                self.channel = Some(channel);
                                return Ok(());
                            }
                            Err(e) => {
                                last_err = Some(e.into());
                            }
                        }
                    }
                    Err(e) => {
                        last_err = Some(e.into());
                    }
                },
                Err(e) => {
                    last_err = Some(e.into());
                }
            }

            let backoff_ms = (1000 * 2u64.pow((attempts - 1).min(5))).min(DEFAULT_MAX_BACKOFF_MS);
            info!("AMQP connection attempt {} failed, retrying in {}ms...", attempts, backoff_ms);
            tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
        }

        Err(last_err.unwrap_or_else(|| "Failed to establish AMQP connection".into()))
    }

    async fn publish_with_retry<T>(&self, payload: &T) -> Result<bool, Box<dyn std::error::Error + Send + Sync>>
    where
        T: Serialize,
    {
        let payload_bytes = serde_json::to_vec(payload)?;
        self.publish_with_retry_bytes(&payload_bytes).await
    }

    async fn publish_with_retry_bytes(&self, payload: &[u8]) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let mut attempts: u32 = 0;
        let max_attempts = DEFAULT_MAX_RETRY_ATTEMPTS;
        let mut last_err: Option<Box<dyn std::error::Error + Send + Sync>> = None;

        while attempts < max_attempts {
            attempts += 1;

            if let Some(channel) = &self.channel {
                match channel
                    .basic_publish(
                        "",
                        &self.queue_name,
                        BasicPublishOptions::default(),
                        payload,
                        BasicProperties::default(),
                    )
                    .await
                {
                    Ok(confirm_fut) => match confirm_fut.await {
                        Ok(confirm) => return Ok(!confirm.is_nack()),
                        Err(e) => {
                            last_err = Some(Box::new(e.clone()));
                            error!("AMQP publish confirm failed: {}", e);
                        }
                    },
                    Err(e) => {
                        last_err = Some(Box::new(e.clone()));
                        error!("AMQP publish failed: {}", e);
                    }
                }
            }

            if attempts < max_attempts {
                let backoff_ms = (100 * 2u64.pow((attempts - 1).min(5))).min(DEFAULT_MAX_BACKOFF_MS);
                info!("AMQP publish attempt {} failed, retrying in {}ms...", attempts, backoff_ms);
                tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
            }
        }

        Err(last_err.unwrap_or_else(|| "AMQP publish failed after retries".into()))
    }

    fn is_connected(&self) -> bool {
        self.connection.is_some() && self.channel.is_some()
    }

    async fn health_check(&self) -> AmqpHealthStatus {
        if self.connection.is_some() && self.channel.is_some() {
            AmqpHealthStatus::Connected
        } else {
            AmqpHealthStatus::Disconnected
        }
    }

    async fn close(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(channel) = &self.channel {
            channel.close(200, "Shutdown").await?;
        }
        if let Some(conn) = &self.connection {
            conn.close(200, "Shutdown").await?;
        }
        self.channel.take();
        self.connection.take();
        info!("AMQP connection closed");
        Ok(())
    }
}

pub enum AmqpHealthStatus {
    Connected,
    Disconnected,
}

impl std::fmt::Display for AmqpHealthStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connected => write!(f, "connected"),
            Self::Disconnected => write!(f, "disconnected"),
        }
    }
}

pub struct AmqpPublisher {
    snapshot_publisher: SingleQueueAmqpPublisher,
    delta_publisher: SingleQueueAmqpPublisher,
}

impl AmqpPublisher {
    pub async fn new(amqp_url: String) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let snapshot_publisher = SingleQueueAmqpPublisher::new(amqp_url.clone(), "hl.l2.snapshot".to_string()).await?;
        let delta_publisher = SingleQueueAmqpPublisher::new(amqp_url, "hl.l2.delta".to_string()).await?;

        info!("L2 AMQP publisher initialized with snapshot and delta queues");

        Ok(Self { snapshot_publisher, delta_publisher })
    }

    pub async fn publish_snapshot(
        &self,
        snapshot: &L2SnapshotMessage,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.snapshot_publisher.publish_with_retry(snapshot).await?;
        Ok(())
    }

    pub async fn publish_delta(&self, delta: &L2DeltaMessage) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.delta_publisher.publish_with_retry(delta).await?;
        Ok(())
    }

    pub async fn publish_delta_from_l2_delta(
        &self,
        delta: &L2Delta,
        source: String,
        from_sequence: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let bids: Vec<(String, String)> = delta.bids.iter().map(|level| (level.px.clone(), level.sz.clone())).collect();
        let asks: Vec<(String, String)> = delta.asks.iter().map(|level| (level.px.clone(), level.sz.clone())).collect();

        let delta_message = L2DeltaMessage {
            coin: delta.coin.clone(),
            timestamp: delta.timestamp,
            sequence: delta.sequence,
            bids,
            asks,
            from_sequence,
            source,
        };

        self.publish_delta(&delta_message).await
    }

    pub async fn health_check(&self) -> (AmqpHealthStatus, AmqpHealthStatus) {
        let snapshot_status = self.snapshot_publisher.health_check().await;
        let delta_status = self.delta_publisher.health_check().await;
        (snapshot_status, delta_status)
    }

    pub fn is_connected(&self) -> bool {
        self.snapshot_publisher.is_connected() && self.delta_publisher.is_connected()
    }

    pub async fn close(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.snapshot_publisher.close().await?;
        self.delta_publisher.close().await?;
        info!("L2 AMQP publisher closed");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_l2_publisher_creation() {
        // This test would require a mock AMQP server
        // For now, we just verify the structure compiles
        assert!(true);
    }

    #[test]
    fn test_amqp_health_status_display() {
        assert_eq!(AmqpHealthStatus::Connected.to_string(), "connected");
        assert_eq!(AmqpHealthStatus::Disconnected.to_string(), "disconnected");
    }
}
