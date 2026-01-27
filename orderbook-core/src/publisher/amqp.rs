use lapin::{
    options::{BasicPublishOptions, QueueDeclareOptions},
    types::{FieldTable, LongString},
    BasicProperties, Channel, Connection, ConnectionProperties,
};
use log::{error, info};
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::RwLock;

const DEFAULT_MAX_RETRY_ATTEMPTS: u32 = 3;
const DEFAULT_MAX_BACKOFF_MS: u64 = 30000;

pub struct AmqpPublisher {
    connection: Option<Connection>,
    channel: Option<Channel>,
    amqp_url: String,
    queue_name: String,
}

impl AmqpPublisher {
    pub async fn new(amqp_url: String, queue_name: String) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
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

    pub async fn publish_with_retry<T>(&self, payload: &T) -> Result<bool, Box<dyn std::error::Error + Send + Sync>>
    where
        T: Serialize,
    {
        let payload_bytes = serde_json::to_vec(payload)?;
        self.publish_with_retry_bytes(&payload_bytes).await
    }

    pub async fn publish_with_retry_bytes(
        &self,
        payload: &[u8],
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
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

    pub fn is_connected(&self) -> bool {
        self.connection.is_some() && self.channel.is_some()
    }

    pub async fn health_check(&self) -> AmqpHealthStatus {
        if self.connection.is_some() && self.channel.is_some() {
            AmqpHealthStatus::Connected
        } else {
            AmqpHealthStatus::Disconnected
        }
    }

    pub async fn close(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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

pub type SharedAmqpPublisher = Arc<RwLock<Option<AmqpPublisher>>>;

pub fn shared_publisher(publisher: Option<AmqpPublisher>) -> SharedAmqpPublisher {
    Arc::new(RwLock::new(publisher))
}

pub async fn publish_to_amqp<T>(
    publisher: &SharedAmqpPublisher,
    payload: &T,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    T: Serialize,
{
    let publisher_guard = publisher.read().await;
    if let Some(publisher) = publisher_guard.as_ref() {
        publisher.publish_with_retry(payload).await?;
    }
    Ok(())
}

pub async fn publish_bytes_to_amqp(
    publisher: &SharedAmqpPublisher,
    payload: &[u8],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let publisher_guard = publisher.read().await;
    if let Some(publisher) = publisher_guard.as_ref() {
        publisher.publish_with_retry_bytes(payload).await?;
    }
    Ok(())
}

pub async fn get_amqp_health(publisher: &SharedAmqpPublisher) -> AmqpHealthStatus {
    let publisher_guard = publisher.read().await;
    if let Some(publisher) = publisher_guard.as_ref() {
        publisher.health_check().await
    } else {
        AmqpHealthStatus::Disconnected
    }
}

pub async fn is_amqp_connected(publisher: &SharedAmqpPublisher) -> bool {
    let publisher_guard = publisher.read().await;
    match publisher_guard.as_ref() {
        Some(publisher) => publisher.is_connected(),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_amqp_health_status_display() {
        assert_eq!(AmqpHealthStatus::Connected.to_string(), "connected");
        assert_eq!(AmqpHealthStatus::Disconnected.to_string(), "disconnected");
    }
}
