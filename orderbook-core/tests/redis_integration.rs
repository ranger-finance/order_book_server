use orderbook_core::{
    Exchange, L2Emitter, UnifiedOrderbook,
    redis::{RedisConfig, RedisPublisher},
};
use rust_decimal::Decimal;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Mutex;

async fn create_test_redis_publisher() -> Arc<RedisPublisher> {
    let config = RedisConfig::from_url("redis://localhost:6379".to_string());
    let pool = config.create_pool().await.expect("Failed to create Redis connection pool");
    let publisher = RedisPublisher::new(Arc::new(pool), "test".to_string());
    Arc::new(publisher)
}

fn create_test_orderbook(symbol: &str, time: i64) -> UnifiedOrderbook {
    let bids = BTreeMap::from([(Decimal::from(100), Decimal::from(1))]);
    let asks = BTreeMap::from([(Decimal::from(101), Decimal::from(1))]);

    UnifiedOrderbook::new(Exchange::Hyperliquid, symbol.to_string(), bids, asks, time)
}

#[tokio::test]
async fn test_redis_l2_emitter_snapshot() {
    if std::env::var("CI").is_ok() {
        return;
    }

    let publisher = create_test_redis_publisher().await;
    let symbol = "BTC";
    let book = create_test_orderbook(symbol, 100);

    publisher.publish_orderbook(Exchange::Hyperliquid, symbol, &book).await.expect("Failed to publish orderbook");

    let retrieved: Option<UnifiedOrderbook> =
        publisher.get_orderbook(Exchange::Hyperliquid, symbol).await.expect("Failed to read orderbook");

    assert!(retrieved.is_some(), "Orderbook should exist in Redis");
    let retrieved_book = retrieved.unwrap();
    assert_eq!(retrieved_book.timestamp_ms, book.timestamp_ms);
    assert_eq!(retrieved_book.exchange, book.exchange);
    assert_eq!(retrieved_book.symbol, book.symbol);
}

#[tokio::test]
async fn test_redis_l2_emitter_overwrite() {
    if std::env::var("CI").is_ok() {
        return;
    }

    let publisher = create_test_redis_publisher().await;
    let symbol = "ETH";

    let book1 = create_test_orderbook(symbol, 100);
    publisher.publish_orderbook(Exchange::Hyperliquid, symbol, &book1).await.unwrap();

    let retrieved1: Option<UnifiedOrderbook> = publisher.get_orderbook(Exchange::Hyperliquid, symbol).await.unwrap();
    assert_eq!(retrieved1.unwrap().timestamp_ms, 100);

    let book2 = create_test_orderbook(symbol, 200);
    publisher.publish_orderbook(Exchange::Hyperliquid, symbol, &book2).await.unwrap();

    let retrieved2: Option<UnifiedOrderbook> = publisher.get_orderbook(Exchange::Hyperliquid, symbol).await.unwrap();
    assert_eq!(retrieved2.unwrap().timestamp_ms, 200);
}

#[tokio::test]
async fn test_l2_emitter_integration() {
    if std::env::var("CI").is_ok() {
        return;
    }

    let publisher = create_test_redis_publisher().await;
    let symbol = "BTC";

    let emitter =
        L2Emitter::new_with_default_interval(orderbook_core::cache::OrderBookCache::default(), Arc::clone(&publisher));
    let emitter_arc = Arc::new(Mutex::new(emitter));

    let book1 = create_test_orderbook(symbol, 100);
    let book2 = create_test_orderbook(symbol, 101);

    {
        let mut emitter = emitter_arc.lock().await;
        emitter.process_coin(symbol, &book1, 100).await.unwrap();
    }

    let retrieved1: Option<UnifiedOrderbook> = publisher.get_orderbook(Exchange::Hyperliquid, symbol).await.unwrap();
    assert!(retrieved1.is_some());
    assert_eq!(retrieved1.unwrap().timestamp_ms, 100);

    {
        let mut emitter = emitter_arc.lock().await;
        emitter.process_coin(symbol, &book2, 101).await.unwrap();
    }

    let retrieved2: Option<UnifiedOrderbook> = publisher.get_orderbook(Exchange::Hyperliquid, symbol).await.unwrap();
    assert!(retrieved2.is_some());
}

#[tokio::test]
async fn test_redis_health_check() {
    let config = RedisConfig::from_url("redis://localhost:6379".to_string());

    match config.create_pool().await {
        Ok(pool) => {
            let publisher = RedisPublisher::new(Arc::new(pool), "test".to_string());
            assert!(publisher.health_check().await);
        }
        Err(_) => {}
    }
}

#[tokio::test]
async fn test_redis_ttl() {
    if std::env::var("CI").is_ok() {
        return;
    }

    let config = RedisConfig::from_url("redis://localhost:6379".to_string());
    let pool = config.create_pool().await.unwrap();
    let publisher = RedisPublisher::new(Arc::new(pool), "test_ttl".to_string());

    let symbol = "TEST_TTL";
    let book = create_test_orderbook(symbol, 100);

    publisher.publish_orderbook(Exchange::Hyperliquid, symbol, &book).await.unwrap();

    let retrieved: Option<UnifiedOrderbook> = publisher.get_orderbook(Exchange::Hyperliquid, symbol).await.unwrap();
    assert!(retrieved.is_some());

    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    let retrieved: Option<UnifiedOrderbook> = publisher.get_orderbook(Exchange::Hyperliquid, symbol).await.unwrap();
    assert!(retrieved.is_none());
}
