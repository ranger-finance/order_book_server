use orderbook_core::{
    Coin,
    L2Emitter,
    redis::{RedisConfig, RedisPublisher},
    types::{L2Book, Level},
};
use std::sync::Arc;
use tokio::sync::Mutex;

async fn create_test_redis_publisher() -> Arc<RedisPublisher> {
    let config = RedisConfig::from_url("redis://localhost:6379".to_string());
    let pool = config.create_pool().await.expect("Failed to create Redis connection pool");
    let publisher = RedisPublisher::new(Arc::new(pool), "test".to_string());
    Arc::new(publisher)
}

fn create_test_orderbook(coin: &Coin, time: u64) -> L2Book {
    let levels = [
        vec![Level { px: "100.0".to_string(), sz: "1.0".to_string(), n: 1 }],
        vec![Level { px: "101.0".to_string(), sz: "1.0".to_string(), n: 1 }],
    ];
    
    L2Book::from_l2_snapshot(coin.value(), levels, time)
}

#[tokio::test]
async fn test_redis_l2_emitter_snapshot() {
    if std::env::var("CI").is_ok() {
        return;
    }

    let publisher = create_test_redis_publisher().await;
    let coin = Coin::new("BTC");
    let book = create_test_orderbook(&coin, 100);

    publisher.publish_l2_book(&coin, &book).await
        .expect("Failed to publish orderbook");

    let retrieved: Option<L2Book> = publisher.get_l2_book(&coin).await
        .expect("Failed to read orderbook");

    assert!(retrieved.is_some(), "Orderbook should exist in Redis");
    let retrieved_book = retrieved.unwrap();
    assert_eq!(retrieved_book.time, book.time);
    assert_eq!(retrieved_book.levels, book.levels);
}

#[tokio::test]
async fn test_redis_l2_emitter_overwrite() {
    if std::env::var("CI").is_ok() {
        return;
    }

    let publisher = create_test_redis_publisher().await;
    let coin = Coin::new("ETH");
    
    let book1 = create_test_orderbook(&coin, 100);
    publisher.publish_l2_book(&coin, &book1).await.unwrap();

    let retrieved1: Option<L2Book> = publisher.get_l2_book(&coin).await.unwrap();
    assert_eq!(retrieved1.unwrap().time, 100);
    
    let book2 = create_test_orderbook(&coin, 200);
    publisher.publish_l2_book(&coin, &book2).await.unwrap();

    let retrieved2: Option<L2Book> = publisher.get_l2_book(&coin).await.unwrap();
    assert_eq!(retrieved2.unwrap().time, 200);
}

#[tokio::test]
async fn test_l2_emitter_integration() {
    if std::env::var("CI").is_ok() {
        return;
    }

    let publisher = create_test_redis_publisher().await;
    let coin = Coin::new("BTC");
    
    let emitter = L2Emitter::new_with_default_interval(
        orderbook_core::cache::OrderBookCache::default(),
        Arc::clone(&publisher),
    );
    let emitter_arc = Arc::new(Mutex::new(emitter));
    
    let book1 = create_test_orderbook(&coin, 100);
    let book2 = create_test_orderbook(&coin, 101);
    
    {
        let mut emitter = emitter_arc.lock().await;
        emitter.process_coin(&coin, &book1, 100).await.unwrap();
    }

    let retrieved1: Option<L2Book> = publisher.get_l2_book(&coin).await.unwrap();
    assert!(retrieved1.is_some());
    assert_eq!(retrieved1.unwrap().time, 100);
    
    {
        let mut emitter = emitter_arc.lock().await;
        emitter.process_coin(&coin, &book2, 101).await.unwrap();
    }

    let retrieved2: Option<L2Book> = publisher.get_l2_book(&coin).await.unwrap();
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
        Err(_) => {
        }
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

    let coin = Coin::new("TEST_TTL");
    let book = create_test_orderbook(&coin, 100);

    publisher.publish_l2_book(&coin, &book).await.unwrap();

    let retrieved: Option<L2Book> = publisher.get_l2_book(&coin).await.unwrap();
    assert!(retrieved.is_some());

    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    let retrieved: Option<L2Book> = publisher.get_l2_book(&coin).await.unwrap();
    assert!(retrieved.is_none());
}
