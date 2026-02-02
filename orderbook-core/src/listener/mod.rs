use crate::{
    HL_NODE, L2Emitter,
    cache::OrderBookCache,
    listener::directory::DirectoryListener,
    metrics::StreamingMetrics,
    orderbook::{
        Coin, Snapshot,
        multi_book::{Snapshots, load_snapshots_from_json},
    },
    prelude::*,
    redis::RedisPublisher,
    state::OrderBookState,
    types::{
        L2Book, L4Order, Level,
        inner::{InnerL4Order, InnerLevel},
        node_data::{Batch, EventSource, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
    },
};
use alloy::primitives::Address;
use fs::File;
use log::{error, info, warn};
use notify::{Event, RecursiveMode, Watcher, recommended_watcher};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet, VecDeque},
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{
        Mutex,
        broadcast::Sender,
        mpsc::{UnboundedSender, unbounded_channel},
    },
    time::{Instant, interval_at, sleep},
};
use utils::{BatchQueue, EventBatch, process_rmp_file, validate_snapshot_consistency};

pub mod cleanup;
pub mod directory;
pub mod event_buffer;
pub mod utils;

pub use cleanup::perform_cleanup;
pub use event_buffer::EventBuffer;

// WARNING - this code assumes no other file system operations are occurring in the watched directories
// if there are scripts running, this may not work as intended
pub async fn hl_listen(listener: Arc<Mutex<OrderBookListener>>, dir: PathBuf) -> Result<()> {
    let order_statuses_dir = EventSource::OrderStatuses.event_source_dir(&dir).canonicalize()?;
    let fills_dir = EventSource::Fills.event_source_dir(&dir).canonicalize()?;
    let order_diffs_dir = EventSource::OrderDiffs.event_source_dir(&dir).canonicalize()?;
    println!("Starting to monitor HL node data directories...");
    info!("Monitoring order status directory: {}", order_statuses_dir.display());
    info!("Monitoring order diffs directory: {}", order_diffs_dir.display());
    info!("Monitoring fills directory: {}", fills_dir.display());

    // monitoring the directory via the notify crate (gives file system events)
    let (fs_event_tx, mut fs_event_rx) = unbounded_channel();
    let mut watcher = recommended_watcher(move |res| {
        let fs_event_tx = fs_event_tx.clone();
        if let Err(err) = fs_event_tx.send(res) {
            error!("Error sending fs event to processor via channel: {err}");
        }
    })?;

    let ignore_spot = {
        let listener = listener.lock().await;
        listener.ignore_spot
    };

    // every so often, we fetch a new snapshot and the snapshot_fetch_task starts running.
    // Result is sent back along this channel (if error, we want to return to top level)
    let (snapshot_fetch_task_tx, mut snapshot_fetch_task_rx) = unbounded_channel::<Result<()>>();

    watcher.watch(&order_statuses_dir, RecursiveMode::Recursive)?;
    watcher.watch(&fills_dir, RecursiveMode::Recursive)?;
    watcher.watch(&order_diffs_dir, RecursiveMode::Recursive)?;
    let start = Instant::now() + Duration::from_secs(5);
    let mut ticker = interval_at(start, Duration::from_secs(10));
    loop {
        tokio::select! {
            event = fs_event_rx.recv() =>  match event {
                Some(Ok(event)) => {
                    if event.kind.is_create() || event.kind.is_modify() {
                        let new_path = &event.paths[0];
                        if new_path.starts_with(&order_statuses_dir) && new_path.is_file() {
                            listener
                                .lock()
                                .await
                                .process_update(&event, new_path, EventSource::OrderStatuses)
                                .map_err(|err| format!("Order status processing error: {err}"))?;
                        } else if new_path.starts_with(&fills_dir) && new_path.is_file() {
                            listener
                                .lock()
                                .await
                                .process_update(&event, new_path, EventSource::Fills)
                                .map_err(|err| format!("Fill update processing error: {err}"))?;
                        } else if new_path.starts_with(&order_diffs_dir) && new_path.is_file() {
                            listener
                                .lock()
                                .await
                                .process_update(&event, new_path, EventSource::OrderDiffs)
                                .map_err(|err| format!("Book diff processing error: {err}"))?;
                        }
                    }
                }
                Some(Err(err)) => {
                    error!("Watcher error: {err}");
                    return Err(format!("Watcher error: {err}").into());
                }
                None => {
                    error!("Channel closed. Listener exiting");
                    return Err("Channel closed.".into());
                }
            },
            snapshot_fetch_res = snapshot_fetch_task_rx.recv() => {
                match snapshot_fetch_res {
                    None => {
                        return Err("Snapshot fetch task sender dropped".into());
                    }
                    Some(Err(err)) => {
                        return Err(format!("Abci state reading error: {err}").into());
                    }
                    Some(Ok(())) => {}
                }
            }
            _ = ticker.tick() => {
                let listener = listener.clone();
                let snapshot_fetch_task_tx = snapshot_fetch_task_tx.clone();
                fetch_snapshot(dir.clone(), listener, snapshot_fetch_task_tx, ignore_spot);
            }
            () = sleep(Duration::from_secs(5)) => {
                let listener = listener.lock().await;
                if listener.is_ready() {
                    return Err(format!("Stream has fallen behind ({HL_NODE} failed?)").into());
                }
            }
        }
    }
}

fn fetch_snapshot(
    dir: PathBuf,
    listener: Arc<Mutex<OrderBookListener>>,
    tx: UnboundedSender<Result<()>>,
    ignore_spot: bool,
) {
    let tx = tx.clone();
    tokio::spawn(async move {
        let res = match process_rmp_file(&dir).await {
            Ok(output_fln) => {
                let state = {
                    let mut listener = listener.lock().await;
                    listener.begin_caching();
                    listener.clone_state()
                };
                let snapshot = load_snapshots_from_json::<InnerL4Order, (Address, L4Order)>(&output_fln).await;
                info!("Snapshot fetched");
                // sleep to let some updates build up.
                sleep(Duration::from_secs(1)).await;
                let mut cache = {
                    let mut listener = listener.lock().await;
                    listener.take_cache()
                };
                info!("Cache has {} elements", cache.len());
                match snapshot {
                    Ok((height, expected_snapshot)) => {
                        if let Some(mut state) = state {
                            while state.height() < height {
                                if let Some((order_statuses, order_diffs)) = cache.pop_front() {
                                    state.apply_updates(order_statuses, order_diffs)?;
                                } else {
                                    return Err::<(), Error>("Not enough cached updates".into());
                                }
                            }
                            if state.height() > height {
                                return Err("Fetched snapshot lagging stored state".into());
                            }
                            let stored_snapshot = state.compute_snapshot().snapshot;
                            info!("Validating snapshot");
                            validate_snapshot_consistency(&stored_snapshot, expected_snapshot, ignore_spot)
                        } else {
                            listener.lock().await.init_from_snapshot(expected_snapshot, height);
                            Ok(())
                        }
                    }
                    Err(err) => Err(err),
                }
            }
            Err(err) => Err(err),
        };
        let _unused = tx.send(res);
        Ok(())
    });
}

impl OrderBookListener {
    fn attempt_catch_up(&mut self, current_height: u64, target_height: u64) -> bool {
        info!("Attempting catch-up: current_height={}, target_height={}, looking for range {}-{}",
              current_height, target_height, current_height + 1, target_height);
        
        let events = self.event_buffer.get_events_in_range(current_height + 1, target_height);
        
        if events.is_empty() {
            info!("No cached events found for missing blocks {} to {}", current_height + 1, target_height);
            return false;
        }

        info!("Found {} cached events for catch-up from {} to {}", events.len(), current_height + 1, target_height);
        
        for (status_wrapper, diff_wrapper) in events {
            let status = status_wrapper.status;
            let diff = diff_wrapper.diff;
            
            info!("Applying cached event at block {}", status_wrapper.block_number);
            
            if let Some(state) = &mut self.order_book_state {
                let status_batch = Batch::new(
                    status_wrapper.local_time.clone(),
                    status_wrapper.block_time.clone(),
                    status_wrapper.block_number,
                    vec![status],
                );
                let diff_batch = Batch::new(
                    diff_wrapper.local_time.clone(),
                    diff_wrapper.block_time.clone(),
                    diff_wrapper.block_number,
                    vec![diff],
                );
                
                if state.apply_single_update(status_batch, diff_batch).is_err() {
                    info!("Failed to apply cached update during catch-up");
                    return false;
                }
            }
        }
        
        let final_height = self.order_book_state.as_ref().map(|s| s.height()).unwrap_or(current_height);
        info!("Catch-up completed: current_height={}, final_height={}, target_height={}",
              current_height, final_height, target_height);
        
        true
    }
}

pub struct OrderBookListener {
    pub ignore_spot: bool,
    max_bids: Option<usize>,
    max_asks: Option<usize>,
    streaming_mode: bool,
    allowed_coins: Vec<String>,
    fill_status_file: Option<File>,
    order_status_file: Option<File>,
    order_diff_file: Option<File>,
    order_book_state: Option<OrderBookState>,
    last_fill: Option<u64>,
    order_diff_cache: BatchQueue<NodeDataOrderDiff>,
    order_status_cache: BatchQueue<NodeDataOrderStatus>,
    fetched_snapshot_cache: Option<VecDeque<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)>>,
    internal_message_tx: Option<Sender<Arc<InternalMessage>>>,
    l2_emitter: Option<Arc<Mutex<L2Emitter>>>,
    event_buffer: EventBuffer,
    affected_coins: HashSet<Coin>,
    pending_l2_coins: Vec<Coin>,
    streaming_metrics: StreamingMetrics,
    pending_updates: Vec<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)>,
    gap_retry_count: u32,
    max_catch_up_blocks: u64,
}

impl OrderBookListener {
    pub fn new_with_streaming(
        internal_message_tx: Option<Sender<Arc<InternalMessage>>>,
        ignore_spot: bool,
        redis_publisher: Option<Arc<RedisPublisher>>,
        max_bids: Option<usize>,
        max_asks: Option<usize>,
        streaming_mode: bool,
        streaming_buffer_ms: Option<u64>,
        allowed_coins: Option<Vec<String>>,
    ) -> Self {
        let l2_emitter = redis_publisher.map(|publisher| {
            Arc::new(Mutex::new(L2Emitter::new(
                OrderBookCache::default(),
                publisher,
                100,
                streaming_mode,
                streaming_buffer_ms,
            )))
        });

        let buffer_window_ms = streaming_buffer_ms.unwrap_or(50);
        let allowed_coins =
            allowed_coins.unwrap_or_else(|| vec!["BTC".to_string(), "ETH".to_string(), "SOL".to_string()]);

        Self {
            ignore_spot,
            max_bids,
            max_asks,
            streaming_mode,
            allowed_coins,
            fill_status_file: None,
            order_status_file: None,
            order_diff_file: None,
            order_book_state: None,
            last_fill: None,
            order_diff_cache: BatchQueue::new(),
            order_status_cache: BatchQueue::new(),
            fetched_snapshot_cache: None,
            internal_message_tx,
            l2_emitter,
            event_buffer: EventBuffer::new(buffer_window_ms),
            affected_coins: HashSet::new(),
            pending_l2_coins: Vec::new(),
            streaming_metrics: StreamingMetrics::new(),
            pending_updates: Vec::new(),
            gap_retry_count: 0,
            max_catch_up_blocks: 10,
        }
    }

    pub fn new(
        internal_message_tx: Option<Sender<Arc<InternalMessage>>>,
        ignore_spot: bool,
        redis_publisher: Option<Arc<RedisPublisher>>,
    ) -> Self {
        Self::new_with_streaming(internal_message_tx, ignore_spot, redis_publisher, None, None, false, None, None)
    }

    fn clone_state(&self) -> Option<OrderBookState> {
        self.order_book_state.clone()
    }

    pub const fn is_ready(&self) -> bool {
        self.order_book_state.is_some()
    }

    pub fn universe(&self) -> HashSet<Coin> {
        self.order_book_state.as_ref().map_or(HashSet::new(), |state| state.compute_universe())
    }

    #[allow(clippy::type_complexity)]
    // pops earliest pair of cached updates that have the same timestamp if possible
    fn pop_cache(&mut self) -> Option<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)> {
        // synchronize to same block
        while let Some(t) = self.order_diff_cache.front() {
            if let Some(s) = self.order_status_cache.front() {
                match t.block_number().cmp(&s.block_number()) {
                    Ordering::Less => {
                        self.order_diff_cache.pop_front();
                    }
                    Ordering::Equal => {
                        return self
                            .order_status_cache
                            .pop_front()
                            .and_then(|t| self.order_diff_cache.pop_front().map(|s| (t, s)));
                    }
                    Ordering::Greater => {
                        self.order_status_cache.pop_front();
                    }
                }
            } else {
                break;
            }
        }
        None
    }

    fn receive_batch(&mut self, updates: EventBatch) -> Result<()> {
        match updates {
            EventBatch::Orders(batch) => {
                self.order_status_cache.push(batch);
            }
            EventBatch::BookDiffs(batch) => {
                self.order_diff_cache.push(batch);
            }
            EventBatch::Fills(batch) => {
                if self.last_fill.is_none_or(|height| height < batch.block_number()) {
                    if let Some(tx) = &self.internal_message_tx {
                        let tx = tx.clone();
                        tokio::spawn(async move {
                            let snapshot = Arc::new(InternalMessage::Fills { batch });
                            let _unused = tx.send(snapshot);
                        });
                    }
                }
            }
        }
        if self.is_ready() {
            if let Some((order_statuses, order_diffs)) = self.pop_cache() {
                self.order_book_state
                    .as_mut()
                    .map(|book| book.apply_updates(order_statuses.clone(), order_diffs.clone()))
                    .transpose()?;
                if let Some(cache) = &mut self.fetched_snapshot_cache {
                    cache.push_back((order_statuses.clone(), order_diffs.clone()));
                }
                if let Some(tx) = &self.internal_message_tx {
                    let tx = tx.clone();
                    tokio::spawn(async move {
                        let updates = Arc::new(InternalMessage::L4BookUpdates {
                            diff_batch: order_diffs,
                            status_batch: order_statuses,
                        });
                        let _unused = tx.send(updates);
                    });
                }
            }
        }
        Ok(())
    }

    fn receive_incremental(&mut self, updates: EventBatch, _event_source: EventSource) -> Result<()> {
        let _file_time = Instant::now();
        match updates {
            EventBatch::Orders(batch) => {
                let block_number = batch.block_number();
                let block_time = batch.block_time_datetime().clone();
                for status in batch.events() {
                    self.event_buffer.add_order_status(block_number, block_time.clone(), status);
                }
            }
            EventBatch::BookDiffs(batch) => {
                let block_number = batch.block_number();
                let block_time = batch.block_time_datetime().clone();
                for diff in batch.events() {
                    self.event_buffer.add_order_diff(block_number, block_time.clone(), diff);
                }
            }
            EventBatch::Fills(batch) => {
                return self.receive_batch(EventBatch::Fills(batch));
            }
        }

        self.streaming_metrics.update_buffer_sizes(self.event_buffer.status_count(), self.event_buffer.diff_count());

        self.try_process_buffered_events()?;
        let current_height = self.order_book_state.as_ref().map(|s| s.height());
        self.event_buffer.flush_old_events(current_height);

        self.maybe_log_metrics();

        Ok(())
    }

    fn try_process_buffered_events(&mut self) -> Result<()> {
        let matched = self.event_buffer.try_match_events();
        self.streaming_metrics.record_matched(matched.len());

        for (status_with_meta, diff_with_meta) in matched {
            let block_number = status_with_meta.block_number;
            let block_time = status_with_meta.block_time.clone();
            let local_time = status_with_meta.local_time.clone();
            let status = status_with_meta.status;
            let diff = diff_with_meta.diff;

            let apply_status_batch =
                Batch::new(local_time.clone(), block_time.clone(), block_number, vec![status.clone()]);
            let apply_diff_batch = Batch::new(local_time.clone(), block_time.clone(), block_number, vec![diff.clone()]);

            let current_height = self.order_book_state.as_ref().map(|s| s.height());
            
            if let Some(current_height) = current_height {
                let update_height = block_number;
                
                if update_height > current_height + 1 {
                    let gap_size = update_height - current_height - 1;
                    
                    info!("Gap detection: current_height={}, update_height={}, gap_size={}, retry_count={}, max_catch_up={}",
                          current_height, update_height, gap_size, self.gap_retry_count, self.max_catch_up_blocks);
                    
                    if gap_size <= 5 && self.gap_retry_count < 3 {
                        info!("Small gap detected: current height {}, update height {}, gap size {}. Buffering update (retry {}).", 
                              current_height, update_height, gap_size, self.gap_retry_count + 1);
                        self.pending_updates.push((apply_status_batch.clone(), apply_diff_batch.clone()));
                        self.gap_retry_count += 1;
                        continue;
                    } else if gap_size <= self.max_catch_up_blocks {
                        info!("Gap detected: current height {}, update height {}, gap size {}. Attempting catch-up.", 
                              current_height, update_height, gap_size);
                        let catch_up_successful = self.attempt_catch_up(current_height, update_height - 1);
                        
                        let updated_height = self.order_book_state.as_ref().map_or(current_height, |s| s.height());
                        
                        if catch_up_successful && updated_height + 1 == update_height {
                            info!("Catch-up successful, now at height {}", updated_height);
                            self.gap_retry_count = 0;
                        } else {
                            info!("Catch-up {} (now at height {}). Triggering snapshot refetch.", 
                                  if catch_up_successful { "partial" } else { "failed" }, updated_height);
                            self.order_book_state = None;
                            self.fetched_snapshot_cache = None;
                            self.pending_updates.clear();
                            self.gap_retry_count = 0;
                            continue;
                        }
                    } else {
                        info!("Gap detected: current height {}, update height {}, gap size {} (exceeds max catch-up {}). Triggering snapshot refetch.", 
                              current_height, update_height, gap_size, self.max_catch_up_blocks);
                        self.order_book_state = None;
                        self.fetched_snapshot_cache = None;
                        self.pending_updates.clear();
                        self.gap_retry_count = 0;
                        continue;
                    }
                }
            }
            
            if let Some(state) = &mut self.order_book_state {
                let coin = status.order.coin.clone();
                
                match state.apply_single_update(apply_status_batch, apply_diff_batch.clone()) {
                    Ok(()) => {
                        self.gap_retry_count = 0;
                        if let Some(cache) = &mut self.fetched_snapshot_cache {
                            let cache_status_batch =
                                Batch::new(local_time.clone(), block_time.clone(), block_number, vec![status.clone()]);
                            cache.push_back((cache_status_batch, apply_diff_batch));
                        }
                        if let Some(tx) = &self.internal_message_tx {
                            let message_status_batch =
                                Batch::new(local_time.clone(), block_time.clone(), block_number, vec![status]);
                            let message_diff_batch = Batch::new(local_time, block_time, block_number, vec![diff]);
                            let updates = Arc::new(InternalMessage::L4BookUpdates {
                                diff_batch: message_diff_batch,
                                status_batch: message_status_batch,
                            });
                            let tx = tx.clone();
                            tokio::spawn(async move {
                                let _unused = tx.send(updates);
                            });
                        }
                        self.affected_coins.insert(Coin::new(&coin));
                    }
                    Err(err) => {
                        error!("Error applying incremental update: {}", err);
                        self.order_book_state = None;
                        return Err(err);
                    }
                }
            }
        }

        if self.streaming_mode && !self.affected_coins.is_empty() {
            self.pending_l2_coins.extend(self.affected_coins.drain());
        }

        Ok(())
    }

    fn begin_caching(&mut self) {
        self.fetched_snapshot_cache = Some(VecDeque::new());
    }

    // take the cached updates and stop collecting updates
    fn take_cache(&mut self) -> VecDeque<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)> {
        self.fetched_snapshot_cache.take().unwrap_or_default()
    }

    fn init_from_snapshot(&mut self, snapshot: Snapshots<InnerL4Order>, height: u64) {
        info!("No existing snapshot");
        let mut new_order_book = OrderBookState::from_snapshot(snapshot, height, 0, true, self.ignore_spot);
        let mut retry = false;

        if let Some(ref mut cache) = self.fetched_snapshot_cache {
            while let Some((order_statuses, order_diffs)) = cache.pop_front() {
                let update_height = order_statuses.block_number();
                if update_height > new_order_book.height() + 1 {
                    info!("Update at block {} is too far ahead of snapshot at block {}, waiting for newer snapshot",
                          update_height, new_order_book.height());
                    retry = true;
                    break;
                }
                if new_order_book.apply_updates(order_statuses, order_diffs).is_err() {
                    info!(
                        "Failed to apply updates to this book (likely missing older updates). Waiting for next snapshot."
                    );
                    retry = true;
                    break;
                }
            }
        }

        if !retry {
            while let Some((order_statuses, order_diffs)) = self.pop_cache() {
                if new_order_book.apply_updates(order_statuses, order_diffs).is_err() {
                    info!(
                        "Failed to apply updates to this book (likely missing older updates). Waiting for next snapshot."
                    );
                    retry = true;
                    break;
                }
            }
        }

        if !retry {
            self.order_book_state = Some(new_order_book);
            
            if !self.pending_updates.is_empty() {
                info!("Applying {} pending updates after snapshot initialization", self.pending_updates.len());
                let mut pending_to_keep = Vec::new();
                for (status_batch, diff_batch) in std::mem::take(&mut self.pending_updates) {
                    let update_height = status_batch.block_number();
                    if let Some(ref mut state) = self.order_book_state {
                        if update_height > state.height() + 1 || update_height <= state.height() {
                            pending_to_keep.push((status_batch, diff_batch));
                        } else if state.apply_updates(status_batch.clone(), diff_batch.clone()).is_err() {
                            info!("Failed to apply pending update at block {}. Re-buffering.", update_height);
                            pending_to_keep.push((status_batch, diff_batch));
                        }
                    }
                }
                self.pending_updates = pending_to_keep;
            }
            
            self.gap_retry_count = 0;
            info!("Order book ready");
        }
    }

    // forcibly grab current snapshot
    pub fn compute_snapshot(&mut self) -> Option<TimedSnapshots> {
        self.order_book_state.as_mut().map(|o| o.compute_snapshot())
    }

    // prevent snapshotting mutiple times at the same height
    fn l2_snapshots(&mut self, prevent_future_snaps: bool) -> Option<(u64, L2Snapshots)> {
        self.order_book_state.as_mut().and_then(|o| o.l2_snapshots(prevent_future_snaps))
    }

    fn process_pending_l2_updates(&mut self) {
        if self.pending_l2_coins.is_empty() {
            return;
        }

        let coins: Vec<Coin> = self.pending_l2_coins.drain(..).collect();
        let block_height = self.order_book_state.as_ref().map_or(0, |s| s.height());
        let max_levels = 100;

        if let Some(state) = &self.order_book_state {
            if let Some(ref l2_emitter) = self.l2_emitter {
                let emitter_arc = l2_emitter.clone();
                let allowed_coins = self.allowed_coins.clone();
                let l2_books: Vec<(Coin, L2Book)> = coins
                    .iter()
                    .filter_map(|coin| state.get_l2_book(coin, max_levels).map(|book| (coin.clone(), book)))
                    .collect();
                let max_bids = self.max_bids;
                let max_asks = self.max_asks;

                tokio::spawn(async move {
                    let mut emitter = emitter_arc.lock().await;
                    for (coin, book) in l2_books {
                        if !allowed_coins.contains(&coin.value()) {
                            continue;
                        }

                        match book.to_unified(&coin.value(), max_bids, max_asks) {
                            Ok(unified_book) => {
                                if let Err(err) =
                                    emitter.process_coin_incremental(&coin.value(), &unified_book, block_height).await
                                {
                                    error!("Failed to publish incremental L2 data to Redis: {err}");
                                }
                            }
                            Err(err) => {
                                error!("Failed to convert L2Book to UnifiedOrderbook: {}", err);
                            }
                        }
                    }
                });
            }
        }
    }

    fn emit_and_publish_l2(&mut self, snapshot: (u64, L2Snapshots)) {
        let block_height = snapshot.0;
        let l2_snapshots = snapshot.1.clone();

        if let Some(ref l2_emitter) = self.l2_emitter {
            let emitter_arc = l2_emitter.clone();
            let max_bids = self.max_bids;
            let max_asks = self.max_asks;
            let allowed_coins = self.allowed_coins.clone();
            tokio::spawn(async move {
                let mut emitter = emitter_arc.lock().await;
                for (coin, params_map) in l2_snapshots.as_ref() {
                    if !allowed_coins.contains(&coin.value()) {
                        continue;
                    }
                    let raw_params = L2SnapshotParams { n_sig_figs: None, mantissa: None };
                    if let Some(snapshot_inner) = params_map.get(&raw_params) {
                        let levels: [Vec<Level>; 2] = snapshot_inner.clone().export_inner_snapshot();
                        let l2_book = L2Book::from_l2_snapshot(coin.value(), levels, block_height);
                        match l2_book.to_unified(&coin.value(), max_bids, max_asks) {
                            Ok(unified_book) => {
                                if let Err(err) = emitter.process_coin(&coin.value(), &unified_book, block_height).await
                                {
                                    error!("Failed to publish L2 data to Redis: {}", err);
                                }
                            }
                            Err(err) => {
                                error!("Failed to convert L2Book to UnifiedOrderbook: {}", err);
                            }
                        }
                    } else {
                        warn!("Raw L2 snapshot not found for coin: {}", coin.value());
                    }
                }
            });
        }
    }
}

impl OrderBookListener {
    fn maybe_log_metrics(&mut self) {
        if self.streaming_metrics.total_events() % 100 == 0 && self.streaming_metrics.total_events() > 0 {
            info!("{}", self.streaming_metrics.format_report());
        }
    }
}

impl OrderBookListener {
    fn process_update(&mut self, event: &Event, new_path: &PathBuf, event_source: EventSource) -> Result<()> {
        if event.kind.is_create() {
            info!("-- Event: {} created --", new_path.display());
            self.on_file_creation(new_path.clone(), event_source)?;
        }
        // Check for `Modify` event (only if the file is already initialized)
        else {
            // If we are not tracking anything right now, we treat a file update as declaring that it has been created.
            // Unfortunately, we miss the update that occurs at this time step.
            // We go to the end of the file to read for updates after that.
            if self.is_reading(event_source) {
                self.on_file_modification(event_source)?;
            } else {
                info!("-- Event: {} modified, tracking it now --", new_path.display());
                let file = self.file_mut(event_source);
                let mut new_file = File::open(new_path)?;
                new_file.seek(SeekFrom::End(0))?;
                *file = Some(new_file);
            }
        }
        Ok(())
    }
}

impl DirectoryListener for OrderBookListener {
    fn is_reading(&self, event_source: EventSource) -> bool {
        match event_source {
            EventSource::Fills => self.fill_status_file.is_some(),
            EventSource::OrderStatuses => self.order_status_file.is_some(),
            EventSource::OrderDiffs => self.order_diff_file.is_some(),
        }
    }

    fn file_mut(&mut self, event_source: EventSource) -> &mut Option<File> {
        match event_source {
            EventSource::Fills => &mut self.fill_status_file,
            EventSource::OrderStatuses => &mut self.order_status_file,
            EventSource::OrderDiffs => &mut self.order_diff_file,
        }
    }

    fn on_file_creation(&mut self, new_file: PathBuf, event_source: EventSource) -> Result<()> {
        if let Some(file) = self.file_mut(event_source).as_mut() {
            let mut buf = String::new();
            file.read_to_string(&mut buf)?;
            if !buf.is_empty() {
                self.process_data(buf, event_source)?;
            }
        }
        *self.file_mut(event_source) = Some(File::open(new_file)?);
        Ok(())
    }

    fn process_data(&mut self, data: String, event_source: EventSource) -> Result<()> {
        let total_len = data.len();
        let lines = data.lines();
        for line in lines {
            if line.is_empty() {
                continue;
            }
            let res = match event_source {
                EventSource::Fills => serde_json::from_str::<Batch<NodeDataFill>>(line).map(|batch| {
                    let height = batch.block_number();
                    (height, EventBatch::Fills(batch))
                }),
                EventSource::OrderStatuses => serde_json::from_str(line)
                    .map(|batch: Batch<NodeDataOrderStatus>| (batch.block_number(), EventBatch::Orders(batch))),
                EventSource::OrderDiffs => serde_json::from_str(line)
                    .map(|batch: Batch<NodeDataOrderDiff>| (batch.block_number(), EventBatch::BookDiffs(batch))),
            };
            let (height, event_batch) = match res {
                Ok(data) => data,
                Err(err) => {
                    if self.streaming_mode {
                        continue;
                    } else {
                        error!(
                            "{event_source} serialization error {err}, height: {:?}, line: {:?}",
                            self.order_book_state.as_ref().map(OrderBookState::height),
                            &line[..line.len().min(100)],
                        );
                        #[allow(clippy::unwrap_used)]
                        let total_len: i64 = total_len.try_into().unwrap();
                        self.file_mut(event_source).as_mut().map(|f| f.seek_relative(-total_len));
                        break;
                    }
                }
            };
            if height % 100 == 0 {
                info!("{event_source} block: {height}");
            }
            if self.streaming_mode {
                if let Err(err) = self.receive_incremental(event_batch, event_source) {
                    self.order_book_state = None;
                    return Err(err);
                }
                self.process_pending_l2_updates();
            } else {
                if let Err(err) = self.receive_batch(event_batch) {
                    self.order_book_state = None;
                    return Err(err);
                }
            }
        }
        let snapshot = self.l2_snapshots(true);
        if let Some(snapshot) = snapshot {
            let time = snapshot.0;
            let l2_snapshots = snapshot.1.clone();
            if let Some(tx) = &self.internal_message_tx {
                let tx = tx.clone();
                let l2_snapshots_clone = l2_snapshots.clone();
                tokio::spawn(async move {
                    let snapshot = Arc::new(InternalMessage::Snapshot { l2_snapshots: l2_snapshots_clone, time });
                    let _unused = tx.send(snapshot);
                });
            }
            self.emit_and_publish_l2((time, l2_snapshots));
            self.streaming_metrics.record_l2_update();
        }
        self.process_pending_l2_updates();

        Ok(())
    }
}

#[derive(Clone)]
pub struct L2Snapshots(pub HashMap<Coin, HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>);

impl L2Snapshots {
    pub const fn as_ref(&self) -> &HashMap<Coin, HashMap<L2SnapshotParams, Snapshot<InnerLevel>>> {
        &self.0
    }
}

pub struct TimedSnapshots {
    pub time: u64,
    pub height: u64,
    pub snapshot: Snapshots<InnerL4Order>,
}

// Messages sent from node data listener to websocket dispatch to support streaming
pub enum InternalMessage {
    Snapshot { l2_snapshots: L2Snapshots, time: u64 },
    Fills { batch: Batch<NodeDataFill> },
    L4BookUpdates { diff_batch: Batch<NodeDataOrderDiff>, status_batch: Batch<NodeDataOrderStatus> },
}

#[derive(Clone, Eq, PartialEq, Hash)]
pub struct L2SnapshotParams {
    pub n_sig_figs: Option<u32>,
    pub mantissa: Option<u64>,
}
