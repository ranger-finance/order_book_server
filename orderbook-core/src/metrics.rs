use std::collections::VecDeque;
use std::time::Instant;

#[derive(Clone)]
pub struct StreamingMetrics {
    event_latencies: VecDeque<u64>,
    max_samples: usize,
    buffer_sizes: (usize, usize),
    total_events: u64,
    matched_events: u64,
    l2_update_count: u64,
    last_l2_update: Option<Instant>,
}

impl StreamingMetrics {
    pub fn new() -> Self {
        Self::with_capacity(1000)
    }

    pub fn with_capacity(max_samples: usize) -> Self {
        Self {
            event_latencies: VecDeque::with_capacity(max_samples),
            max_samples,
            buffer_sizes: (0, 0),
            total_events: 0,
            matched_events: 0,
            l2_update_count: 0,
            last_l2_update: None,
        }
    }

    pub fn record_latency(&mut self, latency_ms: u64) {
        if self.event_latencies.len() >= self.max_samples {
            self.event_latencies.pop_front();
        }
        self.event_latencies.push_back(latency_ms);
    }

    pub fn record_event_processed(&mut self, file_time: Instant, process_time: Instant) {
        let latency = process_time.duration_since(file_time).as_millis() as u64;
        self.record_latency(latency);
        self.total_events += 1;
    }

    pub fn update_buffer_sizes(&mut self, status_count: usize, diff_count: usize) {
        self.buffer_sizes = (status_count, diff_count);
    }

    pub fn record_matched(&mut self, count: usize) {
        self.matched_events += count as u64;
    }

    pub fn record_l2_update(&mut self) {
        self.l2_update_count += 1;
        self.last_l2_update = Some(Instant::now());
    }

    pub fn avg_latency_ms(&self) -> Option<f64> {
        if self.event_latencies.is_empty() {
            return None;
        }
        let sum: u64 = self.event_latencies.iter().sum();
        Some(sum as f64 / self.event_latencies.len() as f64)
    }

    pub fn p95_latency_ms(&self) -> Option<u64> {
        if self.event_latencies.is_empty() {
            return None;
        }
        let mut sorted: Vec<u64> = self.event_latencies.iter().copied().collect();
        sorted.sort_unstable();
        let idx = (sorted.len() as f64 * 0.95) as usize;
        sorted.get(idx).copied()
    }

    pub fn match_rate(&self) -> f64 {
        if self.total_events == 0 {
            return 0.0;
        }
        self.matched_events as f64 / self.total_events as f64
    }

    pub fn buffer_sizes(&self) -> (usize, usize) {
        self.buffer_sizes
    }

    pub fn total_events(&self) -> u64 {
        self.total_events
    }

    pub fn l2_update_count(&self) -> u64 {
        self.l2_update_count
    }

    pub fn format_report(&self) -> String {
        format!(
            "StreamingMetrics: avg_latency={:.2}ms, p95_latency={:?}ms, \
             events={}, matched={:.1}%, buffers=(status:{}, diff:{}), \
             l2_updates={}",
            self.avg_latency_ms().unwrap_or(0.0),
            self.p95_latency_ms(),
            self.total_events,
            self.match_rate() * 100.0,
            self.buffer_sizes.0,
            self.buffer_sizes.1,
            self.l2_update_count
        )
    }
}

impl Default for StreamingMetrics {
    fn default() -> Self {
        Self::new()
    }
}
