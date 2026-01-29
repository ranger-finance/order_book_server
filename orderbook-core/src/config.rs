pub struct StreamingConfig {
    pub streaming_mode: bool,
    pub streaming_buffer_ms: u64,
}

impl StreamingConfig {
    pub fn from_env() -> Self {
        let streaming_mode =
            std::env::var("HL_STREAMING_MODE").map(|v| v == "1" || v.to_lowercase() == "true").unwrap_or(false);

        let streaming_buffer_ms =
            std::env::var("HL_STREAMING_BUFFER_MS").ok().and_then(|v| v.parse::<u64>().ok()).unwrap_or(50);

        Self { streaming_mode, streaming_buffer_ms }
    }
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self { streaming_mode: false, streaming_buffer_ms: 50 }
    }
}
