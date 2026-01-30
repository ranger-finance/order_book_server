pub struct StreamingConfig {
    pub streaming_mode: bool,
    pub streaming_buffer_ms: u64,
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self { streaming_mode: false, streaming_buffer_ms: 50 }
    }
}
