use thiserror::Error;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Parse error: {0}")]
    Parse(String),
}

pub type Result<T> = std::result::Result<T, AppError>;
