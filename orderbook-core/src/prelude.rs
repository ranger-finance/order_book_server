pub(crate) type Error = Box<dyn std::error::Error + Send + Sync>;
pub(crate) type Result<T> = std::result::Result<T, Error>;
pub(crate) use std::{fmt::Debug, fs, io};
