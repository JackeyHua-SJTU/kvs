use failure::Fail;
use std::io;

/// Self defined Error enum
#[derive(Fail, Debug)]
pub enum KvsError {
    /// handle io error
    #[fail(display = "io error {}", _0)]
    IoError(io::Error),
    /// handle serialization error
    #[fail(display = "serde json error {}", _0)]
    SerdeError(serde_json::Error),
    /// handle query error
    #[fail(display = "key not found")]
    KeyNotFound,
    /// Fail to load the log from disk
    #[fail(display = "log failed to load")]
    LogLoadError,
    /// Other unknown error
    #[fail(display = "unexpected command type")]
    UnexpectedType,
}

impl From<io::Error> for KvsError {
    fn from(value: io::Error) -> Self {
        Self::IoError(value)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(value: serde_json::Error) -> Self {
        Self::SerdeError(value)
    }
}

/// Type alias for Result
pub type Result<T> = std::result::Result<T, KvsError>;
