use failure::Fail;
use std::{io, string::FromUtf8Error};

use crate::protocol::{GetResponse, RmResponse, SetResponse};

/// Self defined Error enum
/// TODO: Add possible error for network connection and any other failure situation
#[derive(Fail, Debug)]
pub enum KvsError {
    /// handle io error
    #[fail(display = "io error {}", _0)]
    IoError(io::Error),
    /// handle serialization error
    #[fail(display = "serde json error {}", _0)]
    SerdeError(serde_json::Error),
    /// handle query error
    #[fail(display = "Key not found")]
    KeyNotFound,
    /// Fail to load the log from disk
    #[fail(display = "log failed to load")]
    LogLoadError,
    /// Other unknown error
    #[fail(display = "unexpected command type")]
    UnexpectedType,
    #[fail(display = "{}", _0)]
    StringError(String),
    #[fail(display = "utf 8 error: {}", _0)]
    Utf8Error(FromUtf8Error),
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

impl From<String> for KvsError {
    fn from(value: String) -> Self {
        Self::StringError(value)
    }
}

impl From<FromUtf8Error> for KvsError {
    fn from(value: FromUtf8Error) -> Self {
        Self::Utf8Error(value)
    }
}

/// Type alias for Result
pub type Result<T> = std::result::Result<T, KvsError>;


impl From<Result<Option<String>>> for GetResponse {
    fn from(value: Result<Option<String>>) -> Self {
        match value {
            Ok(v) => Self::Ok(v),
            Err(e) => Self::Err(e.to_string()),
        }
    }
}

impl From<Result<()>> for SetResponse {
    fn from(value: Result<()>) -> Self {
        match value {
            Ok(_) => Self::Ok,
            Err(e) => Self::Err(e.to_string()),
        }
    }
}

impl From<Result<()>> for RmResponse {
    fn from(value: Result<()>) -> Self {
        match value {
            Ok(_) => Self::Ok,
            Err(e) => Self::Err(e.to_string()),
        }
    }
}