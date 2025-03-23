use serde::{Serialize, Deserialize};

use crate::error::{KvsError, Result};

/// A common request format for both server and client
/// 
/// Server deserializes the request and serialize the response.
/// Client serializes the request and deserialize the response.

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    Get { key: String },
    Set { key: String, value: String },
    Rm  { key: String },
}

/// Err will hold string
/// Server will serialize the KvsError as configured in the Fail

#[derive(Serialize, Deserialize, Debug)]
pub enum GetResponse {
    Ok(Option<String>),
    Err(String),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum SetResponse {
    Ok,
    Err(String),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum RmResponse {
    Ok,
    Err(String),
}

