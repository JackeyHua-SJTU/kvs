use std::io::{Read, Write};
use std::net::TcpStream;

use crate::protocol::*;

use super::error::Result;

pub fn send_and_recv(rq: Request, mut stream: TcpStream) -> Result<Option<String>> {
    let s = serde_json::to_string(&rq)?;
    stream.write_all(s.as_bytes())?;
    stream.write_all(b"\n")?;
    stream.flush()?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;

    match rq {
        Request::Get { key: _ } => {
            let result: GetResponse = serde_json::from_str(&response)?;
            match result {
                GetResponse::Ok(s) => Ok(s), 
                GetResponse::Err(e) => Err(e.into()), 
            }
        },
        Request::Set { key: _, value: _ } => {
            let result: SetResponse = serde_json::from_str(&response)?;
            match result {
                SetResponse::Ok => Ok(None), 
                SetResponse::Err(e) => Err(e.into()), 
            }
        },
        Request::Rm { key: _ } => {
            let result: RmResponse = serde_json::from_str(&response)?;
            match result {
                RmResponse::Ok => Ok(None), 
                RmResponse::Err(e) => Err(e.into()), 
            }
        },
    }
}