use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::TcpStream;

use crate::protocol::*;

use super::error::Result;

pub fn send_and_recv(rq: Request, stream: TcpStream) -> Result<Option<String>> {
    let s = serde_json::to_string(&rq)?;
    let mut writer = BufWriter::new(&stream);
    writer.write_all(s.as_bytes())?;
    writer.write_all(b"\n")?;
    writer.flush()?;

    let mut response = Vec::new();
    let mut reader = BufReader::new(&stream);
    reader.read_until(b'\n', &mut response)?;

    let response = String::from_utf8(response)?;

    match rq {
        Request::Get { key: _ } => {
            let result: GetResponse = serde_json::from_str(&response)?;
            match result {
                GetResponse::Ok(s) => Ok(s),
                GetResponse::Err(e) => Err(e.into()),
            }
        }
        Request::Set { key: _, value: _ } => {
            let result: SetResponse = serde_json::from_str(&response)?;
            match result {
                SetResponse::Ok => Ok(None),
                SetResponse::Err(e) => Err(e.into()),
            }
        }
        Request::Rm { key: _ } => {
            let result: RmResponse = serde_json::from_str(&response)?;
            match result {
                RmResponse::Ok => Ok(None),
                RmResponse::Err(e) => Err(e.into()),
            }
        }
    }
}
