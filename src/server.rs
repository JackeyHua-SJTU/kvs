use std::{io::{BufRead, BufReader, Read, Write}, net::TcpStream};

use log::trace;

use crate::{error::{KvsError, Result}, protocol::{GetResponse, Request, RmResponse, SetResponse}};
use crate::engine::KvsEngine;

pub fn handle_stream(mut stream: TcpStream, mut engine: &mut Box<dyn KvsEngine>) {
    let mut buffer = Vec::new();
    trace!("start to retrieve info from the stream");
    let mut reader = BufReader::new(&stream);
    if let Err(e) = reader.read_until(b'\n', &mut buffer) {
        handle_error(e.into(), stream);
        return;
    }
    buffer.pop();
    let request= serde_json::from_slice::<Request>(&buffer);
    let request = match request {
        Ok(r) => r,
        Err(e) => {
            handle_error(e.into(), stream);
            return;
        }
    };

    match request {
        Request::Get { key } => {
            let result = engine.get(key);
            let result: GetResponse = result.into();
            let result = match serde_json::to_string(&result) {
                Ok(s) => s,
                Err(e) => {
                    handle_error(e.into(), stream);
                    return;
                }
            };
            stream.write_all(result.as_bytes())
                    .expect("Error message should be sent to client successfully");
            trace!("get success");
        },
        Request::Set { key, value } => {
            let result = engine.set(key, value);
            trace!("engine done with result");
            let result: SetResponse = result.into();
            let result = match serde_json::to_string(&result) {
                Ok(s) => s,
                Err(e) => {
                    handle_error(e.into(), stream);
                    return;
                }
            };
            trace!("get the set response successfully");
            stream.write_all(result.as_bytes())
                    .expect("Error message should be sent to client successfully");
            trace!("set success");
        },
        Request::Rm { key } => {
            let result = engine.remove(key);
            let result: RmResponse = result.into();
            let result = match serde_json::to_string(&result) {
                Ok(s) => s,
                Err(e) => {
                    handle_error(e.into(), stream);
                    return;
                }
            };
            stream.write_all(result.as_bytes())
                    .expect("Error message should be sent to client successfully");
            trace!("remove success");
        }
    }

    stream.shutdown(std::net::Shutdown::Write).expect("fail to shut down");
}

fn handle_error(error: KvsError, mut stream: TcpStream) {
    let err: String = error.to_string();
    trace!("an error happens: {}", err);
    stream.write_all(err.as_bytes())
        .expect("Error message should be sent to client successfully");
}