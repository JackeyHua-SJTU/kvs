use std::{
    io::{BufRead, BufReader, BufWriter, Write},
    net::TcpStream,
};

use log::trace;

use crate::engine::KvsEngine;
use crate::{
    error::KvsError,
    protocol::{GetResponse, Request, RmResponse, SetResponse},
};

pub fn handle_stream(stream: TcpStream, engine: &mut Box<dyn KvsEngine>) {
    let mut buffer = Vec::new();
    trace!("start to retrieve info from the stream");
    let mut reader = BufReader::new(&stream);
    if let Err(e) = reader.read_until(b'\n', &mut buffer) {
        handle_error(e.into(), stream);
        return;
    }
    buffer.pop();
    let request = serde_json::from_slice::<Request>(&buffer);
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
            match serde_json::to_string(&result) {
                Ok(s) => {
                    respond(s, &stream);
                    trace!("get success");
                }
                Err(e) => {
                    handle_error(e.into(), stream);
                }
            };
        }
        Request::Set { key, value } => {
            let result = engine.set(key, value);
            trace!("engine done with result");
            let result: SetResponse = result.into();
            match serde_json::to_string(&result) {
                Ok(s) => {
                    respond(s, &stream);
                    trace!("set success");
                }
                Err(e) => {
                    handle_error(e.into(), stream);
                }
            };
        }
        Request::Rm { key } => {
            let result = engine.remove(key);
            let result: RmResponse = result.into();
            match serde_json::to_string(&result) {
                Ok(s) => {
                    respond(s, &stream);
                    trace!("remove success");
                }
                Err(e) => {
                    handle_error(e.into(), stream);
                }
            };
        }
    }
}

fn handle_error(error: KvsError, mut stream: TcpStream) {
    let err: String = error.to_string();
    trace!("an error happens: {}", err);
    stream
        .write_all(err.as_bytes())
        .expect("Error message should be sent to client successfully");
}

fn respond(resp: String, stream: &TcpStream) {
    let mut writer = BufWriter::new(stream);
    writer
        .write_all(resp.as_bytes())
        .expect("Fail to send back error message");
    writer
        .write_all(b"\n")
        .expect("Fail to send back stop sign");
    writer.flush().expect("Fail to flush the buffer writer");
}
