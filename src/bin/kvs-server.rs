use kvs::engine::KvsEngine;
use kvs::engine::kvs::KvStore;
use kvs::engine::sled::SledKvsEngine;

use clap::Parser;
use kvs::error::{KvsError, Result};
use log::trace;
use std::env;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::process::exit;

use kvs::server;

fn main() -> Result<()> {
    env_logger::init();

    let cli = Cli::parse();

    run(cli)?;

    Ok(())
}

#[derive(Parser)]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(name = env!("CARGO_PKG_NAME"))]
#[command(about = env!("CARGO_PKG_DESCRIPTION"))]
struct Cli {
    #[arg(
        short,
        long = "addr",
        value_name = "IP-Port",
        default_value = "127.0.0.1:4000"
    )]
    ip: String,

    #[arg(
        short,
        long = "engine",
        value_name = "ENGINE-NAME",
        default_value = "kvs"
    )]
    engine: String,
}

fn run(cli: Cli) -> Result<()> {
    let dir = env::current_dir()?;
    // We need a meta info to record the last format
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(dir.join("meta"))?;

    let mut content = String::new();
    file.read_to_string(&mut content)?;

    if !content.is_empty() && content != cli.engine {
        eprintln!(
            "Error: Previous format is {}, Current is {}",
            content, cli.engine
        );
        exit(1);
    }

    if content.is_empty() {
        file.write_all(cli.engine.as_bytes())?;
    }

    file.flush()?;

    trace!("Version of kvs-server: {}", env!("CARGO_PKG_VERSION"));
    trace!("Server Configuration:");
    trace!("\t IP:Port is {}", cli.ip);
    trace!("\t Engine type is {}", cli.engine);

    // Monitor the IP:Port and Respond
    let listener = TcpListener::bind(cli.ip)?;
    trace!("Server starts to monitor the network address");

    let mut engine: Box<dyn KvsEngine> = match cli.engine.as_str() {
        "kvs" => match KvStore::new() {
            Ok(store) => {
                trace!("Create a kv store as backend");
                Box::new(store)
            }
            Err(_) => {
                trace!("Fail to create a kvs store");
                return Err(KvsError::UnexpectedType);
            }
        },
        "sled" => match SledKvsEngine::new() {
            Ok(store) => {
                trace!("Create a sled as backend");
                Box::new(store)
            }
            Err(_) => {
                trace!("Fail to create a sled engine");
                return Err(KvsError::UnexpectedType);
            }
        },
        _ => return Err(KvsError::UnexpectedType),
    };

    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                trace!("receive a command");
                server::handle_stream(s, &mut engine);
            }
            Err(e) => {
                trace!("Fail to receive from listerner");
                return Err(e.into());
            }
        }
    }

    Ok(())
}
