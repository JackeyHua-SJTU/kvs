use std::io::{Read, Write};
use std::{env, process::exit};
use std::net::TcpStream;
use log::trace;
use clap::{Parser, Subcommand};

use kvs::error::{KvsError, Result};
use kvs::protocol::*;

use kvs::client;

fn main() -> Result<()> {
    env_logger::init();

    let cli = Cli::parse();

    let stream = TcpStream::connect(cli.ip)?;
    trace!("Success: Connects to the server");

    match cli.command {
        Some(Commands::Set { key, value }) => {
            let request = Request::Set { key, value };
            client::send_and_recv(request, stream)?;
            trace!("Success set");
            exit(0);
        },
        Some(Commands::Get { key }) => {
            let request = Request::Get { key };
            let result = client::send_and_recv(request, stream)?;
            if let Some(val) = result {
                trace!("Success get");
                println!("{}", val);
            } else {
                trace!("Get: key is not in the store");
                println!("Key not found");
            }
            exit(0);
        },
        Some(Commands::Rm { key }) => {
            let request = Request::Rm { key };
            client::send_and_recv(request, stream)?;
            trace!("Success remove");
            exit(0);
        },
        None => {
            trace!("Unrecognized command");
            exit(1);
        }
    }
}

#[derive(Parser)]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(name = env!("CARGO_PKG_NAME"))]
#[command(about = env!("CARGO_PKG_DESCRIPTION"))]
struct Cli {
    #[arg(short, long = "addr", value_name = "IP-Port", default_value = "127.0.0.1:4000", global = true)]
    ip: String,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Set <key, value> pair
    Set { key: String, value: String },
    /// Search the value for key
    Get { key: String },
    /// Remove the <key, value> pair if exists
    Rm { key: String },
}
