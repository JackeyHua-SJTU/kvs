use clap::{Parser, Subcommand};
use log::trace;
use std::env;
use std::net::TcpStream;

use kvs::error::{KvsError, Result};
use kvs::protocol::*;

use kvs::client;

fn main() -> Result<()> {
    env_logger::init();

    let cli = Cli::parse();

    run(cli)?;

    Ok(())
}

/// Because we have command and arg at both time,
/// If we do not set `global = true`,
/// then `<command> --addr` is a correct input,
/// but it is not we want. We want to start parsing
/// as soon as `--addr` is met.
///
#[derive(Parser)]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(name = env!("CARGO_PKG_NAME"))]
#[command(about = env!("CARGO_PKG_DESCRIPTION"))]
struct Cli {
    #[arg(
        short,
        long = "addr",
        value_name = "IP-Port",
        default_value = "127.0.0.1:4000",
        global = true
    )]
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

fn run(cli: Cli) -> Result<()> {
    let stream = TcpStream::connect(cli.ip)?;
    trace!("Success: Connects to the server");

    match cli.command {
        Some(Commands::Set { key, value }) => {
            let request = Request::Set { key, value };
            client::send_and_recv(request, stream)?;
            trace!("Success set");
        }
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
        }
        Some(Commands::Rm { key }) => {
            let request = Request::Rm { key };
            client::send_and_recv(request, stream)?;
            trace!("Success remove");
        }
        None => {
            trace!("Unrecognized command");
            return Err(KvsError::UnexpectedType);
        }
    }
    Ok(())
}
