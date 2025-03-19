use std::{env, process::exit};

use clap::{Parser, Subcommand};

use kvs::{
    KvStore,
    error::{KvsError, Result},
};

fn main() -> Result<()> {
    let cli = Cli::parse();

    let mut kvs = KvStore::new()?;

    match cli.command {
        Some(Commands::Set { key, value }) => match kvs.set(key, value) {
            Ok(_) => exit(0),
            Err(e) => {
                eprintln!("{:?}", e);
                exit(1);
            }
        },
        Some(Commands::Get { key }) => match kvs.get(key) {
            Ok(None) => {
                println!("Key not found");
                exit(0)
            }
            Ok(Some(v)) => {
                println!("{}", v);
                exit(0);
            }
            Err(e) => {
                eprintln!("{:?}", e);
                exit(1)
            }
        },
        Some(Commands::Rm { key }) => match kvs.remove(key) {
            Ok(_) => exit(0),
            Err(KvsError::KeyNotFound) => {
                println!("Key not found");
                exit(1)
            }
            Err(e) => {
                println!("{:?}", e);
                exit(1)
            }
        },
        None => {
            exit(1);
        }
    }
}

#[derive(Parser)]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(name = env!("CARGO_PKG_NAME"))]
#[command(about = env!("CARGO_PKG_DESCRIPTION"))]
struct Cli {
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
