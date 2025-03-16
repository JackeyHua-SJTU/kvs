use std::{env, process::exit};

use clap::{Parser, Subcommand};

fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Some(Commands::Set { key, value }) => {
            eprintln!("unimplemented");
            exit(1);
        }
        Some(Commands::Get { key }) => {
            eprintln!("unimplemented");
            exit(1);
        }
        Some(Commands::Rm { key }) => {
            eprintln!("unimplemented");
            exit(1);
        }
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
