# Note for `kvs` project

## Command Line Argument

> Follow the [docs](https://docs.rs/clap/4.5.32/clap/_derive/_tutorial/index.html)

There are several common command line arguments listed below.
- `-h`
- `-h <val>`
- `-h <val1> <val2>`
- `<command> <param1> <param2>`

The sample structure is as follows:
```Rust
use clap::{Parser, Subcommand};

#[derive(Parser)]
/// The following macro will automatically deal with `-V` and `--version`
#[commmand(version = "0.1")]
#[command(about = "A versatile command-line tool")]
struct Cli {
    /// Enable verbose mode with a level
    #[arg(short = 'v', long, value_name = "LEVEL")]
    verbose: Option<String>,

    /// Specify a key-value pair
    #[arg(short = 't', long, num_args = 2, value_names = ["KEY", "VALUE"])]
    tuple: Option<Vec<String>>,

    /// A simple flag
    #[arg(short = 'k', long)]
    key_flag: bool,

    /// Subcommands like 'set'
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Set a key-value pair
    Set {
        /// The key to set
        key: String,
        /// The value to associate with the key
        value: String,
    },
}

fn main() {
    let args = Cli::parse();
    /// parse
}
```
