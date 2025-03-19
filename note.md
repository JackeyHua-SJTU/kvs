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

## Self defined Error

The official `Error` Trait is somewhat verbose, because we need to implement `Display, Debug, and Error` Trait. 

A better tool is `Failure` crate. 
We can define our own error type, and use `Fail` derive to automatically implement `Error` trait.


reset the seek pointer to the beginning of the file


## File Manipulation

### Open a file
OpenOptions crate is definitely the best choice.

We can configure the open mode of the file, whether to create a new file or not.

A file will be created if it does not exist. Otherwise, just open it.
```Rust
let log = OpenOptions::new()
                        .append(true)
                        .create(true)
                        .read(true)
                        .open(file)?;      
```

### Read a file
I think only the buffered version is recommended, namely `BufReader`.

- Create a buffer reader from a file.
```Rust
let reader = BufReader::new(file);
```
- Set the position of next BYTE to read.
```Rust
reader.seek(SeekFrom::Start(0)); // set the position to the beginning of the file
reader.seek(SeekFrom::End(0)); // set the position to the end of the file
```
- Read a line from the file.
```Rust
let mut line = String::new();
reader.read_line(&mut line)?;
```
- Read a byte from the file.
```Rust
let mut byte = [0; 1];
reader.read(&mut byte)?;
```
- Read a buffer from the file.
```Rust
let mut buffer = vec![0; 1024];
reader.read(&mut buffer)?;
```

Please note after `seek`, the position of the file pointer will be changed. Be careful, and reset it if necessary.

### Write a file

Write will be buffered, so we need to **MANUALLY** flush the buffer to the file.

- Create a buffer writer from a file.
```Rust
let writer = BufWriter::new(file);
```
- Write a line to the file.
```Rust
writer.write_all("Hello, world!\n".as_bytes())?;
```

Always remember to flush the buffer to the file.
```Rust
writer.flush()?;
```

And, `write_all` will write all the bytes to the file, use it instead of `write`.

### File R/W
If you only need to perform limited operations on the file, you can use `File::read` and `File::write`.