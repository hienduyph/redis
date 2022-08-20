use std::{num::ParseIntError, time::Duration};

use bytes::Bytes;
use structopt::StructOpt;
use rudis::DEFAULT_PORT;

#[derive(StructOpt, Debug)]
enum Command {
    Get {
        key: String,
    },
    Set {
        key: String,
        #[structopt(parse(from_str=bytes_from_str))]
        value: Bytes,

        #[structopt(parse(try_from_str = duration_from_ms_str))]
        expires: Option<Duration>,
    },
}

#[derive(StructOpt, Debug)]
#[structopt(
    name="rudis-cli",
    version=env!("CARGO_PKG_VERSION"),
    author=env!("CARGO_PKG_AUTHORS"),
    about="rudis cmd",
)]
struct Cli {
    #[structopt(subcommand)]
    command: Command,

    #[structopt(name="hostname", long="--host", default_value="127.0.0.1")]
    host: String,

    #[structopt(name="port", long="--port", default_value=DEFAULT_PORT)]
    port: String,

}

#[tokio::main(flavor="current_thread")]
async fn main() -> rudis::Result<()> {
    tracing_subscriber::fmt::try_init()?;
    let cli = Cli::from_args();
    let addr= format!("{}:{}", cli.host, cli.port);

    let mut client = rudis::client::connect(&addr).await?;

    match cli.command {
        Command::Get { key } => {
            if let Some(value) = client.get(&key).await? {
                if let Ok(string) = std::str::from_utf8(&value) {
                    println!("\"{}\"", string);
                } else {
                    println!("{:?}", value);
                }
            } else {
                println!("(nil)");
            }
        }

        Command::Set {
            key, value, expires: None,
        } => {
            client.set(&key, value).await?;
            println!("OK");
        }

        Command::Set{
            key, value, expires: Some(expires),
        } =>{
            client.set_expires(&key, value, expires).await?;
            println!("OK");
        }
    }
    Ok(())
}

fn bytes_from_str(src: &str) -> Bytes {
    Bytes::from(src.to_string())
}

fn duration_from_ms_str(src: &str) -> Result<Duration, ParseIntError> {
    let ms= src.parse::<u64>()?;
    Ok(Duration::from_millis(ms))
}
