use rudis::{server, DEFAULT_PORT};

use structopt::StructOpt;
use tokio::net::TcpListener;
use tokio::signal;

#[tokio::main]
pub async fn main() -> rudis::Result<()> {
    tracing_subscriber::fmt::try_init()?;
    let cli = Cli::from_args();
    let port = cli.port.as_deref().unwrap_or(DEFAULT_PORT);

    let addr = format!("127.0.0.1:{}", port);
    log::info!("Listening {}", &addr);
    let listener = TcpListener::bind(&addr).await?;
    server::run(listener, signal::ctrl_c()).await
}

#[derive(StructOpt, Debug)]
#[structopt(name = "rudis-server")]
struct Cli {
    #[structopt(name = "port", long = "--port")]
    port: Option<String>,
}
