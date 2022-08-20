pub mod cmd;
pub use cmd::Command;

pub mod frame;
pub use frame::Frame;

mod parse;
use parse::{Parse, ParseError};

mod connection;
pub use connection::Connection;

mod db;
use db::Db;

mod rocks;

mod buffer;
pub use buffer::Buffer;

mod shutdown;
use shutdown::Shutdown;

pub mod client;

pub mod server;

pub const DEFAULT_PORT: &str = "6379";

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub type Result<T> = std::result::Result<T, Error>;
