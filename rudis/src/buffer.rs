use bytes::Bytes;
use tokio::sync::mpsc::{Sender};
use tokio::sync::oneshot;

#[derive(Debug)]
enum Command {
    Get(String),
    Set(String, Bytes),
}

type Message = (Command, oneshot::Sender<crate::Result<Option<Bytes>>>);

pub struct Buffer {
    tx: Sender<Message>,
}
