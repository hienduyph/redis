use std::{pin::Pin, vec};

use crate::{Command, Connection, Db, Frame, Parse, ParseError, Shutdown};
use bytes::Bytes;
use tokio::select;
use tokio::sync::broadcast;
use tokio_stream::{Stream, StreamExt, StreamMap};

use super::Unknown;

#[derive(Debug)]
pub struct Subscribe {
    channels: Vec<String>,
}

#[derive(Debug)]
pub struct Unsubscribe {
    channels: Vec<String>,
}

type Message = Pin<Box<dyn Stream<Item = Bytes> + Send>>;

impl Subscribe {
    pub(crate) fn new(channels: Vec<String>) -> Subscribe {
        Subscribe { channels }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Subscribe> {
        use ParseError::EndOfStream;

        let mut channels = vec![parse.next_string()?];

        loop {
            match parse.next_string() {
                Ok(s) => channels.push(s),

                Err(EndOfStream) => break,

                Err(err) => return Err(err.into()),
            }
        }

        Ok(Subscribe::new(channels))
    }

    pub(crate) async fn apply(
        mut self,
        db: &Db,
        dst: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> crate::Result<()> {
        let mut subscriptions = StreamMap::new();
        loop {
            for channel_name in self.channels.drain(..) {
                subscribe_to_channel(channel_name, &mut subscriptions, db, dst).await?;
            }
            // wait for the one of the following to happend
            select! {
                Some((channel_name, msg)) = subscriptions.next() => {
                    dst.write_frame(&make_message_frame(channel_name, msg)).await?;
                }

                res = dst.read_frame() => {
                    let frame = match res? {
                        Some(frame) => frame,
                        None => return Ok(()),
                    };
                    handle_command(frame, &mut self.channels, &mut subscriptions, dst).await?;
                }

                _ = shutdown.recv() => {
                    return Ok(());
                }
            };
        }
    }
    pub(crate) fn into_frame(self) -> Frame {
        let mut f = Frame::array();
        f.push_bulk(Bytes::from("subscribe".as_bytes()));

        for channel in self.channels {
            f.push_bulk(Bytes::from(channel.into_bytes()));
        }
        f
    }
}

async fn subscribe_to_channel(
    channel_name: String,
    subscriptions: &mut StreamMap<String, Message>,
    db: &Db,
    dst: &mut Connection,
) -> crate::Result<()> {
    let mut rx = db.subscribe(channel_name.clone());

    let rx = Box::pin(async_stream::stream! {
        loop {
            match rx.recv().await {
                Ok(msg) => yield msg,
                Err(broadcast::error::RecvError::Lagged(_)) => {},
                Err(_) => break,
            }
        }
    });

    subscriptions.insert(channel_name.clone(), rx);
    let resp = make_subscribe_frame(channel_name, subscriptions.len());
    dst.write_frame(&resp).await?;

    Ok(())
}
fn make_message_frame(channel_name: String, msg: Bytes) -> Frame {
    let mut f = Frame::array();
    f.push_bulk(Bytes::from_static(b"message"));
    f.push_bulk(Bytes::from(channel_name));
    f.push_bulk(msg);
    f
}

async fn handle_command(
    frame: Frame,
    subscribe_to: &mut Vec<String>,
    subscriptions: &mut StreamMap<String, Message>,
    dst: &mut Connection,
) -> crate::Result<()> {
    match Command::from_frame(frame)? {
        Command::Subscribe(sub) => {
            subscribe_to.extend(sub.channels.into_iter());
        }

        Command::Unsubscribe(mut unsubscribe) => {
            if unsubscribe.channels.is_empty() {
                unsubscribe.channels = subscriptions
                    .keys()
                    .map(|channel_name| channel_name.to_string())
                    .collect();
            }
            for channel_name in unsubscribe.channels {
                subscriptions.remove(&channel_name);

                let resp = make_unsubscribe_frame(channel_name, subscriptions.len());
                dst.write_frame(&resp).await?;
            }
        }
        command => {
            let cmd = Unknown::new(command.get_name());
            cmd.apply(dst).await?;
        }
    }
    Ok(())
}

fn make_subscribe_frame(channel_name: String, num_subs: usize) -> Frame {
    let mut f = Frame::array();
    f.push_bulk(Bytes::from_static(b"subscribe"));
    f.push_bulk(Bytes::from(channel_name));
    f.push_int(num_subs as u64);
    f
}

fn make_unsubscribe_frame(channel_name: String, num_subts: usize) -> Frame {
    let mut f = Frame::array();
    f.push_bulk(Bytes::from_static(b"unsubscribe"));
    f.push_bulk(Bytes::from(channel_name));
    f.push_int(num_subts as u64);
    f
}

impl Unsubscribe {
    pub(crate) fn new(channels: &[String]) -> Unsubscribe {
        Unsubscribe {
            channels: channels.to_vec(),
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Unsubscribe> {
        use ParseError::EndOfStream;
        let mut channels = vec![];

        loop {
            match parse.next_string() {
                Ok(s) => channels.push(s),
                Err(EndOfStream) => break,
                Err(err) => return Err(err.into()),
            }
        }

        Ok(Unsubscribe::new(&channels))
    }

    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("unsubscribe".as_bytes()));
        for channel in self.channels {
            frame.push_bulk(Bytes::from(channel.into_bytes()));
        }

        frame
    }
}
