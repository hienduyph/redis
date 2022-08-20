use std::{io::ErrorKind, time::Duration};

use bytes::Bytes;
use tokio::net::{TcpStream, ToSocketAddrs};
use tracing::{debug, instrument};

use crate::{Connection, Frame, Result, cmd::{Get, Set}};

pub struct Client {
    connection: Connection,
}

pub struct Subscriber {
    client: Client,

    subscribed_channels: Vec<String>,
}

#[derive(Debug)]
pub struct Message {
    pub channel :String,
    pub content: Bytes,
}

pub async fn connect<T: ToSocketAddrs>(addr: T) -> Result<Client> {
    let socket = TcpStream::connect(addr).await?;
    let conn = Connection::new(socket);
    Ok(Client{ connection: conn })
}


impl Client {
    #[instrument(skip(self))]
    pub async fn get(&mut self, key: &str) -> Result<Option<Bytes>> {
        let frame = Get::new(key).into_frame();

        debug!(request = ?frame);

        self.connection.write_frame(&frame).await?;

        match self.read_response().await? {
            Frame::Simple(value) => Ok(Some(value.into())),
            Frame::Bulk(value) => Ok(Some(value)),
            Frame::Null => Ok(None),
            frame => Err(frame.to_error()),
        }
    }

    #[instrument(skip(self))]
    pub async fn set(&mut self, key: &str, value: Bytes) -> crate::Result<()> {
        self.set_cmd(Set::new(key, value, None)).await
    }

    #[instrument(skip(self))]
    pub async fn set_expires(&mut self, key: &str, value: Bytes, expire: Duration) -> crate::Result<()> {
        self.set_cmd(Set::new(key, value, Some(expire))).await
    }

    async fn set_cmd(&mut self, cmd: Set) -> crate::Result<()> {
        let frame = cmd.into_frame();
        debug!(request = ?frame);

        self.connection.write_frame(&frame).await?;

        match self.read_response().await? {
            Frame::Simple(resp) if resp == "OK" => Ok(()),
            frame => Err(frame.to_error()),
        }
    }

    async fn read_response(&mut self) -> Result<Frame> {
        let response = self.connection.read_frame().await?;
        debug!(?response);

        match response {
            Some(Frame::Error(msg)) => Err(msg.into()),
            Some(frame) => Ok(frame),
            None => {
                let err = std::io::Error::new(ErrorKind::ConnectionReset, "connection reset by server");
                Err(err.into())
            }
        }
    }
}
