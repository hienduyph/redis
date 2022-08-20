use crate::{Connection, Db, Frame, Parse};

use bytes::Bytes;

/// Posts a mesasge to a given channel.
///
/// Send a message into a channel without any knowledge of individual consumers
///
/// Channel names has no relation to the key-value namespace. Publishing on a channel named "food"
/// has no relation to setting the "foo" key
#[derive(Debug)]
pub struct Publish {
    channel: String,
    message: Bytes,
}

impl Publish {
    /// Create a new `Publish` command which sends `message` on `channel`
    pub(crate) fn new(channel: impl ToString, mesasge: Bytes) -> Publish {
        Publish {
            channel: channel.to_string(),
            message: mesasge,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Publish> {
        let channel = parse.next_string()?;
        let mesasge = parse.next_bytes()?;
        Ok(Publish::new(channel, mesasge))
    }

    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let num_subs = db.publish(&self.channel, self.message);

        let resp = Frame::Integer(num_subs as u64);
        dst.write_frame(&resp).await?;
        Ok(())
    }

    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("publish".as_bytes()));
        frame.push_bulk(Bytes::from(self.channel.into_bytes()));
        frame.push_bulk(self.message);
        frame
    }
}
