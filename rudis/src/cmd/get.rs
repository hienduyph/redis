use crate::{Connection, Db, Frame, Parse};

use bytes::Bytes;
use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Get {
    key: String,
}

impl Get {
    pub fn new(key: impl ToString) -> Self {
        Get {
            key: key.to_string(),
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn parse_frame(parse: &mut Parse) -> crate::Result<Get> {
        // The `GET` string has already been consumed. The next value is the name of the key to
        // get. If the next value is not a string or the input is fully consumed, the an error is
        // returned!
        let key = parse.next_string()?;

        Ok(Get { key })
    }


    /// Apply the Get command into the db instance
    ///
    /// The response is written into `dst`. This is called by the server in otrder to execute a
    /// received command
    #[instrument(skip(self, db))]
    pub(crate) async fn apply(self, db: &Db) -> crate::Result<Frame> {
        let response = if let Some(value) = db.get(&self.key) {
            Frame::Bulk(value)
        } else {
            Frame::Null
        };
        Ok(response)
    }

    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();

        frame.push_bulk(Bytes::from("get".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame
    }
}
