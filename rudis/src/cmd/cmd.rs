use crate::{Connection, Db, Frame, Parse};

use bytes::Bytes;

#[derive(Debug)]
pub struct Cmd { }

impl Cmd {
    pub fn new() -> Self {
        Cmd { }
    }
    pub(crate) fn parse_frame(parse: &mut Parse) -> crate::Result<Cmd> {
        Ok(Self::new())
    }
    pub(crate) async fn apply(self, db: &Db) -> crate::Result<Frame> {
        Ok(Frame::Simple("OK".into()))
    }
}

