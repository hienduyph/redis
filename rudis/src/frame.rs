use bytes::{Buf, Bytes};

use std::convert::TryInto;
use std::io::Cursor;

// A Frame in redis protocol
#[derive(Clone, Debug)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(u64),
    Bulk(Bytes),
    Null,
    Array(Vec<Frame>),
    Pipe(Vec<Frame>),
}

#[derive(Debug)]

pub enum Error {
    // Not enough data is available to parse a message
    Incomplete,
    // Invalid message encoding
    Other(crate::Error),
}

impl Frame {
    pub(crate) fn array() -> Frame {
        Frame::Array(vec![])
    }

    pub(crate) fn new_array(arr: Vec<Frame>) -> Frame {
        Frame::Array(arr)
    }

    /// Push a bulk frame into the array, `self` must be an Array frame
    /// # Panics
    ///
    /// panics if `self` is not an array
    pub(crate) fn push_bulk(&mut self, bytes: Bytes) {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Bulk(bytes));
            }
            _ => panic!("not an array frame"),
        }
    }

    pub(crate) fn push_int(&mut self, value: u64) {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Integer(value));
            }
            _ => panic!("not an array frame"),
        }
    }

    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        match get_u8(src)? {
            b'+' => {
                get_line(src)?;
                Ok(())
            }
            b'-' => {
                get_line(src)?;
                Ok(())
            }
            b':' => {
                get_decimal(src)?;
                Ok(())
            }
            b'$' => {
                if b'-' == peek_u8(src)? {
                    skip(src, 4)
                } else {
                    let len: usize = get_decimal(src)?.try_into()?;
                    skip(src, len + 2)
                }
            }
            b'*' => {
                let len = get_decimal(src)?;
                for _ in 0..len {
                    Frame::check(src)?;
                }
                Ok(())
            }
            actual => Err(format!("protocol error; invalid frame type byte `{}`", actual).into()),
        }
    }

    /// The message has alraedy been validated with `check`
    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        match get_u8(src)? {
            b'+' => {
                // Read the line and convert it into `Vec<u8>`
                let line = get_line(src)?.to_vec();
                let string = String::from_utf8(line)?;
                Ok(Frame::Simple(string))
            }
            b'-' => {
                let line = get_line(src)?.to_vec();
                let string = String::from_utf8(line)?;
                Ok(Frame::Error(string))
            }
            b':' => {
                let len = get_decimal(src)?;
                Ok(Frame::Integer(len))
            }
            // bulk string
            b'$' => {
                // check null string: `$-1\r\n`
                if b'-' == peek_u8(src)? {
                    let line = get_line(src)?;
                    if line != b"-1" {
                        return Err("protocol error; invalid frame format".into());
                    }
                    return Ok(Frame::Null);
                }

                // get number of bytes
                let len = get_decimal(src)?.try_into()?;
                // skip CRLF
                let next_cursor = len + 2;

                if src.remaining() < next_cursor {
                    return Err(Error::Incomplete);
                }

                let data = Bytes::copy_from_slice(&src.chunk()[..len]);
                // move the cursor end of line
                skip(src, next_cursor)?;

                Ok(Frame::Bulk(data))
            }
            // array type
            b'*' => {
                // get number of elem
                let num_elem = get_decimal(src)?.try_into()?;
                let mut out = Vec::with_capacity(num_elem);
                for _ in 0..num_elem {
                    out.push(Frame::parse(src)?);
                }
                Ok(Frame::Array(out))
            }
            _ => unimplemented!(),
        }
    }

    pub(crate) fn to_error(&self) -> crate::Error {
        format!("unexpected frame: {}", self).into()
    }
}

impl PartialEq<&str> for Frame {
    fn eq(&self, other: &&str) -> bool {
        match self {
            Frame::Simple(s) => s.eq(other),
            Frame::Bulk(s) => s.eq(other),
            _ => false,
        }
    }
}

impl std::fmt::Display for Frame {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        use std::str;
        match self {
            Frame::Simple(r) => r.fmt(fmt),
            Frame::Error(msg) => write!(fmt, "error: {}", msg),
            Frame::Integer(num) => num.fmt(fmt),
            Frame::Bulk(msg) => match str::from_utf8(msg) {
                Ok(s) => s.fmt(fmt),
                Err(_) => write!(fmt, "{:?}", msg),
            },
            Frame::Null => "(nil)".fmt(fmt),
            Frame::Array(parts) | Frame::Pipe(parts) => {
                for (i, part) in parts.iter().enumerate() {
                    if i > 0 {
                        write!(fmt, " ")?;
                        part.fmt(fmt)?;
                    }
                }
                Ok(())
            },
        }
    }
}

// get first bytes without move the cursor
fn peek_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }
    Ok(src.chunk()[0])
}

fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }
    Ok(src.get_u8())
}

fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), Error> {
    if src.remaining() < n {
        return Err(Error::Incomplete);
    }
    src.advance(n);
    Ok(())
}

/// Read a new line terminated decimal
fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<u64, Error> {
    use atoi::atoi;
    let line = get_line(src)?;
    atoi::<u64>(line).ok_or_else(|| "protocol error; invalid frame format decimal".into())
}

// Find a line, return buffer and set the cursor to end after `\n`
fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
    // scan the line directly
    let start = src.position() as usize;
    // scan the second last byte
    let end = src.get_ref().len() - 1;
    for i in start..end {
        if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
            // we found a line, update the potiion th be *after* the \n
            src.set_position((i + 2) as u64);
            return Ok(&src.get_ref()[start..i]);
        }
    }
    Err(Error::Incomplete)
}

impl From<String> for Error {
    fn from(src: String) -> Self {
        Error::Other(src.into())
    }
}

impl From<&str> for Error {
    fn from(src: &str) -> Self {
        src.to_string().into()
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(_src: std::string::FromUtf8Error) -> Self {
        "protocol error; invalid frame format".into()
    }
}

impl From<std::num::TryFromIntError> for Error {
    fn from(_: std::num::TryFromIntError) -> Self {
        "protocol error; invalid frame format".into()
    }
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::Incomplete => "stream ended early".fmt(fmt),
            Error::Other(err) => err.fmt(fmt),
        }
    }
}
