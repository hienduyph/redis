use crate::frame::{self, Frame};

use bytes::{Buf, BytesMut};
use std::io::{self, Cursor};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

#[derive(Debug)]
pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(socket: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(socket),
            // use 4KB read to read
            buffer: BytesMut::with_capacity(4 * 1024),
        }
    }

    /// Read a single `Frame` value from the underlying stream
    ///
    /// the function wais until it has retrieved enough data to parse a frame
    /// Any data remaining in the read buffer after the frame has been parsed
    /// is kept there for the next call to `read_frame`
    ///
    /// # Returns
    /// On succes,s the received frame is returned. If the `TcpStrem`
    /// in closed in a way that doesn't break a frame in half, it returns
    /// `None`. Other wise, an error is returned!
    pub async fn read_frame(&mut self) -> crate::Result<Option<Frame>> {
        loop {
            // attempt to parse a frame from the buffered data. If enough data
            // has been buffeded, the frame is returned
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    return Ok(None);
                }
                return Err("connection reset by pear".into());
            }
        }
    }

    /// tries to parse a frame from the buffer. If the buffer contains enough
    /// data, the frame is returned and the data removed the buffer.
    /// If not enough data has been bufferded yet, `Ok(None)` is returned. It the
    /// buffered data does not represent a valid frame, `Err` is returned
    fn parse_frame(&mut self) -> crate::Result<Option<Frame>> {
        use frame::Error::Incomplete;
        // Cursor used to track the current location in the buffer.
        // Currsor also implements `Buf` from the bytes crate
        // which provides a number of helpful utilities for working
        // with bytes
        let mut buf = Cursor::new(&self.buffer[..]);

        // The first step is to check if enough data has been buffered to parse a single frame.
        // This tstep is usually must faster than doing a full parse of the frame
        // and allow us to skip allocating data structures
        // to hold the frame data unless we know the full frame has been received
        match Frame::check(&mut buf) {
            Ok(_) => {
                // The check function will have advanced the cursor until the end of frame
                //Since the cursor had position set to zero before Frame::check was called,
                //we obtain the length of the frame by checking the cursor position
                let len = buf.position() as usize;
                buf.set_position(0);

                let frame = Frame::parse(&mut buf)?;

                self.buffer.advance(len);
                Ok(Some(frame))
            }
            Err(Incomplete) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Array(val) => {
                // array type
                self.stream.write_u8(b'*').await?;
                // size of array
                self.write_decimal(val.len() as u64).await?;

                // Iterate and encode each entry in the array
                for entry in &**val {
                    self.write_value(entry).await?;
                }
            },
            _ => self.write_value(frame).await?,

        }
        // Ensure the encoded frame is written to the socket. The calls above
        // are to the buffered stream and writes. Calling `flush` writes the remaining content of
        // the buffer to the scoket
        self.stream.flush().await
    }

    /// Write a frame literal to the stream
    async fn write_value(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Simple(val) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Error(val) => {
                self.stream.write_u8(b'-').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Integer(val) => {
                self.stream.write_u8(b':').await?;
                self.write_decimal(*val).await?;
            }
            Frame::Null => {
                self.stream.write_all(b"$-1\r\n").await?;
            }
            Frame::Bulk(val) => {
                let len = val.len();
                self.stream.write_u8(b'$').await?;
                self.write_decimal(len as u64).await?;
                self.stream.write_all(val).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Array(_val) => unreachable!(),
        }

        Ok(())
    }

    async fn write_decimal(&mut self, val: u64) -> io::Result<()> {
        use std::io::Write;

        // convert the vlaue into a string
        let mut buf = [0u8, 20];
        let mut buf = Cursor::new(&mut buf[..]);
        write!(&mut buf, "{}", val)?;

        let pos = buf.position() as usize;
        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        self.stream.write_all(b"\r\n").await?;
        Ok(())
    }
}
