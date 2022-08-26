use crate::frame::{self, Frame};

use bytes::{Buf, BytesMut, BufMut};
use log::{debug};
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
        let mut frames = Vec::new();
        let mut butes_left;
        loop {
            if let Some(frame) = self.parse_frame()? {
                debug!("Got Frame: {:?}", frame);
                frames.push(frame);
                continue;
            }

            // fast path
            if frames.len() > 0 && self.buffer.is_empty() {
                debug!("Fast path to parse stream");
                break;
            }

            debug!("Poll for new data");
            butes_left = self.stream.read_buf(&mut self.buffer).await?;
            // attempt to parse a frame from the buffered data. If enough data
            // has been buffeded, the frame is returned
            // no more data to read
            debug!("Read {}, Buf Size {}, got buf `{:?}`", butes_left, self.buffer.len(),String::from_utf8_lossy(&self.buffer.to_vec()));

            if butes_left == 0 && self.buffer.is_empty() {
                debug!("No things left. Close Reading");
                if frames.len() == 0 {
                    return Err("closed".into());
                }
                break
            }

        }

        debug!("Got Num Frames {}", frames.len());
        if frames.len() == 1 {
            return Ok(Some(frames.remove(0)));
        }
        return Ok(Some(Frame::Pipe(frames)));
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
        self.stream.write_all(&self.to_buf(frame)).await?;
        self.stream.flush().await
    }

    fn to_buf(&self,frame: &Frame) -> bytes::Bytes {
        match frame {
            Frame::Pipe(val) => {
                let bfs = val.iter().map(_frame_to_buf).fold(bytes::BytesMut::new(), |mut acc, xd | {
                    acc.extend(xd);
                    acc
                });
                return bfs.freeze();
            }
            _ => _frame_to_buf(frame).freeze(),
        }
    }

    async fn write_decimal(&mut self, val: u64) -> io::Result<()> {
        use std::io::Write;

        // convert the vlaue into a string
        let mut buf = [0u8, 20];
        let mut buf = Cursor::new(&mut buf[..]); write!(&mut buf, "{}", val)?;

        let pos = buf.position() as usize;
        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        self.stream.write_all(b"\r\n").await?;
        Ok(())
    }
}

fn _dec_to_buf(val: u64) -> io::Result<Vec<u8>> {
    use std::io::Write;

    let mut buf = [0u8, 20];
    let mut buf = Cursor::new(&mut buf[..]);
    write!(&mut buf, "{}", val)?;
    let pos = buf.position() as usize;
    let resp = &buf.get_ref()[..pos];
    Ok(resp.into())
}

fn _frame_to_buf(frame: &Frame) -> bytes::BytesMut {
    let mut buf  = bytes::BytesMut::new();
    match frame {
        Frame::Simple(val) => {
            buf.put_u8(b'+');
            buf.put(val.as_bytes());
        }
        Frame::Error(val) => {
            buf.put_u8(b'-');
            buf.put(val.as_bytes());
        }
        Frame::Integer(val) => {
            buf.put_u8(b':');
            buf.put_u64(*val);
        }
        Frame::Null => {
            buf.put(&b"$-1"[..]);
        }
        Frame::Bulk(val) => {
            let len = val.len();
            buf.put_u8(b'$');
            buf.extend(&_dec_to_buf(len as u64).unwrap());
            buf.put(&b"\r\n"[..]);
            buf.extend(val);
        }
        Frame::Array(val) => {
            // array type
            buf.put_u8(b'*');
            // size of array
            buf.extend(&_dec_to_buf(val.len() as u64).unwrap());
            // Iterate and encode each entry in the array
            for entry in &**val {
                buf.extend(_frame_to_buf(entry));
            }
        },
        Frame::Pipe(_val) => unreachable!(),
    }
    buf.put(&b"\r\n"[..]);
    buf
}
