use std::io::Write;

use hyper::body::Bytes;
use jaq_json::Val;
use tokio::sync::mpsc;

use crate::service::ApiError;

pub struct StreamWriter {
    tx: mpsc::Sender<Bytes>,
    buf: Vec<u8>,
    limit: usize,
}

impl StreamWriter {
    pub fn new(tx: mpsc::Sender<Bytes>) -> Self {
        let limit = 64 * 1024;
        Self {
            tx,
            buf: Vec::with_capacity(limit),
            limit,
        }
    }

    fn emit(&mut self) -> std::io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let out = std::mem::replace(&mut self.buf, Vec::with_capacity(self.limit));
        self.tx.blocking_send(Bytes::from(out)).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::BrokenPipe, "client disconnected")
        })?;
        Ok(())
    }
}

impl Write for StreamWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buf.extend_from_slice(buf);
        if self.buf.len() >= self.limit {
            self.emit()?;
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.emit()
    }
}

pub struct JsonArrayWriter<'a, W: Write> {
    out: &'a mut W,
    first: bool,
}

impl<'a, W: Write> JsonArrayWriter<'a, W> {
    pub fn new(out: &'a mut W) -> Result<Self, ApiError> {
        out.write_all(b"[")
            .map_err(|e| ApiError::Internal(format!("io: {}", e)))?;
        Ok(Self { out, first: true })
    }

    pub fn push_val(&mut self, v: Val) -> Result<(), ApiError> {
        if self.first {
            self.first = false;
        } else {
            self.out
                .write_all(b",")
                .map_err(|e| ApiError::Internal(format!("io: {}", e)))?;
        }
        write!(self.out, "{}", v).map_err(|e| ApiError::Internal(format!("io: {}", e)))?;
        Ok(())
    }

    pub fn finish(self) -> Result<(), ApiError> {
        self.out
            .write_all(b"]")
            .map_err(|e| ApiError::Internal(format!("io: {}", e)))?;
        Ok(())
    }
}
