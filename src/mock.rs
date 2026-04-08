//! Mock stream and backend message builders for testing.
//!
//! `MockStream` implements `AsyncRead + AsyncWrite` backed by `Vec<u8>` buffers.
//! Use the `msg` submodule to construct wire-format backend messages, concatenate
//! them into a byte vec, and feed them to a `PgConnection<MockStream>`.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use pg_stream::connection::PgConnection;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::connection::ConnectionFactory;

/// A fake TCP stream backed by read/write byte buffers.
///
/// - Reads return pre-programmed bytes (simulating server responses).
/// - Writes are captured for inspection (the encoded frontend messages).
/// - When read bytes are exhausted, reads return EOF (simulating connection close).
pub struct MockStream {
    read_data: Vec<u8>,
    read_pos: usize,
    pub write_buf: Vec<u8>,
}

impl MockStream {
    pub fn new(read_data: Vec<u8>) -> Self {
        Self {
            read_data,
            read_pos: 0,
            write_buf: Vec::new(),
        }
    }
}

impl AsyncRead for MockStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let available = &self.read_data[self.read_pos..];
        if available.is_empty() {
            return Poll::Ready(Ok(())); // EOF
        }
        let n = std::cmp::min(available.len(), buf.remaining());
        buf.put_slice(&available[..n]);
        self.read_pos += n;
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for MockStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.write_buf.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl Unpin for MockStream {}

/// Creates mock PgConnections pre-loaded with scripted response bytes.
pub struct MockConnectionFactory {
    pub response_bytes: Vec<u8>,
}

impl ConnectionFactory for MockConnectionFactory {
    type Stream = MockStream;

    async fn connect(&self) -> io::Result<PgConnection<MockStream>> {
        let stream = MockStream::new(self.response_bytes.clone());
        Ok(PgConnection::new(stream))
    }
}

/// Helper to create a `PgConnection<MockStream>` directly from message builders.
pub fn mock_conn(response_bytes: Vec<u8>) -> PgConnection<MockStream> {
    PgConnection::new(MockStream::new(response_bytes))
}

// ---------------------------------------------------------------------------
// Wire-format backend message builders
// ---------------------------------------------------------------------------
// Each function returns a complete wire-format message: type byte + length + payload.
// Concatenate them to build a response stream for MockStream.

pub mod msg {
    /// ReadyForQuery with transaction status.
    /// Status: b'I' = idle, b'T' = in transaction, b'E' = failed.
    pub fn ready_for_query(status: u8) -> Vec<u8> {
        let mut buf = vec![b'Z'];
        buf.extend_from_slice(&5u32.to_be_bytes()); // length: 4 (self) + 1
        buf.push(status);
        buf
    }

    pub fn parse_complete() -> Vec<u8> {
        let mut buf = vec![b'1'];
        buf.extend_from_slice(&4u32.to_be_bytes());
        buf
    }

    pub fn bind_complete() -> Vec<u8> {
        let mut buf = vec![b'2'];
        buf.extend_from_slice(&4u32.to_be_bytes());
        buf
    }

    pub fn close_complete() -> Vec<u8> {
        let mut buf = vec![b'3'];
        buf.extend_from_slice(&4u32.to_be_bytes());
        buf
    }

    pub fn no_data() -> Vec<u8> {
        let mut buf = vec![b'n'];
        buf.extend_from_slice(&4u32.to_be_bytes());
        buf
    }

    pub fn empty_query_response() -> Vec<u8> {
        let mut buf = vec![b'I'];
        buf.extend_from_slice(&4u32.to_be_bytes());
        buf
    }

    pub fn portal_suspended() -> Vec<u8> {
        let mut buf = vec![b's'];
        buf.extend_from_slice(&4u32.to_be_bytes());
        buf
    }

    pub fn copy_done() -> Vec<u8> {
        let mut buf = vec![b'c'];
        buf.extend_from_slice(&4u32.to_be_bytes());
        buf
    }

    /// CommandComplete with the given tag (e.g., "SELECT 1", "INSERT 0 1").
    pub fn command_complete(tag: &str) -> Vec<u8> {
        let payload_len = tag.len() + 1; // null terminator
        let mut buf = vec![b'C'];
        buf.extend_from_slice(&((4 + payload_len) as u32).to_be_bytes());
        buf.extend_from_slice(tag.as_bytes());
        buf.push(0); // null terminator
        buf
    }

    /// ErrorResponse with severity, SQLSTATE code, and message.
    pub fn error_response(severity: &str, code: &str, message: &str) -> Vec<u8> {
        // Fields: S=severity, V=severity(non-localized), C=code, M=message, terminated by \0
        let mut payload = Vec::new();
        // Severity (localized)
        payload.push(b'S');
        payload.extend_from_slice(severity.as_bytes());
        payload.push(0);
        // Severity (non-localized, same value for our purposes)
        payload.push(b'V');
        payload.extend_from_slice(severity.as_bytes());
        payload.push(0);
        // SQLSTATE code
        payload.push(b'C');
        payload.extend_from_slice(code.as_bytes());
        payload.push(0);
        // Message
        payload.push(b'M');
        payload.extend_from_slice(message.as_bytes());
        payload.push(0);
        // Terminator
        payload.push(0);

        let mut buf = vec![b'E'];
        buf.extend_from_slice(&((4 + payload.len()) as u32).to_be_bytes());
        buf.extend_from_slice(&payload);
        buf
    }

    /// NoticeResponse — same wire format as ErrorResponse.
    pub fn notice_response(severity: &str, code: &str, message: &str) -> Vec<u8> {
        let mut msg = error_response(severity, code, message);
        msg[0] = b'N'; // only difference is the type byte
        msg
    }

    /// DataRow with the given column values. Each value is a byte slice; use None for NULL.
    pub fn data_row(columns: &[Option<&[u8]>]) -> Vec<u8> {
        let mut payload = Vec::new();
        // Column count (2 bytes)
        payload.extend_from_slice(&(columns.len() as u16).to_be_bytes());
        for col in columns {
            match col {
                Some(data) => {
                    payload.extend_from_slice(&(data.len() as u32).to_be_bytes());
                    payload.extend_from_slice(data);
                }
                None => {
                    // NULL: length = -1 (as i32)
                    payload.extend_from_slice(&(-1i32).to_be_bytes());
                }
            }
        }

        let mut buf = vec![b'D'];
        buf.extend_from_slice(&((4 + payload.len()) as u32).to_be_bytes());
        buf.extend_from_slice(&payload);
        buf
    }

    /// RowDescription with simple text columns.
    pub fn row_description(columns: &[(&str, u32)]) -> Vec<u8> {
        let mut payload = Vec::new();
        // Column count (2 bytes)
        payload.extend_from_slice(&(columns.len() as u16).to_be_bytes());
        for (name, type_oid) in columns {
            // Column name (null-terminated)
            payload.extend_from_slice(name.as_bytes());
            payload.push(0);
            // Table OID (4 bytes) — 0 for computed columns
            payload.extend_from_slice(&0u32.to_be_bytes());
            // Column attribute number (2 bytes)
            payload.extend_from_slice(&0u16.to_be_bytes());
            // Type OID (4 bytes)
            payload.extend_from_slice(&type_oid.to_be_bytes());
            // Type size (2 bytes) — -1 for variable length
            payload.extend_from_slice(&(-1i16).to_be_bytes());
            // Type modifier (4 bytes) — -1 for default
            payload.extend_from_slice(&(-1i32).to_be_bytes());
            // Format code (2 bytes) — 0 = text
            payload.extend_from_slice(&0u16.to_be_bytes());
        }

        let mut buf = vec![b'T'];
        buf.extend_from_slice(&((4 + payload.len()) as u32).to_be_bytes());
        buf.extend_from_slice(&payload);
        buf
    }

    /// ParameterDescription with the given type OIDs.
    pub fn parameter_description(oids: &[u32]) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&(oids.len() as u16).to_be_bytes());
        for oid in oids {
            payload.extend_from_slice(&oid.to_be_bytes());
        }

        let mut buf = vec![b't'];
        buf.extend_from_slice(&((4 + payload.len()) as u32).to_be_bytes());
        buf.extend_from_slice(&payload);
        buf
    }

    /// CopyInResponse with the given overall format and per-column formats.
    pub fn copy_in_response(format: u8, column_formats: &[u16]) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.push(format); // 0 = text, 1 = binary
        payload.extend_from_slice(&(column_formats.len() as u16).to_be_bytes());
        for fmt in column_formats {
            payload.extend_from_slice(&fmt.to_be_bytes());
        }

        let mut buf = vec![b'G'];
        buf.extend_from_slice(&((4 + payload.len()) as u32).to_be_bytes());
        buf.extend_from_slice(&payload);
        buf
    }

    /// CopyOutResponse — same format as CopyInResponse, different type byte.
    pub fn copy_out_response(format: u8, column_formats: &[u16]) -> Vec<u8> {
        let mut msg = copy_in_response(format, column_formats);
        msg[0] = b'H';
        msg
    }

    /// CopyData with arbitrary payload.
    pub fn copy_data(data: &[u8]) -> Vec<u8> {
        let mut buf = vec![b'd'];
        buf.extend_from_slice(&((4 + data.len()) as u32).to_be_bytes());
        buf.extend_from_slice(data);
        buf
    }

    /// Concatenate multiple messages into a single byte vec for MockStream.
    pub fn concat(messages: &[Vec<u8>]) -> Vec<u8> {
        messages.iter().flat_map(|m| m.iter().copied()).collect()
    }
}
