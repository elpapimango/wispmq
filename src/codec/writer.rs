//! Append-only writer producing MQTT v5.0 wire representations.

use crate::codec::MAX_VARIABLE_BYTE_INTEGER;
use crate::error::{protocol, Result};

#[derive(Default)]
pub struct Writer {
    buf: Vec<u8>,
}

impl Writer {
    pub fn new() -> Self {
        Writer { buf: Vec::new() }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Writer {
            buf: Vec::with_capacity(cap),
        }
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn into_vec(self) -> Vec<u8> {
        self.buf
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    pub fn put_u8(&mut self, v: u8) {
        self.buf.push(v);
    }

    pub fn put_u16(&mut self, v: u16) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    pub fn put_u32(&mut self, v: u32) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    pub fn put_bytes(&mut self, v: &[u8]) {
        self.buf.extend_from_slice(v);
    }

    /// Encode a Variable Byte Integer (Section 1.5.5).
    pub fn put_varint(&mut self, mut value: u32) -> Result<()> {
        if value > MAX_VARIABLE_BYTE_INTEGER {
            return Err(protocol("variable byte integer overflow"));
        }
        loop {
            let mut byte = (value % 128) as u8;
            value /= 128;
            if value > 0 {
                byte |= 0x80;
            }
            self.buf.push(byte);
            if value == 0 {
                break;
            }
        }
        Ok(())
    }

    /// Encode Binary Data with a Two Byte length prefix (Section 1.5.6).
    pub fn put_binary(&mut self, v: &[u8]) {
        self.put_u16(v.len() as u16);
        self.buf.extend_from_slice(v);
    }

    /// Encode a UTF-8 Encoded String (Section 1.5.4).
    pub fn put_utf8(&mut self, v: &str) {
        self.put_u16(v.len() as u16);
        self.buf.extend_from_slice(v.as_bytes());
    }

    pub fn put_utf8_pair(&mut self, k: &str, v: &str) {
        self.put_utf8(k);
        self.put_utf8(v);
    }
}
