use std::io::Write;

#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(String),
    NullBulk,
    Array(Vec<RespValue>),
    NullArray,
    /// Bytes sent exactly as they are, e.g. an RDB file (not valid RESP).
    Raw(Vec<u8>),
}

impl RespValue {
    pub fn ok() -> Self {
        RespValue::Simple("OK".into())
    }

    pub fn bulk(s: impl Into<String>) -> Self {
        RespValue::Bulk(s.into())
    }

    /// A bulk string of any bytes. Sent as-is when not valid UTF-8.
    pub fn bulk_bytes(bytes: &[u8]) -> Self {
        match std::str::from_utf8(bytes) {
            Ok(s) => RespValue::Bulk(s.to_owned()),
            Err(_) => {
                let mut out = format!("${}\r\n", bytes.len()).into_bytes();
                out.extend_from_slice(bytes);
                out.extend_from_slice(b"\r\n");
                RespValue::Raw(out)
            }
        }
    }

    pub fn bulk_array<S: Into<String>>(items: impl IntoIterator<Item = S>) -> Self {
        RespValue::Array(items.into_iter().map(RespValue::bulk).collect())
    }

    /// Several replies to one command, sent one after the other.
    pub fn sequence(values: impl IntoIterator<Item = RespValue>) -> Self {
        let mut out = Vec::new();
        values.into_iter().for_each(|value| value.encode(&mut out));
        RespValue::Raw(out)
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        // Writing to a Vec<u8> cannot fail.
        let _ = match self {
            RespValue::Simple(s) => write!(out, "+{s}\r\n"),
            RespValue::Error(s) => write!(out, "-{s}\r\n"),
            RespValue::Integer(i) => write!(out, ":{i}\r\n"),
            RespValue::Bulk(s) => write!(out, "${}\r\n{s}\r\n", s.len()),
            RespValue::NullBulk => write!(out, "$-1\r\n"),
            RespValue::NullArray => write!(out, "*-1\r\n"),
            RespValue::Raw(bytes) => {
                out.extend_from_slice(bytes);
                Ok(())
            }
            RespValue::Array(items) => {
                let _ = write!(out, "*{}\r\n", items.len());
                items.iter().for_each(|item| item.encode(out));
                Ok(())
            }
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_nested_array() {
        let mut out = Vec::new();
        RespValue::Array(vec![RespValue::bulk("a"), RespValue::Integer(3), RespValue::NullBulk])
            .encode(&mut out);
        assert_eq!(out, b"*3\r\n$1\r\na\r\n:3\r\n$-1\r\n");
    }

    #[test]
    fn bulk_bytes_keeps_any_byte() {
        assert_eq!(RespValue::bulk_bytes(b"@"), RespValue::bulk("@"));
        let mut out = Vec::new();
        RespValue::bulk_bytes(&[0x80, 0]).encode(&mut out);
        assert_eq!(out, b"$2\r\n\x80\x00\r\n");
    }
}
