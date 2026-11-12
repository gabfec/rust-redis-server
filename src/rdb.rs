//! RDB snapshot files: loaded at startup from `<dir>/<dbfilename>`. The
//! master also sends an empty one to a replica after FULLRESYNC.
//!
//! Format: "REDIS" + 4-digit version, then metadata (FA name value),
//! databases (FE index, FB sizes, key/value pairs), then FF + checksum.

use std::path::Path;

use crate::store::{Store, Value};

/// An RDB file with no keys (Redis 7.2 format).
const EMPTY_RDB_HEX: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

pub fn empty() -> Vec<u8> {
    (0..EMPTY_RDB_HEX.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&EMPTY_RDB_HEX[i..i + 2], 16).unwrap())
        .collect()
}

/// A key/value pair read from an RDB file.
#[derive(Debug, PartialEq)]
pub struct Entry {
    pub key: String,
    pub value: String,
}

/// Loads the RDB file at `path` into `store`. A missing file means an
/// empty database. Returns how many keys were loaded.
pub fn load_into(store: &mut Store, path: &Path) -> Result<usize, String> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(format!("{}: {e}", path.display())),
    };
    let entries = parse(&bytes)?;
    let count = entries.len();
    for entry in entries {
        store.set(entry.key, Value::String(entry.value), None);
    }
    Ok(count)
}

pub fn parse(bytes: &[u8]) -> Result<Vec<Entry>, String> {
    let mut r = Reader { bytes, pos: 0 };
    if r.take(5)? != b"REDIS" {
        return Err("not an RDB file".into());
    }
    r.take(4)?; // version
    let mut entries = Vec::new();
    loop {
        match r.byte()? {
            0xFA => {
                // Metadata: name and value, both strings.
                r.string()?;
                r.string()?;
            }
            0xFE => {
                r.length()?; // database index
            }
            0xFB => {
                r.length()?; // number of keys
                r.length()?; // number of keys with an expiry
            }
            0xFF => return Ok(entries), // followed by a checksum
            0x00 => {
                let key = r.string()?;
                let value = r.string()?;
                entries.push(Entry { key, value });
            }
            other => return Err(format!("unsupported RDB entry type 0x{other:02X}")),
        }
    }
}

enum Length {
    Size(usize),
    /// A special string encoding (the low 6 bits of a 0b11 byte).
    Encoded(u8),
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl Reader<'_> {
    fn take(&mut self, n: usize) -> Result<&[u8], String> {
        let end = self.pos + n;
        let slice = self.bytes.get(self.pos..end).ok_or("RDB file ends too early")?;
        self.pos = end;
        Ok(slice)
    }

    fn byte(&mut self) -> Result<u8, String> {
        Ok(self.take(1)?[0])
    }

    /// A size: the first two bits say how it is stored.
    fn length(&mut self) -> Result<usize, String> {
        match self.length_or_encoding()? {
            Length::Size(n) => Ok(n),
            Length::Encoded(kind) => Err(format!("expected a size, found encoding {kind}")),
        }
    }

    fn length_or_encoding(&mut self) -> Result<Length, String> {
        let first = self.byte()?;
        Ok(match first >> 6 {
            0b00 => Length::Size((first & 0x3F) as usize),
            0b01 => Length::Size((((first & 0x3F) as usize) << 8) | self.byte()? as usize),
            0b10 => {
                let b = self.take(4)?;
                Length::Size(u32::from_be_bytes([b[0], b[1], b[2], b[3]]) as usize)
            }
            _ => Length::Encoded(first & 0x3F),
        })
    }

    /// A string: length-prefixed, or an integer stored as 1, 2 or 4
    /// little-endian bytes (C0, C1, C2).
    fn string(&mut self) -> Result<String, String> {
        match self.length_or_encoding()? {
            Length::Size(len) => Ok(String::from_utf8_lossy(self.take(len)?).into_owned()),
            Length::Encoded(0) => Ok((self.byte()? as i8).to_string()),
            Length::Encoded(1) => {
                let b = self.take(2)?;
                Ok(i16::from_le_bytes([b[0], b[1]]).to_string())
            }
            Length::Encoded(2) => {
                let b = self.take(4)?;
                Ok(i32::from_le_bytes([b[0], b[1], b[2], b[3]]).to_string())
            }
            Length::Encoded(kind) => Err(format!("unsupported string encoding {kind} (LZF?)")),
        }
    }
}

#[cfg(test)]
pub mod test_util {
    /// A length-prefixed RDB string (shorter than 64 bytes).
    pub fn string(s: &str) -> Vec<u8> {
        assert!(s.len() < 64);
        [&[s.len() as u8][..], s.as_bytes()].concat()
    }

    /// An RDB file with one database holding `entries` (each an already
    /// encoded entry, starting with its type or expiry byte).
    pub fn file(entries: &[Vec<u8>]) -> Vec<u8> {
        let mut out = b"REDIS0011".to_vec();
        out.extend([0xFA]);
        out.extend(string("redis-ver"));
        out.extend(string("7.2.0"));
        out.extend([0xFE, 0x00, 0xFB, entries.len() as u8, 0x00]);
        entries.iter().for_each(|e| out.extend(e));
        out.extend([0xFF, 0, 0, 0, 0, 0, 0, 0, 0]);
        out
    }

    /// A fresh, empty temporary directory.
    pub fn temp_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("redis-test-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// A string key/value entry without expiry.
    pub fn string_entry(key: &str, value: &str) -> Vec<u8> {
        [vec![0x00], string(key), string(value)].concat()
    }
}

#[cfg(test)]
mod tests {
    use super::test_util::{file, string_entry, temp_dir};
    use super::{load_into, parse, Entry};
    use crate::store::Store;
    #[test]
    fn empty_rdb_has_the_redis_header() {
        let rdb = super::empty();
        assert!(rdb.starts_with(b"REDIS0011"));
        assert_eq!(rdb.len(), 88);
    }

    #[test]
    fn the_empty_rdb_has_no_keys() {
        assert_eq!(parse(&super::empty()), Ok(vec![]));
    }

    #[test]
    fn reads_a_key() {
        let rdb = file(&[string_entry("foo", "bar")]);
        assert_eq!(parse(&rdb), Ok(vec![Entry { key: "foo".into(), value: "bar".into() }]));
    }

    #[test]
    fn length_encodings() {
        let mut r = super::Reader { bytes: &[0x0A, 0x42, 0xBC, 0x80, 0x00, 0x00, 0x42, 0x68], pos: 0 };
        assert_eq!(r.length(), Ok(10));
        assert_eq!(r.length(), Ok(700));
        assert_eq!(r.length(), Ok(17000));
    }

    #[test]
    fn integer_encoded_strings() {
        let bytes = [0xC0, 0x7B, 0xC1, 0x39, 0x30, 0xC2, 0x87, 0xD6, 0x12, 0x00];
        let mut r = super::Reader { bytes: &bytes, pos: 0 };
        assert_eq!(r.string().as_deref(), Ok("123"));
        assert_eq!(r.string().as_deref(), Ok("12345"));
        assert_eq!(r.string().as_deref(), Ok("1234567"));
        assert!(super::Reader { bytes: &[0xC3, 0x00], pos: 0 }.string().is_err());
    }

    #[test]
    fn rejects_bad_files() {
        assert!(parse(b"HELLO").is_err());
        assert!(parse(b"REDIS0011\x00\x03fo").is_err());
    }

    #[test]
    fn loads_a_file_into_the_store() {
        let dir = temp_dir("rdb-load");
        let path = dir.join("dump.rdb");
        let mut store = Store::default();
        assert_eq!(load_into(&mut store, &path), Ok(0), "a missing file is an empty database");

        std::fs::write(&path, file(&[string_entry("foo", "bar")])).unwrap();
        assert_eq!(load_into(&mut store, &path), Ok(1));
        assert_eq!(store.keys(), ["foo"]);
        assert_eq!(store.get_string("foo").unwrap().map(String::as_str), Some("bar"));
    }
}
