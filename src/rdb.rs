//! RDB snapshot files: loaded at startup from `<dir>/<dbfilename>`. The
//! master also sends an empty one to a replica after FULLRESYNC.
//!
//! Format: "REDIS" + 4-digit version, then metadata (FA name value),
//! databases (FE index, FB sizes, key/value pairs), then FF + checksum.

use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
    /// When the key expires, in Unix time milliseconds.
    pub expires_at_ms: Option<u64>,
}

/// Loads the RDB file at `path` into `store`. A missing file means an
/// empty database. Returns how many keys were loaded.
pub fn load_into(store: &mut Store, path: &Path) -> Result<usize, String> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(format!("{}: {e}", path.display())),
    };
    let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    let mut count = 0;
    for entry in parse(&bytes)? {
        let ttl = match entry.expires_at_ms {
            None => None,
            Some(at) if at <= now_ms => continue, // already expired
            Some(at) => Some(Duration::from_millis(at - now_ms)),
        };
        store.set(entry.key, Value::String(entry.value), ttl);
        count += 1;
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
    let mut expires_at_ms = None; // applies to the next key
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
            0xFC => {
                let b = r.take(8)?;
                expires_at_ms = Some(u64::from_le_bytes(b.try_into().unwrap()));
            }
            0xFD => {
                let b = r.take(4)?;
                expires_at_ms = Some(u32::from_le_bytes(b.try_into().unwrap()) as u64 * 1000);
            }
            0x00 => {
                let key = r.string()?;
                let value = r.string()?;
                entries.push(Entry { key, value, expires_at_ms: expires_at_ms.take() });
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

    /// A string entry expiring at `at_ms` (Unix ms), stored as FC + 8 bytes.
    pub fn expiring_entry_ms(key: &str, value: &str, at_ms: u64) -> Vec<u8> {
        [vec![0xFC], at_ms.to_le_bytes().to_vec(), string_entry(key, value)].concat()
    }

    /// A string entry expiring at `at_s` (Unix seconds), stored as FD + 4 bytes.
    pub fn expiring_entry_s(key: &str, value: &str, at_s: u32) -> Vec<u8> {
        [vec![0xFD], at_s.to_le_bytes().to_vec(), string_entry(key, value)].concat()
    }
}

#[cfg(test)]
mod tests {
    use super::test_util::{expiring_entry_ms, expiring_entry_s, file, string_entry, temp_dir};
    use super::{load_into, parse, Entry};
    use crate::commands::test_util::Client;
    use crate::resp::RespValue;
    use crate::store::Store;

    /// A client of a server that loaded an RDB file with `entries`.
    fn client_with_rdb(name: &str, entries: &[Vec<u8>]) -> Client {
        let path = temp_dir(name).join("dump.rdb");
        std::fs::write(&path, file(entries)).unwrap();
        let c = Client::default();
        load_into(&mut c.server.store(), &path).unwrap();
        c
    }
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
        let foo = Entry { key: "foo".into(), value: "bar".into(), expires_at_ms: None };
        assert_eq!(parse(&rdb), Ok(vec![foo]));
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

    #[test]
    fn get_a_string_value() {
        let mut integer = vec![0x00];
        integer.extend(super::test_util::string("count"));
        integer.extend([0xC1, 0x39, 0x30]); // 12345
        let mut c = client_with_rdb("rdb-get", &[string_entry("foo", "bar"), integer]);
        assert_eq!(c.cmd(&["GET", "foo"]), RespValue::bulk("bar"));
        assert_eq!(c.cmd(&["GET", "count"]), RespValue::bulk("12345"));
        assert_eq!(c.cmd(&["INCR", "count"]), RespValue::Integer(12346));
    }

    #[test]
    fn keys_from_a_file_with_several_keys() {
        let entries: Vec<_> = ["foo", "bar", "baz"].iter().map(|k| string_entry(k, "v")).collect();
        let mut c = client_with_rdb("rdb-keys", &entries);
        let RespValue::Array(mut keys) = c.cmd(&["KEYS", "*"]) else { panic!() };
        keys.sort_by_key(|k| format!("{k:?}"));
        assert_eq!(RespValue::Array(keys), RespValue::bulk_array(["bar", "baz", "foo"]));
    }

    #[test]
    fn get_multiple_string_values() {
        let entries = [string_entry("apple", "red"), string_entry("banana", "yellow"), string_entry("grape", "purple")];
        let mut c = client_with_rdb("rdb-values", &entries);
        for (key, value) in [("apple", "red"), ("banana", "yellow"), ("grape", "purple")] {
            assert_eq!(c.cmd(&["GET", key]), RespValue::bulk(value), "{key}");
        }
    }

    #[test]
    fn values_with_expiry() {
        let now_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
        let now_s = (now_ms / 1000) as u32;
        let entries = [
            string_entry("forever", "1"),
            expiring_entry_ms("past_ms", "2", now_ms - 60_000),
            expiring_entry_ms("future_ms", "3", now_ms + 3_600_000),
            expiring_entry_s("past_s", "4", now_s - 60),
            expiring_entry_s("future_s", "5", now_s + 3600),
            expiring_entry_ms("soon", "6", now_ms + 150),
        ];
        assert_eq!(parse(&file(&entries)).unwrap()[1].expires_at_ms, Some(now_ms - 60_000));

        let mut c = client_with_rdb("rdb-expiry", &entries);
        assert_eq!(c.cmd(&["GET", "forever"]), RespValue::bulk("1"));
        assert_eq!(c.cmd(&["GET", "past_ms"]), RespValue::NullBulk);
        assert_eq!(c.cmd(&["GET", "future_ms"]), RespValue::bulk("3"));
        assert_eq!(c.cmd(&["GET", "past_s"]), RespValue::NullBulk);
        assert_eq!(c.cmd(&["GET", "future_s"]), RespValue::bulk("5"));
        assert_eq!(c.cmd(&["GET", "soon"]), RespValue::bulk("6"));
        std::thread::sleep(std::time::Duration::from_millis(200));
        assert_eq!(c.cmd(&["GET", "soon"]), RespValue::NullBulk, "expires on time");
    }
}
