use std::io::Result as IoResult;
use std::io::Write;
use std::net::TcpStream;
use std::time::{Duration, Instant};

use crate::db::{get_valid_entry, Db, Entry, RedisValue};
use crate::resp::Resp;

use super::RedisWrite;

pub fn handle_set(
    stream: &mut TcpStream,
    db: &Db,
    key: String,
    value: String,
    px: Option<u64>,
) -> IoResult<()> {
    let mut db_lock = db.lock().unwrap();

    db_lock.insert(
        key,
        Entry {
            value: RedisValue::String(value),
            created_at: Instant::now(),
            expires_in: px.map(Duration::from_millis),
        },
    );
    stream.write_resp(Resp::string("OK"))
}

pub fn handle_get(stream: &mut TcpStream, db: &Db, key: String) -> IoResult<()> {
    let mut db_lock = db.lock().unwrap();

    match get_valid_entry(&mut db_lock, &key) {
        Some(entry) => match &entry.value {
            RedisValue::String(s) => {
                stream.write_resp(Resp::bulk_string(s))
            }
            _ => {
                stream.write_resp(Resp::error("WRONGTYPE Operation against a key holding the wrong kind of value"))
            }
        },
        None => {
            stream.write_resp(Resp::null_bulk())
        }
    }
}
