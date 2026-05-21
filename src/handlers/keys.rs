// src/handlers/keys.rs
use std::io::Result as IoResult;
use std::io::Write;
use std::net::TcpStream;

use crate::db::{get_valid_entry, Db};
use crate::resp::Resp;

use super::RedisWrite;

pub fn handle_type(stream: &mut TcpStream, db: &Db, key: String) -> IoResult<()> {
    let mut map = db.lock().unwrap();

    let response = match get_valid_entry(&mut map, &key) {
        Some(entry) => entry.value.type_name(),
        None => "none",
    };

    stream.write_resp(Resp::string(response))
}
