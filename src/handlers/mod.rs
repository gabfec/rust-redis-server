pub mod list;
pub mod stream;
pub mod string;
mod keys;

use std::io::{Read, Result as IoResult, Write};
use std::net::TcpStream;

use crate::command::{parse_message, Command};
use crate::db::{get_valid_entry, Cv, Db};
use crate::resp::Resp;

trait RedisWrite {
    fn write_resp(&mut self, resp: impl AsRef<[u8]>) -> IoResult<()>;
}

impl RedisWrite for TcpStream {
    fn write_resp(&mut self, resp: impl AsRef<[u8]>) -> IoResult<()> {
        self.write_all(resp.as_ref())
    }
}

pub fn handle_connection(mut stream: TcpStream, db: Db, cv: Cv) -> IoResult<()> {
    let mut buffer = [0; 1024];
    loop {
        let bytes_read = stream.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }

        let input = String::from_utf8_lossy(&buffer[..bytes_read]);

        if let Some(command) = parse_message(&input) {
            println!("Received command: {:?}", command);

            match command {
                Command::Ping => {
                    stream.write_resp(Resp::string("PONG"))?;
                }
                Command::Echo(content) => {
                    stream.write_resp(Resp::bulk_string(&content))?;
                }
                Command::Set { key, value, px } => {
                    string::handle_set(&mut stream, &db, key, value, px)?;
                }
                Command::Get(key) => {
                    string::handle_get(&mut stream, &db, key)?;
                }
                Command::Rpush { key, values } => {
                    list::handle_rpush(&mut stream, &db, &cv, key, values)?;
                }
                Command::Lpush { key, values } => {
                    list::handle_lpush(&mut stream, &db, &cv, key, values)?;
                }
                Command::Lrange { key, start, stop } => {
                    list::handle_lrange(&mut stream, &db, key, start, stop)?;
                }
                Command::Llen(key) => {
                    list::handle_llen(&mut stream, &db, key)?;
                }
                Command::Lpop { key, count } => {
                    list::handle_lpop(&mut stream, &db, key, count)?;
                }
                Command::Blpop { keys, timeout } => {
                    list::handle_blpop(&mut stream, &db, &cv, keys, timeout)?;
                }
                Command::Xadd { key, id, fields } => {
                    stream::handle_xadd(&mut stream, &db, key, id, fields)?;
                }
                Command::Xrange { key, start, end } => {
                    stream::handle_xrange(&mut stream, &db, key, start, end)?;
                }
                Command::Xread { keys, ids, block_timeout } => {
                    stream::handle_xread(&mut stream, &db, keys, ids, block_timeout)?;
                }
                Command::Type(key) => {
                    keys::handle_type(&mut stream, &db, key)?;
                }
            }
        }
    }
    Ok(())
}
