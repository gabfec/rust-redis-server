use std::io::{self, Read, Write};
use std::net::TcpStream;

use crate::commands::{self, Ctx, Transaction};
use crate::resp::{self, RespValue};
use crate::server::ServerState;

/// Per-connection state (subscriptions, replica flag, ... go here too).
#[derive(Default)]
pub struct ConnState {
    /// `Some` between MULTI and EXEC/DISCARD.
    pub transaction: Option<Transaction>,
    /// Keys from WATCH, with their version at that time.
    pub watched: Vec<(String, u64)>,
    /// Set by PSYNC: the other end is a replica of this server.
    pub is_replica: bool,
    /// Its id in `Replication`, once registered.
    pub replica_id: Option<u64>,
    /// On a replica's link to its master: bytes of commands processed.
    pub master_offset: u64,
}

pub fn handle(mut stream: TcpStream, server: &ServerState) -> io::Result<()> {
    let mut conn = ConnState::default();
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];

    loop {
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            if let Some(id) = conn.replica_id {
                server.replication.remove_replica(id);
            }
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);

        // A single read may hold several (pipelined) commands, or only part of one.
        let mut out = Vec::new();
        loop {
            match resp::parse(&buf) {
                Ok(Some((value, used))) => {
                    buf.drain(..used);
                    if let Some(reply) = execute(value, server, &mut conn) {
                        reply.encode(&mut out);
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    RespValue::Error(format!("ERR Protocol error: {e}")).encode(&mut out);
                    buf.clear();
                    break;
                }
            }
        }
        if conn.is_replica && conn.replica_id.is_none() {
            // PSYNC just ran: `out` ends with the RDB file. From now on the
            // replica also gets every write.
            conn.replica_id = Some(server.replication.add_replica(stream.try_clone()?, &out)?);
        } else if !out.is_empty() {
            stream.write_all(&out)?;
        }
    }
}

/// A command as sent by a client: an array of strings.
pub fn command_words(value: RespValue) -> Option<Vec<String>> {
    match value {
        RespValue::Array(items) => items
            .into_iter()
            .map(|item| match item {
                RespValue::Bulk(s) | RespValue::Simple(s) => Some(s),
                _ => None,
            })
            .collect(),
        _ => None,
    }
}

fn execute(value: RespValue, server: &ServerState, conn: &mut ConnState) -> Option<RespValue> {
    match command_words(value) {
        Some(words) if words.is_empty() => None,
        Some(words) => Some(commands::dispatch(words, &mut Ctx::new(server, conn))),
        None => Some(RespValue::Error("ERR Protocol error: expected an array of bulk strings".into())),
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::Arc;
    use std::thread;

    use crate::replication::rdb;
    use crate::replication::test_util::read_available;
    use crate::resp::RespValue;
    use crate::server::ServerState;

    /// A server accepting connections on a random port.
    fn start_server() -> (u16, Arc<ServerState>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = Arc::new(ServerState::default());
        let shared = Arc::clone(&server);
        thread::spawn(move || {
            for stream in listener.incoming() {
                let server = Arc::clone(&shared);
                thread::spawn(move || super::handle(stream.unwrap(), &server));
            }
        });
        (port, server)
    }

    fn send(stream: &mut TcpStream, words: &[&str]) {
        let mut out = Vec::new();
        RespValue::bulk_array(words.iter().copied()).encode(&mut out);
        stream.write_all(&out).unwrap();
    }

    #[test]
    fn psync_then_writes_are_forwarded_to_the_replica() {
        let (port, server) = start_server();
        let mut replica = TcpStream::connect(("127.0.0.1", port)).unwrap();
        send(&mut replica, &["PSYNC", "?", "-1"]);

        let rdb = rdb::empty();
        let mut expected = format!(
            "+FULLRESYNC {} 0\r\n${}\r\n",
            server.replication.replid,
            rdb.len()
        )
        .into_bytes();
        expected.extend_from_slice(&rdb);
        let mut got = vec![0u8; expected.len()];
        replica.read_exact(&mut got).unwrap();
        assert_eq!(got, expected);

        let mut client = TcpStream::connect(("127.0.0.1", port)).unwrap();
        send(&mut client, &["SET", "foo", "bar"]);
        assert_eq!(read_available(&mut client), "+OK\r\n");
        assert_eq!(
            read_available(&mut replica),
            "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
        );
    }
}
