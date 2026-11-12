use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::commands::{self, Ctx, Transaction};
use crate::resp::{self, RespValue};
use crate::server::ServerState;

/// Where to write to a client. Everything sent to it (replies, and
/// messages published by other clients) goes through this one lock, so
/// they can't interleave.
pub type Writer = Arc<Mutex<TcpStream>>;

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
    /// Pub/Sub channels this client is subscribed to, in order.
    pub subscriptions: Vec<String>,
    /// Unique id, given on first use (see `id()`).
    client_id: Option<u64>,
    /// The client's socket (`None` in unit tests).
    pub writer: Option<Writer>,
}

impl ConnState {
    /// This connection's unique id.
    pub fn id(&mut self) -> u64 {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        *self.client_id.get_or_insert_with(|| NEXT_ID.fetch_add(1, Ordering::Relaxed))
    }
}

pub fn handle(mut stream: TcpStream, server: &ServerState) -> io::Result<()> {
    let mut conn = ConnState::default();
    let result = serve(&mut stream, server, &mut conn);
    // However the connection ended, forget about it.
    if let Some(id) = conn.replica_id {
        server.replication.remove_replica(id);
    }
    if conn.client_id.is_some() {
        server.pubsub.unsubscribe_all(conn.id());
    }
    result
}

fn serve(stream: &mut TcpStream, server: &ServerState, conn: &mut ConnState) -> io::Result<()> {
    let writer = Arc::new(Mutex::new(stream.try_clone()?));
    conn.writer = Some(Arc::clone(&writer));
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];

    loop {
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);

        // A single read may hold several (pipelined) commands, or only part of one.
        let mut out = Vec::new();
        loop {
            match resp::parse(&buf) {
                Ok(Some((value, used))) => {
                    buf.drain(..used);
                    if let Some(reply) = execute(value, server, conn) {
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
            writer.lock().unwrap().write_all(&out)?;
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

    use std::time::{Duration, Instant};

    use crate::rdb;
    use crate::replication::test_util::{read_available, read_command_with_len};
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

    /// Connects as a replica: PSYNC, then reads the reply and the RDB file.
    fn fake_replica(port: u16, server: &ServerState) -> TcpStream {
        let mut replica = TcpStream::connect(("127.0.0.1", port)).unwrap();
        send(&mut replica, &["PSYNC", "?", "-1"]);
        let len = format!("+FULLRESYNC {} 0\r\n$88\r\n", server.replication.replid).len() + 88;
        replica.read_exact(&mut vec![0u8; len]).unwrap();
        replica
    }

    /// Answers every GETACK with the true offset. Panics (stopping the
    /// ACKs) if the master sends anything that isn't a command.
    fn ack_forever(mut replica: TcpStream) {
        thread::spawn(move || {
            let (mut buf, mut offset) = (Vec::new(), 0);
            loop {
                let (words, len) = read_command_with_len(&mut replica, &mut buf);
                if words[..2] == ["REPLCONF", "GETACK"] {
                    send(&mut replica, &["REPLCONF", "ACK", &offset.to_string()]);
                }
                offset += len;
            }
        });
    }

    /// One reply line, waiting up to 5 s.
    fn read_line(stream: &mut TcpStream) -> String {
        stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut line = Vec::new();
        let mut byte = [0u8];
        while !line.ends_with(b"\r\n") {
            stream.read_exact(&mut byte).unwrap();
            line.push(byte[0]);
        }
        String::from_utf8(line).unwrap()
    }

    #[test]
    fn wait_counts_replicas_that_acknowledged_the_writes() {
        let (port, server) = start_server();
        ack_forever(fake_replica(port, &server));
        ack_forever(fake_replica(port, &server));
        let _silent = fake_replica(port, &server);
        let mut client = TcpStream::connect(("127.0.0.1", port)).unwrap();

        // Nothing written yet: every replica is up to date.
        send(&mut client, &["WAIT", "3", "500"]);
        assert_eq!(read_line(&mut client), ":3\r\n");

        send(&mut client, &["SET", "a", "1"]);
        send(&mut client, &["SET", "b", "2"]);
        assert_eq!(read_line(&mut client), "+OK\r\n");
        assert_eq!(read_line(&mut client), "+OK\r\n");

        // Two replicas acknowledge: no need to wait for the timeout.
        let start = Instant::now();
        send(&mut client, &["WAIT", "2", "5000"]);
        assert_eq!(read_line(&mut client), ":2\r\n");
        assert!(start.elapsed() < Duration::from_secs(1));

        // A third never does: wait for the timeout, then report two.
        let start = Instant::now();
        send(&mut client, &["WAIT", "3", "300"]);
        assert_eq!(read_line(&mut client), ":2\r\n");
        assert!(start.elapsed() >= Duration::from_millis(300));
    }

    #[test]
    fn published_messages_reach_subscribers_only() {
        let (port, _server) = start_server();
        let connect = || TcpStream::connect(("127.0.0.1", port)).unwrap();
        let (mut foo1, mut foo2, mut bar) = (connect(), connect(), connect());
        for (client, channel) in [(&mut foo1, "foo"), (&mut foo2, "foo"), (&mut bar, "bar")] {
            send(client, &["SUBSCRIBE", channel]);
            assert!(read_available(client).starts_with("*3\r\n$9\r\nsubscribe\r\n"));
        }

        let mut publisher = connect();
        send(&mut publisher, &["PUBLISH", "foo", "hello"]);
        assert_eq!(read_line(&mut publisher), ":2\r\n");
        let hello = "*3\r\n$7\r\nmessage\r\n$3\r\nfoo\r\n$5\r\nhello\r\n";
        assert_eq!(read_available(&mut foo1), hello);
        assert_eq!(read_available(&mut foo2), hello);
        assert_eq!(read_available(&mut bar), "", "not subscribed to foo");

        send(&mut publisher, &["PUBLISH", "bar", "world"]);
        assert_eq!(read_line(&mut publisher), ":1\r\n");
        assert_eq!(read_available(&mut bar), "*3\r\n$7\r\nmessage\r\n$3\r\nbar\r\n$5\r\nworld\r\n");
        assert_eq!(read_available(&mut foo1), "");

        // A subscriber that disconnects no longer counts.
        drop(bar);
        thread::sleep(Duration::from_millis(50));
        send(&mut publisher, &["PUBLISH", "bar", "anyone?"]);
        assert_eq!(read_line(&mut publisher), ":0\r\n");
    }
}
