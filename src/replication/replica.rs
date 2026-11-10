//! The replica side: connect to the master, do the handshake, then apply
//! every command the master forwards.

use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::thread;

use super::Role;
use crate::commands::{self, Ctx};
use crate::connection::{command_words, ConnState};
use crate::resp::{self, RespValue};
use crate::server::ServerState;

/// If this server is a replica, connects to its master in the background.
/// `own_port` is the port this server listens on.
pub fn start(server: Arc<ServerState>, own_port: u16) {
    let Role::Replica { host, port } = server.replication.role.clone() else { return };
    thread::spawn(move || {
        if let Err(e) = run(&server, &host, port, own_port) {
            eprintln!("replication: {e}");
        }
    });
}

fn run(server: &ServerState, host: &str, port: u16, own_port: u16) -> io::Result<()> {
    let mut link = connect(host, port, own_port)?;
    // TODO: load the master's data once there is RDB support. For now the
    // master always sends an empty file.
    let _rdb = link.read_rdb()?;
    link.apply_commands(server)
}

/// Connects to the master and runs the handshake.
fn connect(host: &str, port: u16, own_port: u16) -> io::Result<MasterLink> {
    let mut link = MasterLink { stream: TcpStream::connect((host, port))?, buf: Vec::new() };
    let ok = |r: &RespValue| *r == RespValue::ok();
    link.expect(&["PING"], |r| *r == RespValue::Simple("PONG".into()))?;
    link.expect(&["REPLCONF", "listening-port", &own_port.to_string()], ok)?;
    link.expect(&["REPLCONF", "capa", "psync2"], ok)?;
    // "?" and -1: we have no data yet, so ask for a full copy.
    link.expect(&["PSYNC", "?", "-1"], |r| {
        matches!(r, RespValue::Simple(s) if s.starts_with("FULLRESYNC "))
    })?;
    Ok(link)
}

/// The connection to the master.
struct MasterLink {
    stream: TcpStream,
    /// Bytes received but not parsed yet.
    buf: Vec<u8>,
}

impl MasterLink {
    fn send(&mut self, words: &[&str]) -> io::Result<()> {
        let mut out = Vec::new();
        RespValue::bulk_array(words.iter().copied()).encode(&mut out);
        self.stream.write_all(&out)
    }

    /// Sends a command and checks the master's reply.
    fn expect(
        &mut self,
        words: &[&str],
        ok: impl Fn(&RespValue) -> bool,
    ) -> io::Result<RespValue> {
        self.send(words)?;
        let reply = self.read_value()?;
        if !ok(&reply) {
            return Err(io::Error::other(format!("master replied {reply:?} to {words:?}")));
        }
        Ok(reply)
    }

    /// Applies the commands the master forwards until the connection ends.
    /// Only REPLCONF (GETACK) gets a reply; everything else runs silently.
    fn apply_commands(&mut self, server: &ServerState) -> io::Result<()> {
        let mut conn = ConnState::default();
        loop {
            let (value, len) = self.read_value_with_len()?;
            let Some(words) = command_words(value.clone()).filter(|w| !w.is_empty()) else {
                return Err(io::Error::other(format!("bad command from master: {value:?}")));
            };
            let wants_reply = words[0].eq_ignore_ascii_case("REPLCONF");
            let reply = commands::dispatch(words, &mut Ctx::new(server, &mut conn));
            if wants_reply {
                let mut out = Vec::new();
                reply.encode(&mut out);
                self.stream.write_all(&out)?;
            }
            // Counted after running it: GETACK reports the offset before itself.
            conn.master_offset += len as u64;
        }
    }

    /// The RDB file after FULLRESYNC: `$<len>\r\n<bytes>`, no trailing CRLF.
    fn read_rdb(&mut self) -> io::Result<Vec<u8>> {
        loop {
            if let Some(eol) = self.buf.windows(2).position(|w| w == b"\r\n") {
                let header = std::str::from_utf8(&self.buf[..eol]).unwrap_or("");
                let len: usize = header
                    .strip_prefix('$')
                    .and_then(|n| n.parse().ok())
                    .ok_or_else(|| io::Error::other(format!("bad RDB header {header:?}")))?;
                let start = eol + 2;
                if self.buf.len() >= start + len {
                    let rdb = self.buf[start..start + len].to_vec();
                    self.buf.drain(..start + len);
                    return Ok(rdb);
                }
            }
            self.fill()?;
        }
    }

    fn read_value(&mut self) -> io::Result<RespValue> {
        Ok(self.read_value_with_len()?.0)
    }

    /// The next value, and how many bytes it took.
    fn read_value_with_len(&mut self) -> io::Result<(RespValue, usize)> {
        loop {
            match resp::parse(&self.buf) {
                Ok(Some((value, used))) => {
                    self.buf.drain(..used);
                    return Ok((value, used));
                }
                Ok(None) => self.fill()?,
                Err(e) => return Err(io::Error::other(e.to_string())),
            }
        }
    }

    /// Reads more bytes from the master into `buf`.
    fn fill(&mut self) -> io::Result<()> {
        let mut chunk = [0u8; 4096];
        let n = self.stream.read(&mut chunk)?;
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "master closed the connection"));
        }
        self.buf.extend_from_slice(&chunk[..n]);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::thread::{self, JoinHandle};

    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use super::{connect, run};
    use crate::replication::test_util::read_command;
    use crate::replication::rdb;
    use crate::resp::RespValue;
    use crate::server::ServerState;

    /// A fake master that expects `script`'s commands in order and sends
    /// each step's raw reply. Returns its port.
    pub fn fake_master(script: Vec<(Vec<&'static str>, Vec<u8>)>) -> (u16, JoinHandle<TcpStream>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = Vec::new();
            for (expected, reply) in script {
                let got = read_command(&mut stream, &mut buf);
                assert_eq!(got, expected, "replica sent the wrong command");
                stream.write_all(&reply).unwrap();
            }
            stream
        });
        (port, handle)
    }

    /// The handshake steps, as (command the replica sends, master's reply).
    pub fn handshake_script(own_port: &'static str) -> Vec<(Vec<&'static str>, Vec<u8>)> {
        vec![
            (vec!["PING"], b"+PONG\r\n".to_vec()),
            (vec!["REPLCONF", "listening-port", own_port], b"+OK\r\n".to_vec()),
            (vec!["REPLCONF", "capa", "psync2"], b"+OK\r\n".to_vec()),
            (vec!["PSYNC", "?", "-1"], format!("+FULLRESYNC {} 0\r\n", "a".repeat(40)).into()),
        ]
    }

    #[test]
    fn handshake() {
        let (port, master) = fake_master(handshake_script("6380"));
        connect("127.0.0.1", port, 6380).unwrap();
        master.join().unwrap();
    }

    #[test]
    fn handshake_fails_on_unexpected_reply() {
        let (port, master) = fake_master(vec![(vec!["PING"], b"-ERR nope\r\n".to_vec())]);
        assert!(connect("127.0.0.1", port, 6380).is_err());
        master.join().unwrap();
    }

    fn encode(commands: &[&[&str]]) -> Vec<u8> {
        let mut out = Vec::new();
        for words in commands {
            RespValue::bulk_array(words.iter().copied()).encode(&mut out);
        }
        out
    }

    /// The master's reply to PSYNC, followed by `extra` in the same packet.
    fn fullresync_then(extra: &[u8]) -> Vec<u8> {
        let rdb = rdb::empty();
        let mut out = format!("+FULLRESYNC {} 0\r\n${}\r\n", "a".repeat(40), rdb.len()).into_bytes();
        out.extend_from_slice(&rdb);
        out.extend_from_slice(extra);
        out
    }

    /// Starts a replica of a fake master that runs the handshake, then
    /// sends `after_psync` right behind the RDB file. Returns the replica's
    /// state and the master's end of the connection.
    fn replica_of_fake_master(after_psync: &[u8]) -> (Arc<ServerState>, TcpStream) {
        let mut script = handshake_script("6380");
        script.last_mut().unwrap().1 = fullresync_then(after_psync);
        let (port, master) = fake_master(script);
        let server = Arc::new(ServerState::default());
        let replica = Arc::clone(&server);
        thread::spawn(move || run(&replica, "127.0.0.1", port, 6380));
        (server, master.join().unwrap())
    }

    /// Waits up to 1 s for `key` to have `value` on the replica.
    fn wait_for_value(server: &ServerState, key: &str, value: &str) {
        let deadline = Instant::now() + Duration::from_secs(1);
        while Instant::now() < deadline {
            if server.store().get_string(key).ok().flatten().map(String::as_str) == Some(value) {
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
        panic!("{key} never became {value}");
    }

    #[test]
    fn applies_commands_from_the_master_without_replying() {
        // Commands in the same packet as the RDB file...
        let (server, mut master) =
            replica_of_fake_master(&encode(&[&["SET", "foo", "1"], &["SET", "bar", "2"]]));
        wait_for_value(&server, "foo", "1");
        wait_for_value(&server, "bar", "2");
        // ...and in later packets.
        master.write_all(&encode(&[&["SET", "baz", "3"], &["INCR", "foo"]])).unwrap();
        wait_for_value(&server, "baz", "3");
        wait_for_value(&server, "foo", "2");

        master.set_read_timeout(Some(Duration::from_millis(100))).unwrap();
        let mut buf = [0u8; 64];
        assert!(master.read(&mut buf).is_err(), "the replica must not reply");
    }

    #[test]
    fn answers_getack() {
        let (_server, mut master) = replica_of_fake_master(&encode(&[&["REPLCONF", "GETACK", "*"]]));
        let mut buf = Vec::new();
        assert_eq!(read_command(&mut master, &mut buf), ["REPLCONF", "ACK", "0"]);
    }

    #[test]
    fn ack_offset_counts_the_bytes_of_processed_commands() {
        let getack: &[&str] = &["REPLCONF", "GETACK", "*"];
        let (_server, mut master) = replica_of_fake_master(&encode(&[getack]));
        let mut buf = Vec::new();
        assert_eq!(read_command(&mut master, &mut buf), ["REPLCONF", "ACK", "0"]);

        let commands = encode(&[&["PING"], &["SET", "foo", "1"], &["SET", "bar", "2"]]);
        master.write_all(&commands).unwrap();
        master.write_all(&encode(&[getack])).unwrap();
        // Includes the first GETACK, but not the one being answered.
        let expected = encode(&[getack]).len() + commands.len();
        assert_eq!(read_command(&mut master, &mut buf), ["REPLCONF", "ACK", &expected.to_string()]);
    }
}
