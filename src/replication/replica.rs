//! The replica side: connect to the master and do the handshake.

use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::thread;

use super::Role;
use crate::resp::{self, RespValue};
use crate::server::ServerState;

/// If this server is a replica, connects to its master in the background.
pub fn start(server: Arc<ServerState>) {
    let Role::Replica { host, port } = server.replication.role.clone() else { return };
    thread::spawn(move || {
        if let Err(e) = connect(&host, port) {
            eprintln!("replication: {e}");
        }
    });
}

/// Connects to the master and runs the handshake.
fn connect(host: &str, port: u16) -> io::Result<MasterLink> {
    let mut link = MasterLink { stream: TcpStream::connect((host, port))?, buf: Vec::new() };
    link.expect(&["PING"], |r| *r == RespValue::Simple("PONG".into()))?;
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

    fn read_value(&mut self) -> io::Result<RespValue> {
        loop {
            match resp::parse(&self.buf) {
                Ok(Some((value, used))) => {
                    self.buf.drain(..used);
                    return Ok(value);
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

    use super::connect;
    use crate::resp::{self, RespValue};

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

    pub fn read_command(stream: &mut TcpStream, buf: &mut Vec<u8>) -> Vec<String> {
        loop {
            if let Some((value, used)) = resp::parse(buf).unwrap() {
                buf.drain(..used);
                let RespValue::Array(items) = value else { panic!("{value:?}") };
                return items
                    .into_iter()
                    .map(|i| match i {
                        RespValue::Bulk(s) => s,
                        other => panic!("{other:?}"),
                    })
                    .collect();
            }
            let mut chunk = [0u8; 4096];
            let n = stream.read(&mut chunk).unwrap();
            assert!(n > 0, "replica closed the connection");
            buf.extend_from_slice(&chunk[..n]);
        }
    }

    #[test]
    fn handshake_starts_with_ping() {
        let (port, master) = fake_master(vec![(vec!["PING"], b"+PONG\r\n".to_vec())]);
        connect("127.0.0.1", port).unwrap();
        master.join().unwrap();
    }

    #[test]
    fn handshake_fails_on_unexpected_reply() {
        let (port, master) = fake_master(vec![(vec!["PING"], b"-ERR nope\r\n".to_vec())]);
        assert!(connect("127.0.0.1", port).is_err());
        master.join().unwrap();
    }
}
