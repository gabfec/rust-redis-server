//! Leader-follower replication.

pub mod rdb;
pub mod replica;

use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::io::{self, Write};
use std::net::TcpStream;
use std::sync::{Mutex, MutexGuard};

use crate::config::Config;
use crate::resp::RespValue;

#[derive(Debug, Clone, PartialEq, Default)]
pub enum Role {
    #[default]
    Master,
    /// Replica of the master at `host:port`.
    Replica { host: String, port: u16 },
}

/// Replication state shared by all connections.
pub struct Replication {
    pub role: Role,
    /// Random 40-character id of this server's replication history.
    pub replid: String,
    /// Master side: who to forward writes to.
    replicas: Mutex<Replicas>,
}

#[derive(Default)]
struct Replicas {
    links: Vec<ReplicaLink>,
    next_id: u64,
    /// Bytes propagated so far (master_repl_offset).
    offset: u64,
}

struct ReplicaLink {
    id: u64,
    stream: TcpStream,
}

impl Default for Replication {
    fn default() -> Self {
        Replication::new(&Config::default())
    }
}

impl Replication {
    pub fn new(config: &Config) -> Self {
        let role = match &config.replicaof {
            None => Role::Master,
            Some((host, port)) => Role::Replica { host: host.clone(), port: *port },
        };
        Replication { role, replid: random_replid(), replicas: Mutex::default() }
    }

    fn replicas(&self) -> MutexGuard<'_, Replicas> {
        self.replicas.lock().unwrap()
    }

    /// Sends `first` (the reply to PSYNC, ending with the RDB file) to
    /// `stream`, then starts forwarding writes to it. Both happen under the
    /// lock `propagate` takes, so no write can be lost or arrive before
    /// the RDB file. Returns the replica's id.
    pub fn add_replica(&self, mut stream: TcpStream, first: &[u8]) -> io::Result<u64> {
        let mut replicas = self.replicas();
        stream.write_all(first)?;
        let id = replicas.next_id;
        replicas.next_id += 1;
        replicas.links.push(ReplicaLink { id, stream });
        Ok(id)
    }

    pub fn remove_replica(&self, id: u64) {
        self.replicas().links.retain(|link| link.id != id);
    }

    /// Forwards a command (as received from the client) to every replica.
    pub fn propagate(&self, words: &[String]) {
        let mut replicas = self.replicas();
        if replicas.links.is_empty() {
            return;
        }
        let mut out = Vec::new();
        RespValue::bulk_array(words.iter().cloned()).encode(&mut out);
        replicas.offset += out.len() as u64;
        // A replica we can't write to is gone.
        replicas.links.retain_mut(|link| link.stream.write_all(&out).is_ok());
    }

    /// master_repl_offset: how many bytes of commands were propagated.
    pub fn offset(&self) -> u64 {
        self.replicas().offset
    }

    /// The "replication" section of INFO.
    pub fn info(&self) -> String {
        let role = match self.role {
            Role::Master => "master",
            Role::Replica { .. } => "slave",
        };
        [
            "# Replication".to_string(),
            format!("role:{role}"),
            format!("master_replid:{}", self.replid),
            format!("master_repl_offset:{}", self.offset()),
        ]
        .join("\r\n")
    }
}

/// 40 random lowercase hex characters, like Redis.
fn random_replid() -> String {
    // Each RandomState is seeded with fresh random keys.
    (0..3)
        .map(|i| format!("{:016x}", RandomState::new().hash_one(i)))
        .collect::<String>()[..40]
        .to_string()
}

/// Helpers for tests that need a replica link.
#[cfg(test)]
pub mod test_util {
    use std::io::{ErrorKind, Read};
    use std::net::{TcpListener, TcpStream};
    use std::time::Duration;

    /// Two connected sockets.
    pub fn socket_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let a = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (b, _) = listener.accept().unwrap();
        (a, b)
    }

    /// Everything that arrives on `stream` within ~100 ms.
    pub fn read_available(stream: &mut TcpStream) -> String {
        stream.set_read_timeout(Some(Duration::from_millis(100))).unwrap();
        let mut out = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            match stream.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => out.extend_from_slice(&chunk[..n]),
                Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => break,
                Err(e) => panic!("{e}"),
            }
        }
        String::from_utf8(out).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::test_util::{read_available, socket_pair};
    use super::*;
    use crate::commands::test_util::Client;

    #[test]
    fn replid_is_40_random_alphanumerics() {
        let (a, b) = (random_replid(), random_replid());
        assert_eq!(a.len(), 40);
        assert!(a.chars().all(|c| c.is_ascii_alphanumeric()));
        assert_ne!(a, b);
    }

    #[test]
    fn successful_writes_are_propagated_as_received() {
        let mut c = Client::default();
        let (to_replica, mut replica) = socket_pair();
        c.server.replication.add_replica(to_replica, &[]).unwrap();

        c.cmd(&["SET", "foo", "1"]);
        c.cmd(&["GET", "foo"]); // not a write
        c.cmd(&["incr", "foo"]);
        c.cmd(&["SET", "s", "x"]);
        c.cmd(&["INCR", "s"]); // fails: not propagated
        c.cmd(&["RPUSH", "l", "a", "b"]);

        let expected = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$1\r\n1\r\n\
                        *2\r\n$4\r\nincr\r\n$3\r\nfoo\r\n\
                        *3\r\n$3\r\nSET\r\n$1\r\ns\r\n$1\r\nx\r\n\
                        *4\r\n$5\r\nRPUSH\r\n$1\r\nl\r\n$1\r\na\r\n$1\r\nb\r\n";
        assert_eq!(read_available(&mut replica), expected);
        assert_eq!(c.server.replication.offset(), expected.len() as u64);
    }

    #[test]
    fn writes_inside_exec_are_propagated() {
        let mut c = Client::default();
        let (to_replica, mut replica) = socket_pair();
        c.server.replication.add_replica(to_replica, &[]).unwrap();
        c.cmd(&["MULTI"]);
        c.cmd(&["SET", "a", "1"]);
        c.cmd(&["GET", "a"]);
        c.cmd(&["EXEC"]);
        assert_eq!(read_available(&mut replica), "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n");
    }

    #[test]
    fn offset_stays_zero_without_replicas() {
        let mut c = Client::default();
        c.cmd(&["SET", "a", "1"]);
        assert_eq!(c.server.replication.offset(), 0);
    }
}
