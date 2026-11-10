//! Leader-follower replication.

pub mod rdb;
pub mod replica;

use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;

use crate::config::Config;

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
        Replication { role, replid: random_replid() }
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
            "master_repl_offset:0".to_string(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replid_is_40_random_alphanumerics() {
        let (a, b) = (random_replid(), random_replid());
        assert_eq!(a.len(), 40);
        assert!(a.chars().all(|c| c.is_ascii_alphanumeric()));
        assert_ne!(a, b);
    }
}
