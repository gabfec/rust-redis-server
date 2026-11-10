//! Leader-follower replication.

use crate::config::Config;

#[derive(Debug, Clone, PartialEq, Default)]
pub enum Role {
    #[default]
    Master,
    /// Replica of the master at `host:port`.
    Replica { host: String, port: u16 },
}

/// Replication state shared by all connections.
#[derive(Default)]
pub struct Replication {
    pub role: Role,
}

impl Replication {
    pub fn new(config: &Config) -> Self {
        let role = match &config.replicaof {
            None => Role::Master,
            Some((host, port)) => Role::Replica { host: host.clone(), port: *port },
        };
        Replication { role }
    }

    /// The "replication" section of INFO.
    pub fn info(&self) -> String {
        let role = match self.role {
            Role::Master => "master",
            Role::Replica { .. } => "slave",
        };
        ["# Replication".to_string(), format!("role:{role}")].join("\r\n")
    }
}
