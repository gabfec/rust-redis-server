//! Users, for ACL and AUTH. There is only the `default` user.

use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Clone, Debug, Default)]
pub struct User {
    /// Any password (or none) authenticates as this user.
    pub nopass: bool,
    /// SHA-256 hashes of the passwords, in lowercase hex.
    pub passwords: Vec<String>,
}

impl User {
    /// The flags ACL GETUSER shows.
    pub fn flags(&self) -> Vec<&'static str> {
        let mut flags = Vec::new();
        if self.nopass {
            flags.push("nopass");
        }
        flags
    }
}

pub struct Users {
    users: Mutex<HashMap<String, User>>,
}

impl Default for Users {
    fn default() -> Self {
        // Like in Redis, the default user starts without a password, so
        // new connections are authenticated as it.
        let users = HashMap::from([("default".to_string(), User { nopass: true, passwords: Vec::new() })]);
        Users { users: Mutex::new(users) }
    }
}

impl Users {
    pub fn get(&self, name: &str) -> Option<User> {
        self.users.lock().unwrap().get(name).cloned()
    }
}
