//! Users, for ACL and AUTH.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::sha256;

/// A rule of ACL SETUSER.
#[derive(Clone, Debug, PartialEq)]
pub enum Rule {
    /// `>password`: add a password (and drop nopass).
    AddPassword(String),
    /// `<password`: remove a password.
    RemovePassword(String),
    /// `nopass`: any password works; forget the passwords.
    NoPass,
    /// `resetpass`: forget the passwords and drop nopass.
    ResetPass,
}

impl Rule {
    pub fn parse(rule: &str) -> Option<Rule> {
        if let Some(password) = rule.strip_prefix('>') {
            return Some(Rule::AddPassword(password.to_string()));
        }
        if let Some(password) = rule.strip_prefix('<') {
            return Some(Rule::RemovePassword(password.to_string()));
        }
        match rule.to_ascii_lowercase().as_str() {
            "nopass" => Some(Rule::NoPass),
            "resetpass" => Some(Rule::ResetPass),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct User {
    /// Any password (or none) authenticates as this user.
    pub nopass: bool,
    /// SHA-256 hashes of the passwords, in lowercase hex.
    pub passwords: Vec<String>,
}

impl User {
    fn apply(&mut self, rule: Rule) {
        match rule {
            Rule::AddPassword(password) => {
                let hash = sha256::hex_digest(password.as_bytes());
                if !self.passwords.contains(&hash) {
                    self.passwords.push(hash);
                }
                self.nopass = false;
            }
            Rule::RemovePassword(password) => {
                let hash = sha256::hex_digest(password.as_bytes());
                self.passwords.retain(|p| *p != hash);
            }
            Rule::NoPass => {
                self.nopass = true;
                self.passwords.clear();
            }
            Rule::ResetPass => {
                self.nopass = false;
                self.passwords.clear();
            }
        }
    }

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

    /// Whether this username and password authenticate.
    pub fn check(&self, name: &str, password: &str) -> bool {
        let users = self.users.lock().unwrap();
        users.get(name).is_some_and(|user| {
            user.nopass || user.passwords.contains(&sha256::hex_digest(password.as_bytes()))
        })
    }

    /// Applies the rules to the user, creating it if needed.
    pub fn set(&self, name: &str, rules: Vec<Rule>) {
        let mut users = self.users.lock().unwrap();
        let user = users.entry(name.to_string()).or_default();
        rules.into_iter().for_each(|rule| user.apply(rule));
    }
}
