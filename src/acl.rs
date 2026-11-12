//! Users, for ACL and AUTH. There is only the `default` user.

use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Clone, Debug, Default)]
pub struct User {}

impl User {
    /// The flags ACL GETUSER shows.
    pub fn flags(&self) -> Vec<&'static str> {
        Vec::new()
    }
}

pub struct Users {
    users: Mutex<HashMap<String, User>>,
}

impl Default for Users {
    fn default() -> Self {
        let users = HashMap::from([("default".to_string(), User::default())]);
        Users { users: Mutex::new(users) }
    }
}

impl Users {
    pub fn get(&self, name: &str) -> Option<User> {
        self.users.lock().unwrap().get(name).cloned()
    }
}
