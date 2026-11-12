//! Pub/Sub: which clients are subscribed to which channels, server-wide.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

#[derive(Default)]
pub struct PubSub {
    /// Channel -> ids of the clients subscribed to it.
    channels: Mutex<HashMap<String, HashSet<u64>>>,
}

impl PubSub {
    pub fn subscribe(&self, client: u64, channel: &str) {
        self.channels.lock().unwrap().entry(channel.to_string()).or_default().insert(client);
    }

    /// Removes the client from every channel (when it disconnects).
    pub fn unsubscribe_all(&self, client: u64) {
        let mut channels = self.channels.lock().unwrap();
        channels.retain(|_, clients| {
            clients.remove(&client);
            !clients.is_empty()
        });
    }

    pub fn subscriber_count(&self, channel: &str) -> usize {
        self.channels.lock().unwrap().get(channel).map_or(0, HashSet::len)
    }
}
