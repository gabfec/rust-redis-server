//! Pub/Sub: which clients are subscribed to which channels, server-wide,
//! and delivering published messages to them.

use std::collections::HashMap;
use std::io::Write;
use std::sync::Mutex;

use crate::connection::Writer;
use crate::resp::RespValue;

#[derive(Default)]
pub struct PubSub {
    /// Channel -> the clients subscribed to it (by id), with where to
    /// send messages (`None` only for clients without a socket, in tests).
    channels: Mutex<HashMap<String, HashMap<u64, Option<Writer>>>>,
}

impl PubSub {
    pub fn subscribe(&self, client: u64, writer: Option<Writer>, channel: &str) {
        let mut channels = self.channels.lock().unwrap();
        channels.entry(channel.to_string()).or_default().insert(client, writer);
    }

    /// Removes the client from every channel (when it disconnects).
    pub fn unsubscribe_all(&self, client: u64) {
        let mut channels = self.channels.lock().unwrap();
        channels.retain(|_, clients| {
            clients.remove(&client);
            !clients.is_empty()
        });
    }

    /// Sends ["message", channel, message] to every subscriber of the
    /// channel. Returns how many clients are subscribed.
    pub fn publish(&self, channel: &str, message: &str) -> usize {
        let channels = self.channels.lock().unwrap();
        let Some(clients) = channels.get(channel) else { return 0 };
        let mut out = Vec::new();
        RespValue::bulk_array(["message", channel, message]).encode(&mut out);
        for writer in clients.values().flatten() {
            // A client that's gone is cleaned up when its connection ends.
            let _ = writer.lock().unwrap().write_all(&out);
        }
        clients.len()
    }
}
