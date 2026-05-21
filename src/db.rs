use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

#[allow(dead_code)]
#[derive(Debug)]
pub struct StreamEntry {
    pub id_ms: u64,
    pub id_seq: u64,
    pub fields: HashMap<String, String>,
}

#[derive(Debug)]
pub enum RedisValue {
    String(String),
    List(Vec<String>),
    Stream(Vec<StreamEntry>),
}

#[derive(Debug)]
pub struct Entry {
    pub value: RedisValue,
    pub created_at: Instant,
    pub expires_in: Option<Duration>,
}

pub type Db = Arc<Mutex<HashMap<String, Entry>>>;
pub type Cv = Arc<Condvar>;

impl RedisValue {
    pub fn type_name(&self) -> &'static str {
        match self {
            RedisValue::String(_) => "string",
            RedisValue::List(_) => "list",
            RedisValue::Stream(_) => "stream",
        }
    }
}

pub fn get_valid_entry<'a>(map: &'a mut HashMap<String, Entry>, key: &str) -> Option<&'a Entry> {
    let expired = if let Some(entry) = map.get(key) {
        if let Some(duration) = entry.expires_in {
            entry.created_at.elapsed() > duration
        } else {
            false
        }
    } else {
        false
    };

    if expired {
        map.remove(key);
        return None;
    }

    // Now we can safely return a reference because we know it's valid.
    map.get(key)
}
