mod stream;
mod zset;

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

pub use stream::{Stream, StreamEntry, StreamId};
pub use zset::SortedSet;

pub enum Value {
    String(String),
    List(VecDeque<String>),
    Stream(Stream),
    SortedSet(SortedSet),
}

impl Value {
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::String(_) => "string",
            Value::List(_) => "list",
            Value::Stream(_) => "stream",
            Value::SortedSet(_) => "zset",
        }
    }
}

struct Entry {
    value: Value,
    expires_at: Option<Instant>,
}

/// The key is holding a different type than the command expects.
#[derive(Debug)]
pub struct WrongType;

/// The keyspace. Every accessor drops the key first if it has expired.
///
/// Every key also has a version (for WATCH) that changes whenever the key
/// is created, modified, replaced, deleted or expires. Accessors that hand
/// out `&mut` count as a modification.
#[derive(Default)]
pub struct Store {
    data: HashMap<String, Entry>,
    /// Last version of every key ever modified; 0 if never modified.
    versions: HashMap<String, u64>,
    last_version: u64,
}

impl Store {
    /// All keys that haven't expired.
    pub fn keys(&mut self) -> Vec<String> {
        let keys: Vec<String> = self.data.keys().cloned().collect();
        keys.into_iter().filter(|key| self.get(key).is_some()).collect()
    }

    pub fn version(&mut self, key: &str) -> u64 {
        self.purge_if_expired(key);
        self.versions.get(key).copied().unwrap_or(0)
    }

    fn touch(&mut self, key: &str) {
        self.last_version += 1;
        self.versions.insert(key.to_owned(), self.last_version);
    }

    pub fn get(&mut self, key: &str) -> Option<&Value> {
        self.purge_if_expired(key);
        self.data.get(key).map(|e| &e.value)
    }

    fn get_mut(&mut self, key: &str) -> Option<&mut Value> {
        self.purge_if_expired(key);
        if self.data.contains_key(key) {
            self.touch(key);
        }
        self.data.get_mut(key).map(|e| &mut e.value)
    }

    fn get_or_insert(&mut self, key: String, default: impl FnOnce() -> Value) -> &mut Value {
        self.purge_if_expired(&key);
        self.touch(&key);
        let entry = self.data.entry(key).or_insert_with(|| Entry { value: default(), expires_at: None });
        &mut entry.value
    }

    pub fn set(&mut self, key: String, value: Value, ttl: Option<Duration>) {
        let expires_at = ttl.map(|ttl| Instant::now() + ttl);
        self.touch(&key);
        self.data.insert(key, Entry { value, expires_at });
    }

    fn purge_if_expired(&mut self, key: &str) {
        let expired = self
            .data
            .get(key)
            .and_then(|e| e.expires_at)
            .is_some_and(|at| Instant::now() >= at);
        if expired {
            self.data.remove(key);
            self.touch(key);
        }
    }

    // ---- typed accessors ----

    pub fn get_string(&mut self, key: &str) -> Result<Option<&String>, WrongType> {
        match self.get(key) {
            None => Ok(None),
            Some(Value::String(s)) => Ok(Some(s)),
            Some(_) => Err(WrongType),
        }
    }

    pub fn get_string_mut(&mut self, key: &str) -> Result<Option<&mut String>, WrongType> {
        match self.get_mut(key) {
            None => Ok(None),
            Some(Value::String(s)) => Ok(Some(s)),
            Some(_) => Err(WrongType),
        }
    }

    pub fn get_list(&mut self, key: &str) -> Result<Option<&VecDeque<String>>, WrongType> {
        match self.get(key) {
            None => Ok(None),
            Some(Value::List(l)) => Ok(Some(l)),
            Some(_) => Err(WrongType),
        }
    }

    pub fn get_list_mut(&mut self, key: &str) -> Result<Option<&mut VecDeque<String>>, WrongType> {
        match self.get_mut(key) {
            None => Ok(None),
            Some(Value::List(l)) => Ok(Some(l)),
            Some(_) => Err(WrongType),
        }
    }

    pub fn list_or_create(&mut self, key: String) -> Result<&mut VecDeque<String>, WrongType> {
        match self.get_or_insert(key, || Value::List(VecDeque::new())) {
            Value::List(l) => Ok(l),
            _ => Err(WrongType),
        }
    }

    pub fn get_stream(&mut self, key: &str) -> Result<Option<&Stream>, WrongType> {
        match self.get(key) {
            None => Ok(None),
            Some(Value::Stream(s)) => Ok(Some(s)),
            Some(_) => Err(WrongType),
        }
    }

    pub fn stream_or_create(&mut self, key: String) -> Result<&mut Stream, WrongType> {
        match self.get_or_insert(key, || Value::Stream(Stream::default())) {
            Value::Stream(s) => Ok(s),
            _ => Err(WrongType),
        }
    }

    pub fn get_zset(&mut self, key: &str) -> Result<Option<&SortedSet>, WrongType> {
        match self.get(key) {
            None => Ok(None),
            Some(Value::SortedSet(z)) => Ok(Some(z)),
            Some(_) => Err(WrongType),
        }
    }

    pub fn zset_or_create(&mut self, key: String) -> Result<&mut SortedSet, WrongType> {
        match self.get_or_insert(key, || Value::SortedSet(SortedSet::default())) {
            Value::SortedSet(z) => Ok(z),
            _ => Err(WrongType),
        }
    }
}
