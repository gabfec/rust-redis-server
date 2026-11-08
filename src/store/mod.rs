mod stream;

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

pub use stream::{Stream, StreamEntry, StreamId};

pub enum Value {
    String(String),
    List(VecDeque<String>),
    Stream(Stream),
}

impl Value {
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::String(_) => "string",
            Value::List(_) => "list",
            Value::Stream(_) => "stream",
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
#[derive(Default)]
pub struct Store {
    data: HashMap<String, Entry>,
}

impl Store {
    pub fn get(&mut self, key: &str) -> Option<&Value> {
        self.purge_if_expired(key);
        self.data.get(key).map(|e| &e.value)
    }

    fn get_mut(&mut self, key: &str) -> Option<&mut Value> {
        self.purge_if_expired(key);
        self.data.get_mut(key).map(|e| &mut e.value)
    }

    fn get_or_insert(&mut self, key: String, default: impl FnOnce() -> Value) -> &mut Value {
        self.purge_if_expired(&key);
        let entry = self.data.entry(key).or_insert_with(|| Entry { value: default(), expires_at: None });
        &mut entry.value
    }

    pub fn set(&mut self, key: String, value: Value, ttl: Option<Duration>) {
        let expires_at = ttl.map(|ttl| Instant::now() + ttl);
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
}
