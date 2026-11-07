use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct StreamId {
    pub ms: u64,
    pub seq: u64,
}

impl StreamId {
    pub const MIN: StreamId = StreamId { ms: 0, seq: 0 };
    pub const MAX: StreamId = StreamId { ms: u64::MAX, seq: u64::MAX };

    pub fn new(ms: u64, seq: u64) -> Self {
        StreamId { ms, seq }
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

/// Parses "<ms>-<seq>" or "<ms>" (seq = 0).
impl FromStr for StreamId {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split_once('-') {
            Some((ms, seq)) => Ok(StreamId::new(ms.parse()?, seq.parse()?)),
            None => Ok(StreamId::new(s.parse()?, 0)),
        }
    }
}

pub struct StreamEntry {
    pub id: StreamId,
    pub fields: HashMap<String, String>,
}

/// Entries are always sorted by id (XADD only appends larger ids).
#[derive(Default)]
pub struct Stream {
    entries: Vec<StreamEntry>,
}

impl Stream {
    pub fn last_id(&self) -> Option<StreamId> {
        self.entries.last().map(|e| e.id)
    }

    pub fn push(&mut self, entry: StreamEntry) {
        debug_assert!(self.last_id().is_none_or(|last| entry.id > last));
        self.entries.push(entry);
    }

    /// Entries with `start <= id <= end`.
    pub fn range(&self, start: StreamId, end: StreamId) -> impl Iterator<Item = &StreamEntry> {
        self.entries.iter().filter(move |e| start <= e.id && e.id <= end)
    }

    /// Entries with `id > after`.
    pub fn after(&self, after: StreamId) -> impl Iterator<Item = &StreamEntry> {
        self.entries.iter().filter(move |e| e.id > after)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_orders_ids() {
        let a: StreamId = "5-3".parse().unwrap();
        let b: StreamId = "5".parse().unwrap();
        assert_eq!(a, StreamId::new(5, 3));
        assert_eq!(b, StreamId::new(5, 0));
        assert!(b < a && a < StreamId::new(6, 0));
        assert_eq!(a.to_string(), "5-3");
        assert!("x-1".parse::<StreamId>().is_err());
    }
}
