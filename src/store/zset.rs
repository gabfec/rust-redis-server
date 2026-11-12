use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};

/// A sorted set: unique members, each with a score, ordered by score,
/// then by member (lexicographically) for equal scores.
#[derive(Default)]
pub struct SortedSet {
    scores: HashMap<String, f64>,
    /// The same members, in order.
    order: BTreeSet<(Score, String)>,
}

impl SortedSet {
    /// Adds the member, or updates its score. Returns true if it is new.
    pub fn insert(&mut self, member: String, score: f64) -> bool {
        let old = self.scores.insert(member.clone(), score);
        if let Some(old) = old {
            self.order.remove(&(Score(old), member.clone()));
        }
        self.order.insert((Score(score), member));
        old.is_none()
    }
}

/// A score that can be ordered. Never NaN (commands reject it).
#[derive(Clone, Copy, Debug, PartialEq)]
struct Score(f64);

impl Eq for Score {}

impl PartialOrd for Score {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Score {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_update() {
        let mut set = SortedSet::default();
        assert!(set.insert("a".into(), 2.0));
        assert!(set.insert("b".into(), 1.0));
        assert!(!set.insert("a".into(), 0.5), "an update, not a new member");
        let order: Vec<_> = set.order.iter().map(|(_, m)| m.as_str()).collect();
        assert_eq!(order, ["a", "b"]);
        assert_eq!(set.scores.len(), 2);
    }
}
