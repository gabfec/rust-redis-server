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

    pub fn score(&self, member: &str) -> Option<f64> {
        self.scores.get(member).copied()
    }

    pub fn len(&self) -> usize {
        self.scores.len()
    }

    /// Members at positions `start..=stop`, in order.
    pub fn range(&self, start: usize, stop: usize) -> impl Iterator<Item = &str> {
        self.order.iter().skip(start).take(stop.saturating_sub(start) + 1).map(|(_, m)| m.as_str())
    }

    /// Returns true if the member was there.
    pub fn remove(&mut self, member: &str) -> bool {
        match self.scores.remove(member) {
            Some(score) => self.order.remove(&(Score(score), member.to_string())),
            None => false,
        }
    }

    /// 0-based position of the member in the order.
    pub fn rank(&self, member: &str) -> Option<usize> {
        let score = *self.scores.get(member)?;
        Some(self.order.range(..(Score(score), member.to_string())).count())
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

    #[test]
    fn rank_orders_equal_scores_by_member() {
        let mut set = SortedSet::default();
        set.insert("member_with_score_1".into(), 1.0);
        set.insert("member_with_score_2".into(), 2.0);
        set.insert("another_member_with_score_2".into(), 2.0);
        assert_eq!(set.rank("member_with_score_1"), Some(0));
        assert_eq!(set.rank("another_member_with_score_2"), Some(1));
        assert_eq!(set.rank("member_with_score_2"), Some(2));
        assert_eq!(set.rank("missing"), None);
    }
}
