//! Offset tracking for one Kafka partition.
//!
//! **The problem.** Kafka remembers a consumer group's progress as a single
//! number per partition: the *committed offset*, "everything before this has
//! been handled". But our lanes finish messages out of order: offset 7 (a fast
//! endpoint) can finish before offset 5 (a slow one). If we committed 8 after
//! 7 finished and then crashed, offset 5 would never be redelivered: lost.
//!
//! **The fix.** Track which offsets are still in flight, and only ever commit
//! up to the first unfinished one (the *contiguous prefix*). After a crash,
//! anything from that point on is redelivered. Some of it may already have
//! been delivered, and the claim step in Postgres skips those.

use std::collections::BTreeSet;

#[derive(Debug, Default)]
pub struct OffsetTracker {
    in_flight: BTreeSet<i64>,
    highest_seen: Option<i64>,
    last_committed: Option<i64>,
}

impl OffsetTracker {
    pub fn new() -> OffsetTracker {
        OffsetTracker::default()
    }

    /// A message at `offset` has been received and handed to a lane.
    pub fn start(&mut self, offset: i64) {
        self.in_flight.insert(offset);
        self.highest_seen = Some(self.highest_seen.map_or(offset, |h| h.max(offset)));
    }

    /// The message at `offset` is completely handled.
    pub fn finish(&mut self, offset: i64) {
        self.in_flight.remove(&offset);
    }

    pub fn in_flight(&self) -> usize {
        self.in_flight.len()
    }

    /// The offset Kafka should resume from: the first unfinished message, or
    /// one past the highest seen if everything is finished. (Kafka's convention
    /// is to commit the offset of the *next* message to read.)
    pub fn committable(&self) -> Option<i64> {
        match self.in_flight.first() {
            Some(first_unfinished) => Some(*first_unfinished),
            None => self.highest_seen.map(|h| h + 1),
        }
    }

    /// Returns the offset to commit if it moved forward since the last commit.
    pub fn take_commit(&mut self) -> Option<i64> {
        let c = self.committable()?;
        if self.last_committed.is_some_and(|last| c <= last) {
            return None;
        }
        self.last_committed = Some(c);
        Some(c)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nothing_to_commit_before_any_message() {
        let mut t = OffsetTracker::new();
        assert_eq!(t.take_commit(), None);
    }

    #[test]
    fn out_of_order_completion_commits_only_the_contiguous_prefix() {
        let mut t = OffsetTracker::new();
        for o in 10..15 {
            t.start(o);
        }
        // 12, 13, 14 finish first: 10 is still in flight, so we may not move.
        t.finish(12);
        t.finish(14);
        t.finish(13);
        assert_eq!(t.committable(), Some(10));
        // 10 finishes: now 11 is the first unfinished one.
        t.finish(10);
        assert_eq!(t.committable(), Some(11));
        // 11 finishes: everything up to 14 is done -> resume from 15.
        t.finish(11);
        assert_eq!(t.committable(), Some(15));
    }

    #[test]
    fn take_commit_only_reports_progress() {
        let mut t = OffsetTracker::new();
        t.start(0);
        t.start(1);
        assert_eq!(t.take_commit(), Some(0));
        assert_eq!(t.take_commit(), None, "no progress, no new commit");
        t.finish(1);
        assert_eq!(t.take_commit(), None, "0 still unfinished");
        t.finish(0);
        assert_eq!(t.take_commit(), Some(2));
        assert_eq!(t.take_commit(), None);
    }

    #[test]
    fn gaps_in_offsets_are_fine() {
        // Offsets are not always contiguous (e.g. transaction markers, compaction).
        let mut t = OffsetTracker::new();
        t.start(5);
        t.start(9);
        t.finish(9);
        assert_eq!(t.committable(), Some(5));
        t.finish(5);
        assert_eq!(t.committable(), Some(10));
    }

    #[test]
    fn in_flight_count() {
        let mut t = OffsetTracker::new();
        t.start(1);
        t.start(2);
        assert_eq!(t.in_flight(), 2);
        t.finish(1);
        assert_eq!(t.in_flight(), 1);
    }
}
