pub mod discard;
pub mod exec;
pub mod multi;

use super::{Args, Handler};

/// Commands queued between MULTI and EXEC.
#[derive(Default)]
pub struct Transaction {
    pub queued: Vec<(Handler, Args)>,
    /// A command was rejected while queueing: EXEC will refuse to run.
    pub aborted: bool,
}

/// Commands that run right away even inside MULTI (instead of being queued).
pub fn runs_immediately(name: &str) -> bool {
    matches!(name, "multi" | "exec" | "discard")
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::Client;
    use crate::resp::RespValue;

    #[test]
    fn commands_after_multi_are_queued_not_run() {
        let mut c = Client::default();
        c.cmd(&["MULTI"]);
        assert_eq!(c.cmd(&["SET", "foo", "41"]), RespValue::Simple("QUEUED".into()));
        assert_eq!(c.cmd(&["INCR", "foo"]), RespValue::Simple("QUEUED".into()));
        assert_eq!(c.other(&["GET", "foo"]), RespValue::NullBulk);
        assert_eq!(c.conn.transaction.as_ref().unwrap().queued.len(), 2);
    }
}
