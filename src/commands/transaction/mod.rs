pub mod discard;
pub mod exec;
pub mod multi;
pub mod unwatch;
pub mod watch;

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
    matches!(name, "multi" | "exec" | "discard" | "watch")
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::{run, Client};
    use crate::connection::ConnState;
    use crate::resp::RespValue;
    use crate::server::ServerState;

    #[test]
    fn commands_after_multi_are_queued_not_run() {
        let mut c = Client::default();
        c.cmd(&["MULTI"]);
        assert_eq!(c.cmd(&["SET", "foo", "41"]), RespValue::Simple("QUEUED".into()));
        assert_eq!(c.cmd(&["INCR", "foo"]), RespValue::Simple("QUEUED".into()));
        assert_eq!(c.other(&["GET", "foo"]), RespValue::NullBulk);
        assert_eq!(c.conn.transaction.as_ref().unwrap().queued.len(), 2);
    }

    #[test]
    fn each_connection_has_its_own_transaction() {
        let server = ServerState::default();
        let (mut a, mut b) = (ConnState::default(), ConnState::default());
        run(&server, &mut a, &["MULTI"]);
        run(&server, &mut b, &["MULTI"]);
        run(&server, &mut a, &["INCR", "n"]);
        run(&server, &mut b, &["INCR", "n"]);
        run(&server, &mut b, &["INCR", "n"]);
        assert_eq!(run(&server, &mut b, &["EXEC"]), RespValue::Array(vec![
            RespValue::Integer(1),
            RespValue::Integer(2),
        ]));
        assert_eq!(run(&server, &mut a, &["EXEC"]), RespValue::Array(vec![RespValue::Integer(3)]));
    }

    #[test]
    fn concurrent_transactions_do_not_interleave() {
        const CLIENTS: usize = 8;
        const INCRS: usize = 50;
        let server = ServerState::default();
        let replies: Vec<RespValue> = std::thread::scope(|s| {
            let handles: Vec<_> = (0..CLIENTS)
                .map(|_| {
                    s.spawn(|| {
                        let mut conn = ConnState::default();
                        run(&server, &mut conn, &["MULTI"]);
                        for _ in 0..INCRS {
                            run(&server, &mut conn, &["INCR", "n"]);
                        }
                        run(&server, &mut conn, &["EXEC"])
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });

        for reply in replies {
            let RespValue::Array(values) = reply else { panic!("{reply:?}") };
            let values: Vec<i64> = values
                .into_iter()
                .map(|v| match v {
                    RespValue::Integer(i) => i,
                    other => panic!("{other:?}"),
                })
                .collect();
            // No other client's INCR ran in the middle of this EXEC.
            assert!(values.windows(2).all(|w| w[1] == w[0] + 1), "{values:?}");
        }
        assert_eq!(
            run(&server, &mut ConnState::default(), &["GET", "n"]),
            RespValue::bulk((CLIENTS * INCRS).to_string())
        );
    }
}
