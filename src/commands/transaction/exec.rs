use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// EXEC
pub struct Exec;

impl Command for Exec {
    fn parse(args: Args) -> Result<Self, CmdError> {
        args.finish()?;
        Ok(Exec)
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let Some(tx) = ctx.conn.transaction.take() else {
            return Err(CmdError::Other("ERR EXEC without MULTI".into()));
        };
        // EXEC always ends the WATCH, whatever the outcome.
        let watched = std::mem::take(&mut ctx.conn.watched);
        if tx.aborted {
            return Err(CmdError::Other(
                "EXECABORT Transaction discarded because of previous errors.".into(),
            ));
        }
        // A command that fails here only puts its error in the replies;
        // the others still run (Redis has no rollback).
        let replies = ctx.atomically(|ctx| {
            // Checked under the same lock, so nothing can change in between.
            if watched_key_changed(ctx, &watched) {
                return None;
            }
            Some(tx.queued.into_iter().map(|(handler, args)| handler(args, ctx)).collect())
        });
        // A watched key changed: nothing ran, reply with a null array.
        Ok(replies.map_or(RespValue::NullArray, RespValue::Array))
    }
}

fn watched_key_changed(ctx: &mut Ctx, watched: &[(String, u64)]) -> bool {
    let mut store = ctx.store();
    watched.iter().any(|(key, version)| store.version(key) != *version)
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::Client;
    use crate::resp::RespValue;

    #[test]
    fn exec_without_multi_is_an_error() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["EXEC"]), RespValue::Error("ERR EXEC without MULTI".into()));
    }

    #[test]
    fn exec_ends_the_transaction() {
        let mut c = Client::default();
        c.cmd(&["MULTI"]);
        assert_eq!(c.cmd(&["EXEC"]), RespValue::Array(vec![]));
        assert!(c.conn.transaction.is_none());
        assert_eq!(c.cmd(&["EXEC"]), RespValue::Error("ERR EXEC without MULTI".into()));
    }

    #[test]
    fn exec_runs_queued_commands_in_order() {
        let mut c = Client::default();
        c.cmd(&["MULTI"]);
        c.cmd(&["SET", "foo", "41"]);
        c.cmd(&["INCR", "foo"]);
        c.cmd(&["GET", "foo"]);
        assert_eq!(
            c.cmd(&["EXEC"]),
            RespValue::Array(vec![RespValue::ok(), RespValue::Integer(42), RespValue::bulk("42")])
        );
        assert_eq!(c.other(&["GET", "foo"]), RespValue::bulk("42"));
    }

    #[test]
    fn a_failing_command_does_not_stop_the_others() {
        let mut c = Client::default();
        c.cmd(&["SET", "foo", "xyz"]);
        c.cmd(&["MULTI"]);
        c.cmd(&["INCR", "foo"]);
        c.cmd(&["SET", "bar", "7"]);
        c.cmd(&["INCR", "bar"]);
        assert_eq!(
            c.cmd(&["EXEC"]),
            RespValue::Array(vec![
                RespValue::Error("ERR value is not an integer or out of range".into()),
                RespValue::ok(),
                RespValue::Integer(8),
            ])
        );
    }

    #[test]
    fn unknown_command_aborts_the_transaction() {
        let mut c = Client::default();
        c.cmd(&["MULTI"]);
        assert_eq!(c.cmd(&["NOPE"]), RespValue::Error("ERR unknown command 'nope'".into()));
        assert_eq!(c.cmd(&["SET", "a", "1"]), RespValue::Simple("QUEUED".into()));
        assert_eq!(
            c.cmd(&["EXEC"]),
            RespValue::Error("EXECABORT Transaction discarded because of previous errors.".into())
        );
        assert_eq!(c.cmd(&["GET", "a"]), RespValue::NullBulk);
        assert!(c.conn.transaction.is_none());
    }

    #[test]
    fn blocking_commands_do_not_block_inside_exec() {
        let mut c = Client::default();
        c.cmd(&["MULTI"]);
        c.cmd(&["BLPOP", "list", "0"]);
        c.cmd(&["XREAD", "BLOCK", "0", "STREAMS", "s", "$"]);
        assert_eq!(
            c.cmd(&["EXEC"]),
            RespValue::Array(vec![RespValue::NullArray, RespValue::NullArray])
        );
    }

    #[test]
    fn exec_unwatches_all_keys() {
        let mut c = Client::default();
        for first_exec_succeeds in [true, false] {
            c.cmd(&["WATCH", "k"]);
            if !first_exec_succeeds {
                c.other(&["SET", "k", "changed"]);
            }
            c.cmd(&["MULTI"]);
            c.cmd(&["SET", "k", "first"]);
            c.cmd(&["EXEC"]);

            // k changes after EXEC: the next transaction must not care.
            c.other(&["SET", "k", "changed again"]);
            c.cmd(&["MULTI"]);
            c.cmd(&["SET", "k", "second"]);
            assert_eq!(c.cmd(&["EXEC"]), RespValue::Array(vec![RespValue::ok()]));
        }
    }
}
