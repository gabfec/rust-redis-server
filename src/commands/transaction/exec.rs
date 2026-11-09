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
        if tx.aborted {
            return Err(CmdError::Other(
                "EXECABORT Transaction discarded because of previous errors.".into(),
            ));
        }
        // A command that fails here only puts its error in the replies;
        // the others still run (Redis has no rollback).
        let replies = ctx.atomically(|ctx| {
            tx.queued.into_iter().map(|(handler, args)| handler(args, ctx)).collect()
        });
        Ok(RespValue::Array(replies))
    }
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
}
