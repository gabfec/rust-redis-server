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
        let Some(_tx) = ctx.conn.transaction.take() else {
            return Err(CmdError::Other("ERR EXEC without MULTI".into()));
        };
        // TODO: run the queued commands. For now they are dropped.
        Ok(RespValue::Array(vec![]))
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
}
