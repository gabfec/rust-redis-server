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
        if !ctx.conn.in_multi {
            return Err(CmdError::Other("ERR EXEC without MULTI".into()));
        }
        ctx.conn.in_multi = false;
        // Nothing is queued yet, so there are no replies to return.
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
        assert!(!c.conn.in_multi);
        assert_eq!(c.cmd(&["EXEC"]), RespValue::Error("ERR EXEC without MULTI".into()));
    }
}
