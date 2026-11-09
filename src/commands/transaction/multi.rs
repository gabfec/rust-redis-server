use crate::commands::{Args, CmdError, Command, Ctx, Transaction};
use crate::resp::RespValue;

/// MULTI
pub struct Multi;

impl Command for Multi {
    fn parse(args: Args) -> Result<Self, CmdError> {
        args.finish()?;
        Ok(Multi)
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        if ctx.conn.transaction.is_some() {
            return Err(CmdError::Other("ERR MULTI calls can not be nested".into()));
        }
        ctx.conn.transaction = Some(Transaction::default());
        Ok(RespValue::ok())
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::Client;
    use crate::resp::RespValue;

    #[test]
    fn starts_a_transaction() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["MULTI"]), RespValue::ok());
        assert!(c.conn.transaction.is_some());
        assert_eq!(
            c.cmd(&["MULTI"]),
            RespValue::Error("ERR MULTI calls can not be nested".into())
        );
    }
}
