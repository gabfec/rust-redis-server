use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// DISCARD
pub struct Discard;

impl Command for Discard {
    fn parse(args: Args) -> Result<Self, CmdError> {
        args.finish()?;
        Ok(Discard)
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        if ctx.conn.transaction.take().is_none() {
            return Err(CmdError::Other("ERR DISCARD without MULTI".into()));
        }
        Ok(RespValue::ok())
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::{err, Client};
    use crate::resp::RespValue;

    #[test]
    fn discard_drops_the_queue() {
        let mut c = Client::default();
        c.cmd(&["MULTI"]);
        c.cmd(&["SET", "foo", "41"]);
        assert_eq!(c.cmd(&["DISCARD"]), RespValue::ok());
        assert_eq!(c.cmd(&["GET", "foo"]), RespValue::NullBulk, "runs right away again");
        assert_eq!(c.cmd(&["EXEC"]), err("ERR EXEC without MULTI"));
    }

    #[test]
    fn discard_without_multi() {
        assert_eq!(Client::default().cmd(&["DISCARD"]), err("ERR DISCARD without MULTI"));
    }
}
