use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// UNWATCH
pub struct Unwatch;

impl Command for Unwatch {
    fn parse(args: Args) -> Result<Self, CmdError> {
        args.finish()?;
        Ok(Unwatch)
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        ctx.conn.watched.clear();
        Ok(RespValue::ok())
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::Client;
    use crate::resp::RespValue;

    #[test]
    fn unwatch_forgets_all_watched_keys() {
        let mut c = Client::default();
        c.cmd(&["WATCH", "a", "b"]);
        assert_eq!(c.cmd(&["UNWATCH"]), RespValue::ok());
        c.other(&["SET", "a", "changed"]);
        c.cmd(&["MULTI"]);
        c.cmd(&["SET", "a", "tx"]);
        assert_eq!(c.cmd(&["EXEC"]), RespValue::Array(vec![RespValue::ok()]));
    }

    #[test]
    fn unwatch_without_watch_is_fine() {
        assert_eq!(Client::default().cmd(&["UNWATCH"]), RespValue::ok());
    }
}
