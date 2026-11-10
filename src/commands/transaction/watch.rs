use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// WATCH key [key ...]
pub struct Watch;

impl Command for Watch {
    fn parse(args: Args) -> Result<Self, CmdError> {
        // TODO: remember the keys, to abort EXEC when one of them changes.
        args.rest_min(1)?;
        Ok(Watch)
    }

    fn execute(self, _ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        Ok(RespValue::ok())
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::{err, Client};
    use crate::resp::RespValue;

    #[test]
    fn watch() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["WATCH", "a"]), RespValue::ok());
        assert_eq!(c.cmd(&["WATCH", "a", "b"]), RespValue::ok());
        assert_eq!(c.cmd(&["WATCH"]), err("ERR wrong number of arguments for 'watch' command"));
    }
}
