use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// LLEN key
pub struct Llen {
    key: String,
}

impl Command for Llen {
    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let key = args.next_string()?;
        args.finish()?;
        Ok(Llen { key })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let mut store = ctx.store();
        let len = store.get_list(&self.key)?.map_or(0, |l| l.len());
        Ok(RespValue::Integer(len as i64))
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::Client;
    use crate::resp::RespValue;

    #[test]
    fn length() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["LLEN", "l"]), RespValue::Integer(0));
        c.cmd(&["RPUSH", "l", "a", "b"]);
        assert_eq!(c.cmd(&["LLEN", "l"]), RespValue::Integer(2));
    }
}
