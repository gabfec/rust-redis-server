use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// ZCARD key   (number of members, 0 if the key doesn't exist)
pub struct Zcard {
    key: String,
}

impl Command for Zcard {
    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let key = args.next_string()?;
        args.finish()?;
        Ok(Zcard { key })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let mut store = ctx.store();
        let len = store.get_zset(&self.key)?.map_or(0, |set| set.len());
        Ok(RespValue::Integer(len as i64))
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::Client;
    use crate::resp::RespValue;

    #[test]
    fn counts_members() {
        let mut c = Client::default();
        for (score, member) in [("20.0", "m1"), ("30.1", "m2"), ("40.2", "m3"), ("50.3", "m4")] {
            c.cmd(&["ZADD", "zset_key", score, member]);
        }
        assert_eq!(c.cmd(&["ZCARD", "zset_key"]), RespValue::Integer(4));
        c.cmd(&["ZADD", "zset_key", "100.0", "m1"]);
        assert_eq!(c.cmd(&["ZCARD", "zset_key"]), RespValue::Integer(4), "updating a score adds nothing");
        assert_eq!(c.cmd(&["ZCARD", "missing_key"]), RespValue::Integer(0));
    }
}
