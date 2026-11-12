use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// ZSCORE key member
/// The member's score as a string ("30.1", "20", "inf"), or nil.
pub struct Zscore {
    key: String,
    member: String,
}

impl Command for Zscore {
    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let key = args.next_string()?;
        let member = args.next_string()?;
        args.finish()?;
        Ok(Zscore { key, member })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let mut store = ctx.store();
        let score = store.get_zset(&self.key)?.and_then(|set| set.score(&self.member));
        // Rust prints the shortest form that reads back as the same f64,
        // like Redis.
        Ok(score.map_or(RespValue::NullBulk, |score| RespValue::bulk(score.to_string())))
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::Client;
    use crate::resp::RespValue;

    #[test]
    fn scores() {
        let mut c = Client::default();
        for (score, member) in [("20.0", "m1"), ("30.1", "m2"), ("40.2", "m3"), ("-inf", "m4")] {
            c.cmd(&["ZADD", "zset_key", score, member]);
        }
        assert_eq!(c.cmd(&["ZSCORE", "zset_key", "m2"]), RespValue::bulk("30.1"));
        assert_eq!(c.cmd(&["ZSCORE", "zset_key", "m1"]), RespValue::bulk("20"));
        assert_eq!(c.cmd(&["ZSCORE", "zset_key", "m4"]), RespValue::bulk("-inf"));
        c.cmd(&["ZADD", "zset_key", "100.99", "m2"]);
        assert_eq!(c.cmd(&["ZSCORE", "zset_key", "m2"]), RespValue::bulk("100.99"));
        assert_eq!(c.cmd(&["ZSCORE", "zset_key", "m100"]), RespValue::NullBulk);
        assert_eq!(c.cmd(&["ZSCORE", "missing_key", "member"]), RespValue::NullBulk);
    }
}
