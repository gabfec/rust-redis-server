use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// ZRANK key member
/// The member's 0-based rank by increasing score (ties: by member name),
/// or nil if the key or member doesn't exist.
pub struct Zrank {
    key: String,
    member: String,
}

impl Command for Zrank {
    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let key = args.next_string()?;
        let member = args.next_string()?;
        args.finish()?;
        Ok(Zrank { key, member })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let mut store = ctx.store();
        let rank = store.get_zset(&self.key)?.and_then(|set| set.rank(&self.member));
        Ok(rank.map_or(RespValue::NullBulk, |rank| RespValue::Integer(rank as i64)))
    }
}

#[cfg(test)]
pub mod tests {
    use crate::commands::test_util::Client;
    use crate::resp::RespValue;

    /// The sorted set from the stage examples: baz caz paz bar foo.
    pub fn client_with_zset() -> Client {
        let mut c = Client::default();
        for (score, member) in [("100.0", "foo"), ("100.0", "bar"), ("20.0", "baz"), ("30.1", "caz"), ("40.2", "paz")] {
            assert_eq!(c.cmd(&["ZADD", "zset_key", score, member]), RespValue::Integer(1));
        }
        c
    }

    #[test]
    fn ranks() {
        let mut c = client_with_zset();
        for (member, rank) in [("baz", 0), ("caz", 1), ("paz", 2), ("bar", 3), ("foo", 4)] {
            assert_eq!(c.cmd(&["ZRANK", "zset_key", member]), RespValue::Integer(rank), "{member}");
        }
        assert_eq!(c.cmd(&["ZRANK", "zset_key", "missing_member"]), RespValue::NullBulk);
        assert_eq!(c.cmd(&["ZRANK", "missing_key", "member"]), RespValue::NullBulk);
    }
}
