use super::parse_score;
use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// ZADD key score member
/// Returns how many members were added (not counting updated scores).
pub struct Zadd {
    key: String,
    score: f64,
    member: String,
}

impl Command for Zadd {
    const IS_WRITE: bool = true;

    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let key = args.next_string()?;
        let score = parse_score(&args.next_string()?)?;
        let member = args.next_string()?;
        args.finish()?;
        Ok(Zadd { key, score, member })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let mut store = ctx.store();
        let added = store.zset_or_create(self.key)?.insert(self.member, self.score);
        Ok(RespValue::Integer(added as i64))
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::{err, Client};
    use crate::resp::RespValue;

    #[test]
    fn creates_a_sorted_set() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["ZADD", "zset_key", "10.0", "zset_member"]), RespValue::Integer(1));
        assert_eq!(c.cmd(&["TYPE", "zset_key"]), RespValue::Simple("zset".into()));
    }

    #[test]
    fn bad_arguments() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["ZADD", "z", "abc", "m"]), err("ERR value is not a valid float"));
        assert_eq!(c.cmd(&["ZADD", "z", "nan", "m"]), err("ERR value is not a valid float"));
        c.cmd(&["SET", "s", "x"]);
        assert_eq!(
            c.cmd(&["ZADD", "s", "1", "m"]),
            err("WRONGTYPE Operation against a key holding the wrong kind of value")
        );
    }
}
