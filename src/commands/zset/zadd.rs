use super::parse_score;
use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// ZADD key score member [score member ...]
/// Adds members, or updates the scores of existing ones. Returns how many
/// members were added (not counting updated scores).
pub struct Zadd {
    key: String,
    members: Vec<(f64, String)>,
}

impl Command for Zadd {
    const IS_WRITE: bool = true;

    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let key = args.next_string()?;
        let wrong_args = args.wrong_args();
        let pairs = args.rest_min(2)?;
        if pairs.len() % 2 != 0 {
            return Err(wrong_args);
        }
        let members = pairs
            .chunks(2)
            .map(|pair| Ok((parse_score(&pair[0])?, pair[1].clone())))
            .collect::<Result<_, CmdError>>()?;
        Ok(Zadd { key, members })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let mut store = ctx.store();
        let set = store.zset_or_create(self.key)?;
        let added = self.members.into_iter().filter(|(score, member)| set.insert(member.clone(), *score)).count();
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
    fn adds_members_and_updates_scores() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["ZADD", "z", "20.0", "m1"]), RespValue::Integer(1));
        assert_eq!(c.cmd(&["ZADD", "z", "30.1", "m2"]), RespValue::Integer(1));
        assert_eq!(c.cmd(&["ZADD", "z", "40.2", "m3"]), RespValue::Integer(1));
        assert_eq!(c.cmd(&["ZADD", "z", "100.0", "m1"]), RespValue::Integer(0), "existing member updated");
        assert_eq!(c.cmd(&["ZADD", "z", "1", "m1", "2", "m4", "3", "m5"]), RespValue::Integer(2));
    }

    #[test]
    fn bad_arguments() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["ZADD", "z", "abc", "m"]), err("ERR value is not a valid float"));
        assert_eq!(c.cmd(&["ZADD", "z", "nan", "m"]), err("ERR value is not a valid float"));
        assert_eq!(c.cmd(&["ZADD", "z", "1", "m", "2"]), err("ERR wrong number of arguments for 'zadd' command"));
        assert_eq!(c.cmd(&["TYPE", "z"]), RespValue::Simple("none".into()), "nothing was created");
        c.cmd(&["SET", "s", "x"]);
        assert_eq!(
            c.cmd(&["ZADD", "s", "1", "m"]),
            err("WRONGTYPE Operation against a key holding the wrong kind of value")
        );
    }
}
