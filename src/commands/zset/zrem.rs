use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// ZREM key member [member ...]
/// Returns how many members were removed. A set left empty is deleted.
pub struct Zrem {
    key: String,
    members: Vec<String>,
}

impl Command for Zrem {
    const IS_WRITE: bool = true;

    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let key = args.next_string()?;
        let members = args.rest_min(1)?;
        Ok(Zrem { key, members })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let mut store = ctx.store();
        let Some(set) = store.get_zset_mut(&self.key)? else {
            return Ok(RespValue::Integer(0));
        };
        let removed = self.members.iter().filter(|member| set.remove(member)).count();
        if set.len() == 0 {
            store.remove(&self.key);
        }
        Ok(RespValue::Integer(removed as i64))
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::Client;
    use crate::resp::RespValue;

    #[test]
    fn removes_members() {
        let mut c = Client::default();
        for (score, member) in [("80.5", "foo"), ("50.3", "baz"), ("80.5", "bar")] {
            c.cmd(&["ZADD", "zset_key", score, member]);
        }
        assert_eq!(c.cmd(&["ZREM", "zset_key", "baz"]), RespValue::Integer(1));
        assert_eq!(c.cmd(&["ZRANGE", "zset_key", "0", "-1"]), RespValue::bulk_array(["bar", "foo"]));
        assert_eq!(c.cmd(&["ZRANK", "zset_key", "foo"]), RespValue::Integer(1));
        assert_eq!(c.cmd(&["ZREM", "zset_key", "missing_member"]), RespValue::Integer(0));
        assert_eq!(c.cmd(&["ZREM", "missing_key", "foo"]), RespValue::Integer(0));
    }

    #[test]
    fn an_emptied_set_is_deleted() {
        let mut c = Client::default();
        c.cmd(&["ZADD", "z", "1", "a", "2", "b"]);
        assert_eq!(c.cmd(&["ZREM", "z", "a", "b", "c"]), RespValue::Integer(2));
        assert_eq!(c.cmd(&["TYPE", "z"]), RespValue::Simple("none".into()));
    }
}
