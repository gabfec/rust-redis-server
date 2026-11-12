use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// ZRANGE key start stop   (0-based positions, `stop` included; negative
/// positions count from the end: -1 is the last member)
pub struct Zrange {
    key: String,
    start: i64,
    stop: i64,
}

impl Command for Zrange {
    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let key = args.next_string()?;
        let start = args.next_parse()?;
        let stop = args.next_parse()?;
        args.finish()?;
        Ok(Zrange { key, start, stop })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let mut store = ctx.store();
        let Some(set) = store.get_zset(&self.key)? else {
            return Ok(RespValue::Array(vec![]));
        };
        let len = set.len() as i64;
        let from_end = |i: i64| if i < 0 { len + i } else { i };
        // Like Redis: a start too far back means the first member, but a
        // stop too far back stays before it (so the range is empty).
        let start = from_end(self.start).max(0);
        let stop = from_end(self.stop).min(len - 1);
        if start > stop {
            return Ok(RespValue::Array(vec![]));
        }
        Ok(RespValue::bulk_array(set.range(start as usize, stop as usize)))
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::Client;
    use crate::commands::zset::zrank::tests::client_with_zset;
    use crate::resp::RespValue;

    fn zrange(c: &mut Client, start: &str, stop: &str) -> RespValue {
        c.cmd(&["ZRANGE", "zset_key", start, stop])
    }

    #[test]
    fn ranges() {
        let mut c = client_with_zset(); // baz caz paz bar foo
        assert_eq!(zrange(&mut c, "2", "4"), RespValue::bulk_array(["paz", "bar", "foo"]));
        assert_eq!(zrange(&mut c, "0", "1"), RespValue::bulk_array(["baz", "caz"]));
        assert_eq!(zrange(&mut c, "3", "100"), RespValue::bulk_array(["bar", "foo"]));
        assert_eq!(zrange(&mut c, "5", "10"), RespValue::Array(vec![]));
        assert_eq!(zrange(&mut c, "3", "1"), RespValue::Array(vec![]));
        assert_eq!(c.cmd(&["ZRANGE", "missing", "0", "1"]), RespValue::Array(vec![]));
    }

    #[test]
    fn negative_indexes() {
        let mut c = Client::default();
        for (score, member) in [("20.0", "foo"), ("30.1", "bar"), ("40.2", "baz"), ("25.0", "paz"), ("25.0", "caz")] {
            c.cmd(&["ZADD", "zset_key", score, member]);
        }
        // Order: foo caz paz bar baz
        assert_eq!(zrange(&mut c, "2", "-1"), RespValue::bulk_array(["paz", "bar", "baz"]));
        assert_eq!(zrange(&mut c, "-2", "-1"), RespValue::bulk_array(["bar", "baz"]));
        assert_eq!(zrange(&mut c, "0", "-3"), RespValue::bulk_array(["foo", "caz", "paz"]));
        assert_eq!(zrange(&mut c, "-100", "1"), RespValue::bulk_array(["foo", "caz"]));
        assert_eq!(zrange(&mut c, "-1", "-2"), RespValue::Array(vec![]));
        assert_eq!(zrange(&mut c, "0", "-100"), RespValue::Array(vec![]), "as in Redis");
    }
}
