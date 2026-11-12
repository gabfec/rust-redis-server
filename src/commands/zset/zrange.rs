use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// ZRANGE key start stop   (0-based positions, `stop` included)
pub struct Zrange {
    key: String,
    start: usize,
    stop: usize,
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
        let stop = self.stop.min(set.len().saturating_sub(1));
        if self.start >= set.len() || self.start > stop {
            return Ok(RespValue::Array(vec![]));
        }
        Ok(RespValue::bulk_array(set.range(self.start, stop)))
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
}
