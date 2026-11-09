use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// LPOP key [count]
pub struct Lpop {
    key: String,
    count: Option<usize>,
}

impl Command for Lpop {
    const IS_WRITE: bool = true;

    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let key = args.next_string()?;
        let count = match args.next_opt() {
            Some(n) => Some(n.parse().map_err(|_| CmdError::NotInteger)?),
            None => None,
        };
        args.finish()?;
        Ok(Lpop { key, count })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let mut store = ctx.store();
        let Some(list) = store.get_list_mut(&self.key)? else {
            return Ok(RespValue::NullBulk);
        };
        Ok(match self.count {
            None => list.pop_front().map_or(RespValue::NullBulk, RespValue::bulk),
            Some(n) => {
                let n = n.min(list.len());
                RespValue::bulk_array(list.drain(..n))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::Client;
    use crate::resp::RespValue;

    #[test]
    fn pops_one() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["LPOP", "l"]), RespValue::NullBulk);
        c.cmd(&["RPUSH", "l", "a", "b"]);
        assert_eq!(c.cmd(&["LPOP", "l"]), RespValue::bulk("a"));
        assert_eq!(c.cmd(&["LPOP", "l"]), RespValue::bulk("b"));
        assert_eq!(c.cmd(&["LPOP", "l"]), RespValue::NullBulk);
    }

    #[test]
    fn pops_count() {
        let mut c = Client::default();
        c.cmd(&["RPUSH", "l", "a", "b", "c"]);
        assert_eq!(c.cmd(&["LPOP", "l", "2"]), RespValue::bulk_array(["a", "b"]));
        assert_eq!(c.cmd(&["LPOP", "l", "10"]), RespValue::bulk_array(["c"]));
        assert_eq!(c.cmd(&["LLEN", "l"]), RespValue::Integer(0));
    }
}
