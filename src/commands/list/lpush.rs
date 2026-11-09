use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// LPUSH key element [element ...]
pub struct Lpush {
    key: String,
    values: Vec<String>,
}

impl Command for Lpush {
    const IS_WRITE: bool = true;

    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let key = args.next_string()?;
        let values = args.rest_min(1)?;
        Ok(Lpush { key, values })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let mut store = ctx.store();
        let list = store.list_or_create(self.key)?;
        for value in self.values {
            list.push_front(value);
        }
        Ok(RespValue::Integer(list.len() as i64))
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::Client;
    use crate::resp::RespValue;

    #[test]
    fn prepends_each_value_in_turn() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["LPUSH", "l", "c"]), RespValue::Integer(1));
        assert_eq!(c.cmd(&["LPUSH", "l", "b", "a"]), RespValue::Integer(3));
        assert_eq!(c.cmd(&["LRANGE", "l", "0", "-1"]), RespValue::bulk_array(["a", "b", "c"]));
    }
}
