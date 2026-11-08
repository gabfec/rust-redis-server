use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// RPUSH key element [element ...]
pub struct Rpush {
    key: String,
    values: Vec<String>,
}

impl Command for Rpush {
    const IS_WRITE: bool = true;

    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let key = args.next_string()?;
        let values = args.rest_min(1)?;
        Ok(Rpush { key, values })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let mut store = ctx.server.store();
        let list = store.list_or_create(self.key)?;
        list.extend(self.values);
        Ok(RespValue::Integer(list.len() as i64))
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::{err, Client};
    use crate::resp::RespValue;

    #[test]
    fn appends_and_returns_length() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["RPUSH", "l", "a"]), RespValue::Integer(1));
        assert_eq!(c.cmd(&["RPUSH", "l", "b", "c"]), RespValue::Integer(3));
        assert_eq!(c.cmd(&["LRANGE", "l", "0", "-1"]), RespValue::bulk_array(["a", "b", "c"]));
    }

    #[test]
    fn errors() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["RPUSH", "l"]), err("ERR wrong number of arguments for 'rpush' command"));
        c.cmd(&["SET", "s", "v"]);
        assert_eq!(
            c.cmd(&["RPUSH", "s", "x"]),
            err("WRONGTYPE Operation against a key holding the wrong kind of value")
        );
    }
}
