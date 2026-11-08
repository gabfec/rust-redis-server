use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// INCR key
pub struct Incr {
    key: String,
}

impl Command for Incr {
    const IS_WRITE: bool = true;

    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let key = args.next_string()?;
        args.finish()?;
        Ok(Incr { key })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let mut store = ctx.server.store();
        // Update in place so the key keeps its TTL.
        let value = store.get_string_mut(&self.key)?.ok_or(CmdError::NotInteger)?;
        let n = value.parse::<i64>().map_err(|_| CmdError::NotInteger)? + 1;
        *value = n.to_string();
        Ok(RespValue::Integer(n))
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::Client;
    use crate::resp::RespValue;

    #[test]
    fn increments_existing_integer() {
        let mut c = Client::default();
        c.cmd(&["SET", "n", "41"]);
        assert_eq!(c.cmd(&["INCR", "n"]), RespValue::Integer(42));
        assert_eq!(c.cmd(&["INCR", "n"]), RespValue::Integer(43));
        assert_eq!(c.cmd(&["GET", "n"]), RespValue::bulk("43"));
    }
}
