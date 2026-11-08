use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;
use crate::store::Value;

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
        let Some(value) = store.get_string_mut(&self.key)? else {
            // A missing key counts as 0.
            store.set(self.key, Value::String("1".into()), None);
            return Ok(RespValue::Integer(1));
        };
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

    #[test]
    fn missing_key_starts_at_zero() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["INCR", "n"]), RespValue::Integer(1));
        assert_eq!(c.cmd(&["INCR", "n"]), RespValue::Integer(2));
    }
}
