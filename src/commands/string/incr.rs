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
        let mut store = ctx.store();
        // Update in place so the key keeps its TTL.
        let Some(value) = store.get_string_mut(&self.key)? else {
            // A missing key counts as 0.
            store.set(self.key, Value::String(b"1".to_vec()), None);
            return Ok(RespValue::Integer(1));
        };
        let n = std::str::from_utf8(value)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .ok_or(CmdError::NotInteger)?
            .checked_add(1)
            .ok_or_else(|| CmdError::Other("ERR increment or decrement would overflow".into()))?;
        *value = n.to_string().into_bytes();
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

    #[test]
    fn rejects_non_integer_values() {
        let mut c = Client::default();
        let not_integer = RespValue::Error("ERR value is not an integer or out of range".into());
        for bad in ["abc", "1.5", "", "99999999999999999999"] {
            c.cmd(&["SET", "n", bad]);
            assert_eq!(c.cmd(&["INCR", "n"]), not_integer, "value {bad:?}");
            assert_eq!(c.cmd(&["GET", "n"]), RespValue::bulk(bad), "value unchanged");
        }
    }

    #[test]
    fn rejects_overflow_and_wrong_type() {
        let mut c = Client::default();
        c.cmd(&["SET", "n", &i64::MAX.to_string()]);
        assert_eq!(
            c.cmd(&["INCR", "n"]),
            RespValue::Error("ERR increment or decrement would overflow".into())
        );
        c.cmd(&["RPUSH", "l", "x"]);
        assert!(matches!(c.cmd(&["INCR", "l"]), RespValue::Error(e) if e.starts_with("WRONGTYPE")));
    }
}
