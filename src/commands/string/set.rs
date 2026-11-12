use std::time::Duration;

use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;
use crate::store::Value;

/// SET key value [EX seconds | PX milliseconds]
pub struct Set {
    key: String,
    value: String,
    ttl: Option<Duration>,
}

impl Command for Set {
    const IS_WRITE: bool = true;

    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let key = args.next_string()?;
        let value = args.next_string()?;
        let mut ttl = None;
        while let Some(opt) = args.next_opt() {
            match opt.to_ascii_uppercase().as_str() {
                "PX" => ttl = Some(Duration::from_millis(args.next_parse()?)),
                "EX" => ttl = Some(Duration::from_secs(args.next_parse()?)),
                _ => return Err(CmdError::Syntax),
            }
        }
        Ok(Set { key, value, ttl })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        ctx.store().set(self.key, Value::String(self.value.into_bytes()), self.ttl);
        Ok(RespValue::ok())
    }
}

#[cfg(test)]
mod tests {
    use std::thread::sleep;
    use std::time::Duration;

    use crate::commands::test_util::{err, Client};
    use crate::resp::RespValue;

    #[test]
    fn set_overwrites_any_type() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["SET", "k", "a"]), RespValue::ok());
        assert_eq!(c.cmd(&["SET", "k", "b"]), RespValue::ok());
        assert_eq!(c.cmd(&["GET", "k"]), RespValue::bulk("b"));
        c.cmd(&["RPUSH", "l", "x"]);
        c.cmd(&["SET", "l", "now a string"]);
        assert_eq!(c.cmd(&["GET", "l"]), RespValue::bulk("now a string"));
    }

    #[test]
    fn px_expires_the_key() {
        let mut c = Client::default();
        c.cmd(&["SET", "k", "v", "px", "50"]);
        assert_eq!(c.cmd(&["GET", "k"]), RespValue::bulk("v"));
        sleep(Duration::from_millis(60));
        assert_eq!(c.cmd(&["GET", "k"]), RespValue::NullBulk);
        assert_eq!(c.cmd(&["TYPE", "k"]), RespValue::Simple("none".into()));
    }

    #[test]
    fn plain_set_clears_the_ttl() {
        let mut c = Client::default();
        c.cmd(&["SET", "k", "v", "PX", "50"]);
        c.cmd(&["SET", "k", "v2"]);
        sleep(Duration::from_millis(60));
        assert_eq!(c.cmd(&["GET", "k"]), RespValue::bulk("v2"));
    }

    #[test]
    fn bad_options() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["SET", "k", "v", "XX"]), err("ERR syntax error"));
        assert_eq!(
            c.cmd(&["SET", "k", "v", "PX", "soon"]),
            err("ERR value is not an integer or out of range")
        );
        assert_eq!(c.cmd(&["SET", "k", "v", "EX"]), err("ERR wrong number of arguments for 'set' command"));
    }
}
