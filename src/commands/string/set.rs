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
        ctx.server.store().set(self.key, Value::String(self.value), self.ttl);
        Ok(RespValue::ok())
    }
}
