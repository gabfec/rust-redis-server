use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// LRANGE key start stop   (inclusive, negative indexes count from the end)
pub struct Lrange {
    key: String,
    start: i64,
    stop: i64,
}

impl Command for Lrange {
    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let key = args.next_string()?;
        let start = args.next_parse()?;
        let stop = args.next_parse()?;
        args.finish()?;
        Ok(Lrange { key, start, stop })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let mut store = ctx.server.store();
        let Some(list) = store.get_list(&self.key)? else {
            return Ok(RespValue::Array(vec![]));
        };

        let len = list.len() as i64;
        let start = (if self.start < 0 { len + self.start } else { self.start }).clamp(0, len) as usize;
        let stop = (if self.stop < 0 { len + self.stop } else { self.stop }).clamp(0, len - 1) as usize;
        if start >= list.len() || start > stop {
            return Ok(RespValue::Array(vec![]));
        }
        Ok(RespValue::bulk_array(list[start..=stop].iter().cloned()))
    }
}
