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
        let normalize = |i: i64| if i < 0 { (len + i).max(0) } else { i };
        let start = normalize(self.start);
        let stop = normalize(self.stop).min(len - 1);
        if start > stop {
            return Ok(RespValue::Array(vec![]));
        }
        Ok(RespValue::bulk_array(list.range(start as usize..=stop as usize).cloned()))
    }
}
