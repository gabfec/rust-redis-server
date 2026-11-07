use super::{entry_to_resp, invalid_id};
use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;
use crate::store::StreamId;

/// XRANGE key start end   (inclusive; `-` / `+` = min / max)
pub struct Xrange {
    key: String,
    start: StreamId,
    end: StreamId,
}

impl Command for Xrange {
    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let key = args.next_string()?;
        let start = parse_bound(&args.next_string()?, 0)?;
        let end = parse_bound(&args.next_string()?, u64::MAX)?;
        args.finish()?;
        Ok(Xrange { key, start, end })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let mut store = ctx.server.store();
        let entries = match store.get_stream(&self.key)? {
            Some(stream) => stream.range(self.start, self.end).map(entry_to_resp).collect(),
            None => vec![],
        };
        Ok(RespValue::Array(entries))
    }
}

/// `-`, `+`, `<ms>` (seq defaults to `missing_seq`) or `<ms>-<seq>`.
fn parse_bound(s: &str, missing_seq: u64) -> Result<StreamId, CmdError> {
    match s {
        "-" => Ok(StreamId::MIN),
        "+" => Ok(StreamId::MAX),
        _ if !s.contains('-') => {
            s.parse().map(|ms| StreamId::new(ms, missing_seq)).map_err(|_| invalid_id())
        }
        _ => s.parse().map_err(|_| invalid_id()),
    }
}
