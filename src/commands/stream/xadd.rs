use std::time::{SystemTime, UNIX_EPOCH};

use super::invalid_id;
use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;
use crate::store::{Stream, StreamEntry, StreamId};

/// XADD key <* | ms-* | ms-seq> field value [field value ...]
pub struct Xadd {
    key: String,
    id: IdSpec,
    fields: Vec<(String, String)>,
}

enum IdSpec {
    /// `*`
    Auto,
    /// `<ms>-*`
    AutoSeq(u64),
    /// `<ms>-<seq>`
    Explicit(StreamId),
}

impl Command for Xadd {
    const IS_WRITE: bool = true;

    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let key = args.next_string()?;
        let id = parse_id_spec(&args.next_string()?)?;
        let wrong_args = args.wrong_args();
        let rest = args.rest_min(2)?;
        if rest.len() % 2 != 0 {
            return Err(wrong_args);
        }
        let mut it = rest.into_iter();
        let fields = std::iter::from_fn(|| Some((it.next()?, it.next()?))).collect();
        Ok(Xadd { key, id, fields })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let mut store = ctx.server.store();
        let last = store.get_stream(&self.key)?.and_then(Stream::last_id);
        let id = resolve_id(self.id, last);

        if id == StreamId::MIN {
            return Err(CmdError::Other(
                "ERR The ID specified in XADD must be greater than 0-0".into(),
            ));
        }
        if last.is_some_and(|last| id <= last) {
            return Err(CmdError::Other(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                    .into(),
            ));
        }

        store.stream_or_create(self.key)?.push(StreamEntry { id, fields: self.fields });
        Ok(RespValue::bulk(id.to_string()))
    }
}

fn parse_id_spec(s: &str) -> Result<IdSpec, CmdError> {
    if s == "*" {
        return Ok(IdSpec::Auto);
    }
    if let Some(ms) = s.strip_suffix("-*") {
        return ms.parse().map(IdSpec::AutoSeq).map_err(|_| invalid_id());
    }
    s.parse().map(IdSpec::Explicit).map_err(|_| invalid_id())
}

fn resolve_id(spec: IdSpec, last: Option<StreamId>) -> StreamId {
    let next_seq = |ms: u64| match last {
        Some(last) if last.ms == ms => last.seq + 1,
        _ if ms == 0 => 1,
        _ => 0,
    };
    match spec {
        IdSpec::Explicit(id) => id,
        IdSpec::AutoSeq(ms) => StreamId::new(ms, next_seq(ms)),
        IdSpec::Auto => {
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
            // Never go backwards, even if the clock does.
            let ms = last.map_or(now, |last| last.ms.max(now));
            StreamId::new(ms, next_seq(ms))
        }
    }
}
