use std::time::Duration;

use super::{entry_to_resp, invalid_id};
use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;
use crate::store::{Store, StreamId, WrongType};

/// XREAD [COUNT count] [BLOCK ms] STREAMS key [key ...] id [id ...]
pub struct Xread {
    count: Option<usize>,
    block: Option<u64>,
    keys: Vec<String>,
    ids: Vec<ReadFrom>,
}

enum ReadFrom {
    /// `$`: only entries added after the command started.
    Latest,
    After(StreamId),
}

impl Command for Xread {
    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let (mut count, mut block) = (None, None);
        loop {
            match args.next_string()?.to_ascii_uppercase().as_str() {
                "COUNT" => count = Some(args.next_parse()?),
                "BLOCK" => block = Some(args.next_parse()?),
                "STREAMS" => break,
                _ => return Err(CmdError::Syntax),
            }
        }

        let mut keys = args.rest_min(2)?;
        if keys.len() % 2 != 0 {
            return Err(CmdError::Other(
                "ERR Unbalanced 'xread' list of streams: for each stream key an ID or '$' must be specified.".into(),
            ));
        }
        let ids = keys
            .split_off(keys.len() / 2)
            .iter()
            .map(|id| match id.as_str() {
                "$" => Ok(ReadFrom::Latest),
                _ => id.parse().map(ReadFrom::After).map_err(|_| invalid_id()),
            })
            .collect::<Result<_, _>>()?;
        Ok(Xread { count, block, keys, ids })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        // Resolve `$` to the current last id before waiting.
        let ids: Vec<StreamId> = {
            let mut store = ctx.server.store();
            let mut ids = Vec::with_capacity(self.keys.len());
            for (key, from) in self.keys.iter().zip(&self.ids) {
                ids.push(match from {
                    ReadFrom::After(id) => *id,
                    ReadFrom::Latest => {
                        store.get_stream(key)?.and_then(|s| s.last_id()).unwrap_or(StreamId::MIN)
                    }
                });
            }
            ids
        };

        let timeout = match self.block {
            None => Some(Duration::ZERO), // check once
            Some(0) => None,              // forever
            Some(ms) => Some(Duration::from_millis(ms)),
        };
        let result = ctx.server.wait_for(timeout, |store| {
            read_streams(store, &self.keys, &ids, self.count).transpose()
        });
        Ok(result.transpose()?.unwrap_or(RespValue::NullArray))
    }
}

/// `[[key, [entry, ...]], ...]` for every stream that has entries after its id,
/// or `None` if none do.
fn read_streams(
    store: &mut Store,
    keys: &[String],
    ids: &[StreamId],
    count: Option<usize>,
) -> Result<Option<RespValue>, WrongType> {
    let mut result = Vec::new();
    for (key, &after) in keys.iter().zip(ids) {
        let Some(stream) = store.get_stream(key)? else { continue };
        let entries: Vec<RespValue> =
            stream.after(after).take(count.unwrap_or(usize::MAX)).map(entry_to_resp).collect();
        if !entries.is_empty() {
            result.push(RespValue::Array(vec![RespValue::bulk(key.clone()), RespValue::Array(entries)]));
        }
    }
    Ok((!result.is_empty()).then_some(RespValue::Array(result)))
}
