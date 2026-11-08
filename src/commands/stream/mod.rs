pub mod xadd;
pub mod xrange;
pub mod xread;

use crate::commands::CmdError;
use crate::resp::RespValue;
use crate::store::StreamEntry;

/// `[id, [field, value, field, value, ...]]`
fn entry_to_resp(entry: &StreamEntry) -> RespValue {
    RespValue::Array(vec![
        RespValue::bulk(entry.id.to_string()),
        RespValue::bulk_array(entry.fields.iter().flat_map(|(f, v)| [f.clone(), v.clone()])),
    ])
}

fn invalid_id() -> CmdError {
    CmdError::Other("ERR Invalid stream ID specified as stream command argument".into())
}

/// `[id, [field, value, ...]]`, as the stream commands reply.
#[cfg(test)]
fn entry(id: &str, fields: &[&str]) -> RespValue {
    RespValue::Array(vec![RespValue::bulk(id), RespValue::bulk_array(fields.iter().copied())])
}
