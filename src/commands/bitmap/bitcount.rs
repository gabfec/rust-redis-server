use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// BITCOUNT key [start end]
/// The number of set bits in a string, or in its bytes `start..=end`.
/// Negative indexes count from the end (-1 is the last byte).
pub struct Bitcount {
    key: String,
    range: Option<(i64, i64)>,
}

impl Command for Bitcount {
    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let key = args.next_string()?;
        let range = match args.next_opt() {
            None => None,
            Some(start) => {
                // A start without an end is a syntax error, like in Redis.
                let end = args.next_opt().ok_or(CmdError::Syntax)?;
                let start = start.parse().map_err(|_| CmdError::NotInteger)?;
                let end = end.parse().map_err(|_| CmdError::NotInteger)?;
                Some((start, end))
            }
        };
        if args.next_opt().is_some() {
            return Err(CmdError::Syntax);
        }
        Ok(Bitcount { key, range })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let mut store = ctx.store();
        let Some(bytes) = store.get_string(&self.key)? else {
            return Ok(RespValue::Integer(0));
        };
        let bytes = match self.range {
            None => &bytes[..],
            Some((start, end)) => byte_range(bytes, start, end),
        };
        let count: u32 = bytes.iter().map(|b| b.count_ones()).sum();
        Ok(RespValue::Integer(count as i64))
    }
}

/// Bytes `start..=end`, after resolving negative indexes and clamping to
/// the string.
fn byte_range(bytes: &[u8], start: i64, end: i64) -> &[u8] {
    let len = bytes.len() as i64;
    let resolve = |i: i64| if i < 0 { (len + i).max(0) } else { i };
    let (start, end) = (resolve(start), resolve(end).min(len - 1));
    if start > end {
        return &[];
    }
    &bytes[start as usize..=end as usize]
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::{err, Client};
    use crate::resp::RespValue;

    fn two_bytes() -> Client {
        let mut c = Client::default();
        c.cmd(&["SETBIT", "bitmap_key", "1", "1"]);
        c.cmd(&["SETBIT", "bitmap_key", "10", "1"]);
        c.cmd(&["SETBIT", "bitmap_key", "11", "1"]);
        c
    }

    #[test]
    fn counts_the_whole_string() {
        let mut c = two_bytes();
        assert_eq!(c.cmd(&["BITCOUNT", "bitmap_key"]), RespValue::Integer(3));
        assert_eq!(c.cmd(&["BITCOUNT", "missing"]), RespValue::Integer(0));
        c.cmd(&["SET", "s", "foobar"]);
        assert_eq!(c.cmd(&["BITCOUNT", "s"]), RespValue::Integer(26));
    }

    #[test]
    fn counts_a_byte_range() {
        let mut c = two_bytes();
        let count = |c: &mut Client, start: &str, end: &str| c.cmd(&["BITCOUNT", "bitmap_key", start, end]);
        assert_eq!(count(&mut c, "0", "1"), RespValue::Integer(3));
        assert_eq!(count(&mut c, "0", "0"), RespValue::Integer(1));
        assert_eq!(count(&mut c, "1", "1"), RespValue::Integer(2));
        assert_eq!(count(&mut c, "0", "100"), RespValue::Integer(3), "end past the end");
        assert_eq!(count(&mut c, "2", "5"), RespValue::Integer(0), "start past the end");
        assert_eq!(count(&mut c, "1", "0"), RespValue::Integer(0), "start after end");
        assert_eq!(count(&mut c, "-1", "-1"), RespValue::Integer(2), "the last byte");
        assert_eq!(count(&mut c, "-100", "0"), RespValue::Integer(1));
    }

    #[test]
    fn bad_arguments() {
        let mut c = two_bytes();
        assert_eq!(c.cmd(&["BITCOUNT", "bitmap_key", "0"]), err("ERR syntax error"));
        assert_eq!(c.cmd(&["BITCOUNT", "bitmap_key", "0", "x"]), err("ERR value is not an integer or out of range"));
        c.cmd(&["RPUSH", "l", "x"]);
        assert_eq!(
            c.cmd(&["BITCOUNT", "l"]),
            err("WRONGTYPE Operation against a key holding the wrong kind of value")
        );
    }
}
