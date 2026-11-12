use super::{mask, parse_offset};
use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// SETBIT key offset value
/// Sets or clears one bit of a string, growing it with zero bytes if
/// needed. Returns the bit's previous value.
pub struct Setbit {
    key: String,
    offset: u64,
    bit: bool,
}

impl Command for Setbit {
    const IS_WRITE: bool = true;

    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let key = args.next_string()?;
        let offset = parse_offset(&args.next_string()?)?;
        let bit = match args.next_string()?.as_str() {
            "0" => false,
            "1" => true,
            _ => return Err(CmdError::Other("ERR bit is not an integer or out of range".into())),
        };
        args.finish()?;
        Ok(Setbit { key, offset, bit })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let mut store = ctx.store();
        let bytes = store.string_or_create(self.key)?;
        let index = (self.offset / 8) as usize;
        if bytes.len() <= index {
            bytes.resize(index + 1, 0);
        }
        let mask = mask(self.offset);
        let old = bytes[index] & mask != 0;
        if self.bit {
            bytes[index] |= mask;
        } else {
            bytes[index] &= !mask;
        }
        Ok(RespValue::Integer(old as i64))
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::{err, Client};
    use crate::resp::RespValue;

    #[test]
    fn returns_the_previous_bit() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["SETBIT", "bitmap_key", "3", "1"]), RespValue::Integer(0));
        assert_eq!(c.cmd(&["SETBIT", "bitmap_key", "3", "1"]), RespValue::Integer(1));
        assert_eq!(c.cmd(&["SETBIT", "bitmap_key", "3", "0"]), RespValue::Integer(1));
        assert_eq!(c.cmd(&["SETBIT", "bitmap_key", "3", "0"]), RespValue::Integer(0));
        assert_eq!(c.cmd(&["TYPE", "bitmap_key"]), RespValue::Simple("string".into()));
    }

    #[test]
    fn writes_bytes_that_get_returns() {
        let mut c = Client::default();
        c.cmd(&["SETBIT", "mykey", "1", "1"]); // 01000000
        assert_eq!(c.cmd(&["GET", "mykey"]), RespValue::bulk("@"));
        c.cmd(&["SET", "s", "A"]); // 01000001
        c.cmd(&["SETBIT", "s", "6", "1"]); // 01000011
        assert_eq!(c.cmd(&["GET", "s"]), RespValue::bulk("C"));
    }

    #[test]
    fn bad_arguments() {
        let mut c = Client::default();
        let bad_offset = err("ERR bit offset is not an integer or out of range");
        assert_eq!(c.cmd(&["SETBIT", "k", "-1", "1"]), bad_offset);
        assert_eq!(c.cmd(&["SETBIT", "k", "4294967296", "1"]), bad_offset);
        assert_eq!(c.cmd(&["SETBIT", "k", "0", "2"]), err("ERR bit is not an integer or out of range"));
        assert_eq!(c.cmd(&["TYPE", "k"]), RespValue::Simple("none".into()), "nothing was created");
        c.cmd(&["RPUSH", "l", "x"]);
        assert_eq!(
            c.cmd(&["SETBIT", "l", "0", "1"]),
            err("WRONGTYPE Operation against a key holding the wrong kind of value")
        );
    }
}
