use super::{mask, parse_offset};
use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// GETBIT key offset
/// The bit at `offset` of a string. Bits past the end, and of a missing
/// key, are 0.
pub struct Getbit {
    key: String,
    offset: u64,
}

impl Command for Getbit {
    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let key = args.next_string()?;
        let offset = parse_offset(&args.next_string()?)?;
        args.finish()?;
        Ok(Getbit { key, offset })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let mut store = ctx.store();
        let byte = store
            .get_string(&self.key)?
            .and_then(|bytes| bytes.get((self.offset / 8) as usize).copied())
            .unwrap_or(0);
        Ok(RespValue::Integer((byte & mask(self.offset) != 0) as i64))
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::{err, Client};
    use crate::resp::RespValue;

    #[test]
    fn reads_bits() {
        let mut c = Client::default();
        c.cmd(&["SETBIT", "bit_key", "2", "1"]);
        assert_eq!(c.cmd(&["GETBIT", "bit_key", "2"]), RespValue::Integer(1));
        assert_eq!(c.cmd(&["GETBIT", "bit_key", "3"]), RespValue::Integer(0));
        assert_eq!(c.cmd(&["GETBIT", "bit_key", "100"]), RespValue::Integer(0), "past the end");
        assert_eq!(c.cmd(&["GETBIT", "new_key", "2"]), RespValue::Integer(0));
    }

    #[test]
    fn reads_bits_of_a_set_string() {
        let mut c = Client::default();
        c.cmd(&["SET", "mykey", "A"]); // 01000001
        assert_eq!(c.cmd(&["GETBIT", "mykey", "1"]), RespValue::Integer(1));
        assert_eq!(c.cmd(&["GETBIT", "mykey", "7"]), RespValue::Integer(1));
        assert_eq!(c.cmd(&["GETBIT", "mykey", "0"]), RespValue::Integer(0));
    }

    #[test]
    fn bad_arguments() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["GETBIT", "k", "x"]), err("ERR bit offset is not an integer or out of range"));
        c.cmd(&["RPUSH", "l", "x"]);
        assert_eq!(
            c.cmd(&["GETBIT", "l", "0"]),
            err("WRONGTYPE Operation against a key holding the wrong kind of value")
        );
    }
}
