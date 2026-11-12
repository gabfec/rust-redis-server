use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// STRLEN key
/// The length of a string in bytes; 0 for a missing key.
pub struct Strlen {
    key: String,
}

impl Command for Strlen {
    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let key = args.next_string()?;
        args.finish()?;
        Ok(Strlen { key })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let mut store = ctx.store();
        let len = store.get_string(&self.key)?.map_or(0, Vec::len);
        Ok(RespValue::Integer(len as i64))
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::{err, Client};
    use crate::resp::RespValue;

    #[test]
    fn lengths() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["STRLEN", "k"]), RespValue::Integer(0));
        c.cmd(&["SET", "k", "hello"]);
        assert_eq!(c.cmd(&["STRLEN", "k"]), RespValue::Integer(5));
        c.cmd(&["RPUSH", "l", "x"]);
        assert_eq!(
            c.cmd(&["STRLEN", "l"]),
            err("WRONGTYPE Operation against a key holding the wrong kind of value")
        );
    }

    #[test]
    fn setbit_grows_the_string() {
        let mut c = Client::default();
        c.cmd(&["SETBIT", "bitmap_key", "1", "1"]);
        assert_eq!(c.cmd(&["STRLEN", "bitmap_key"]), RespValue::Integer(1));
        assert_eq!(c.cmd(&["SETBIT", "bitmap_key", "10", "1"]), RespValue::Integer(0));
        assert_eq!(c.cmd(&["GETBIT", "bitmap_key", "10"]), RespValue::Integer(1));
        assert_eq!(c.cmd(&["GETBIT", "bitmap_key", "1"]), RespValue::Integer(1), "first byte kept");
        assert_eq!(c.cmd(&["STRLEN", "bitmap_key"]), RespValue::Integer(2));
        c.cmd(&["SETBIT", "bitmap_key", "100", "0"]);
        assert_eq!(c.cmd(&["STRLEN", "bitmap_key"]), RespValue::Integer(13), "clearing a bit grows it too");
    }
}
