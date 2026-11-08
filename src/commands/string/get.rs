use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// GET key
pub struct Get {
    key: String,
}

impl Command for Get {
    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let key = args.next_string()?;
        args.finish()?;
        Ok(Get { key })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let mut store = ctx.server.store();
        Ok(match store.get_string(&self.key)? {
            Some(s) => RespValue::bulk(s.clone()),
            None => RespValue::NullBulk,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::{err, Client};
    use crate::resp::RespValue;

    #[test]
    fn get() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["GET", "k"]), RespValue::NullBulk);
        c.cmd(&["SET", "k", "v"]);
        assert_eq!(c.cmd(&["GET", "k"]), RespValue::bulk("v"));
    }

    #[test]
    fn get_on_a_list_is_wrongtype() {
        let mut c = Client::default();
        c.cmd(&["RPUSH", "l", "v"]);
        assert_eq!(
            c.cmd(&["GET", "l"]),
            err("WRONGTYPE Operation against a key holding the wrong kind of value")
        );
    }
}
