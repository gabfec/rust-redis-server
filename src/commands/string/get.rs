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
