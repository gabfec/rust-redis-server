use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// LLEN key
pub struct Llen {
    key: String,
}

impl Command for Llen {
    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let key = args.next_string()?;
        args.finish()?;
        Ok(Llen { key })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let mut store = ctx.server.store();
        let len = store.get_list(&self.key)?.map_or(0, |l| l.len());
        Ok(RespValue::Integer(len as i64))
    }
}
