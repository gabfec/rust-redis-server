use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// TYPE key
pub struct Type {
    key: String,
}

impl Command for Type {
    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let key = args.next_string()?;
        args.finish()?;
        Ok(Type { key })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let mut store = ctx.server.store();
        let name = store.get(&self.key).map_or("none", |v| v.type_name());
        Ok(RespValue::Simple(name.into()))
    }
}
