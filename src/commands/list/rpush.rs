use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// RPUSH key element [element ...]
pub struct Rpush {
    key: String,
    values: Vec<String>,
}

impl Command for Rpush {
    const IS_WRITE: bool = true;

    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let key = args.next_string()?;
        let values = args.rest_min(1)?;
        Ok(Rpush { key, values })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let mut store = ctx.server.store();
        let list = store.list_or_create(self.key)?;
        list.extend(self.values);
        Ok(RespValue::Integer(list.len() as i64))
    }
}
