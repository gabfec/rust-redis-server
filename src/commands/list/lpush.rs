use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// LPUSH key element [element ...]
pub struct Lpush {
    key: String,
    values: Vec<String>,
}

impl Command for Lpush {
    const IS_WRITE: bool = true;

    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let key = args.next_string()?;
        let values = args.rest_min(1)?;
        Ok(Lpush { key, values })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let mut store = ctx.server.store();
        let list = store.list_or_create(self.key)?;
        for value in self.values {
            list.insert(0, value);
        }
        Ok(RespValue::Integer(list.len() as i64))
    }
}
