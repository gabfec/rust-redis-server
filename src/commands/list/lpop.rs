use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// LPOP key [count]
pub struct Lpop {
    key: String,
    count: Option<usize>,
}

impl Command for Lpop {
    const IS_WRITE: bool = true;

    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let key = args.next_string()?;
        let count = match args.next_opt() {
            Some(n) => Some(n.parse().map_err(|_| CmdError::NotInteger)?),
            None => None,
        };
        args.finish()?;
        Ok(Lpop { key, count })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let mut store = ctx.server.store();
        let Some(list) = store.get_list_mut(&self.key)? else {
            return Ok(RespValue::NullBulk);
        };
        Ok(match self.count {
            None if list.is_empty() => RespValue::NullBulk,
            None => RespValue::bulk(list.remove(0)),
            Some(n) => {
                let n = n.min(list.len());
                RespValue::bulk_array(list.drain(..n))
            }
        })
    }
}
