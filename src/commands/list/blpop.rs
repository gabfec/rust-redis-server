use std::time::Duration;

use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// BLPOP key [key ...] timeout   (timeout in seconds, 0 = forever)
pub struct Blpop {
    keys: Vec<String>,
    timeout: Option<Duration>,
}

impl Command for Blpop {
    const IS_WRITE: bool = true;

    fn parse(args: Args) -> Result<Self, CmdError> {
        let mut keys = args.rest_min(2)?;
        let secs: f64 = keys.pop().unwrap().parse().map_err(|_| {
            CmdError::Other("ERR timeout is not a float or out of range".into())
        })?;
        if !secs.is_finite() || secs < 0.0 {
            return Err(CmdError::Other("ERR timeout is negative".into()));
        }
        let timeout = (secs > 0.0).then(|| Duration::from_secs_f64(secs));
        Ok(Blpop { keys, timeout })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let popped = ctx.server.wait_for(self.timeout, |store| {
            self.keys.iter().find_map(|key| {
                let list = store.get_list_mut(key).ok()??;
                let value = list.pop_front()?;
                Some(RespValue::bulk_array([key.clone(), value]))
            })
        });
        Ok(popped.unwrap_or(RespValue::NullArray))
    }
}
