use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// ECHO message
pub struct Echo {
    message: String,
}

impl Command for Echo {
    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let message = args.next_string()?;
        args.finish()?;
        Ok(Echo { message })
    }

    fn execute(self, _ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        Ok(RespValue::bulk(self.message))
    }
}
