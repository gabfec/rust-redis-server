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

#[cfg(test)]
mod tests {
    use crate::commands::test_util::{err, Client};
    use crate::resp::RespValue;

    #[test]
    fn echo() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["ECHO", "hey there"]), RespValue::bulk("hey there"));
        assert_eq!(c.cmd(&["ECHO"]), err("ERR wrong number of arguments for 'echo' command"));
    }
}
