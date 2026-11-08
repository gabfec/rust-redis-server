use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// PING [message]
pub struct Ping {
    message: Option<String>,
}

impl Command for Ping {
    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let message = args.next_opt();
        args.finish()?;
        Ok(Ping { message })
    }

    fn execute(self, _ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        Ok(match self.message {
            Some(msg) => RespValue::bulk(msg),
            None => RespValue::Simple("PONG".into()),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::{err, Client};
    use crate::resp::RespValue;

    #[test]
    fn ping() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["PING"]), RespValue::Simple("PONG".into()));
        assert_eq!(c.cmd(&["PING", "hi"]), RespValue::bulk("hi"));
        assert_eq!(c.cmd(&["PING", "a", "b"]), err("ERR wrong number of arguments for 'ping' command"));
    }
}
