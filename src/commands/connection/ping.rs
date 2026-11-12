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

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        // In subscribed mode the reply is ["pong", message or ""].
        if !ctx.conn.subscriptions.is_empty() {
            return Ok(RespValue::bulk_array(["pong".to_string(), self.message.unwrap_or_default()]));
        }
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

    #[test]
    fn ping_in_subscribed_mode() {
        let mut c = Client::default();
        c.cmd(&["SUBSCRIBE", "foo"]);
        assert_eq!(c.cmd(&["PING"]), RespValue::bulk_array(["pong", ""]));
        assert_eq!(c.cmd(&["PING", "hi"]), RespValue::bulk_array(["pong", "hi"]));
        assert_eq!(c.other(&["PING"]), RespValue::Simple("PONG".into()), "unsubscribed client");
    }
}
