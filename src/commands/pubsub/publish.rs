use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// PUBLISH channel message
/// Delivers the message to the channel's subscribers and returns how many
/// there are.
pub struct Publish {
    channel: String,
    message: String,
}

impl Command for Publish {
    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let channel = args.next_string()?;
        let message = args.next_string()?;
        args.finish()?;
        Ok(Publish { channel, message })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let count = ctx.server.pubsub.publish(&self.channel, &self.message);
        Ok(RespValue::Integer(count as i64))
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::run;
    use crate::connection::ConnState;
    use crate::resp::RespValue;
    use crate::server::ServerState;

    #[test]
    fn returns_the_number_of_subscribers() {
        let server = ServerState::default();
        let mut clients: Vec<ConnState> = (0..3).map(|_| ConnState::default()).collect();
        run(&server, &mut clients[0], &["SUBSCRIBE", "foo"]);
        run(&server, &mut clients[1], &["SUBSCRIBE", "bar"]);
        run(&server, &mut clients[2], &["SUBSCRIBE", "bar"]);
        run(&server, &mut clients[2], &["SUBSCRIBE", "bar"]); // counted once

        let mut publisher = ConnState::default();
        assert_eq!(run(&server, &mut publisher, &["PUBLISH", "bar", "msg"]), RespValue::Integer(2));
        assert_eq!(run(&server, &mut publisher, &["PUBLISH", "foo", "msg"]), RespValue::Integer(1));
        assert_eq!(run(&server, &mut publisher, &["PUBLISH", "baz", "msg"]), RespValue::Integer(0));

        server.pubsub.unsubscribe_all(clients[2].id()); // e.g. it disconnected
        assert_eq!(run(&server, &mut publisher, &["PUBLISH", "bar", "msg"]), RespValue::Integer(1));
    }
}
