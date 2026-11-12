use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// SUBSCRIBE channel
pub struct Subscribe {
    channel: String,
}

impl Command for Subscribe {
    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let channel = args.next_string()?;
        args.finish()?;
        Ok(Subscribe { channel })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let subscriptions = &mut ctx.conn.subscriptions;
        if !subscriptions.contains(&self.channel) {
            subscriptions.push(self.channel.clone());
        }
        // ["subscribe", channel, how many channels this client is subscribed to]
        Ok(RespValue::Array(vec![
            RespValue::bulk("subscribe"),
            RespValue::bulk(self.channel),
            RespValue::Integer(subscriptions.len() as i64),
        ]))
    }
}

#[cfg(test)]
pub mod tests {
    use crate::resp::RespValue;
    use crate::commands::test_util::Client;

    /// ["subscribe", channel, count]
    pub fn subscribed(channel: &str, count: i64) -> RespValue {
        RespValue::Array(vec![RespValue::bulk("subscribe"), RespValue::bulk(channel), RespValue::Integer(count)])
    }

    #[test]
    fn subscribe_to_a_channel() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["SUBSCRIBE", "foo"]), subscribed("foo", 1));
    }
}
