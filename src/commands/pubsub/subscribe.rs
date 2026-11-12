use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// SUBSCRIBE channel [channel ...]
pub struct Subscribe {
    channels: Vec<String>,
}

impl Command for Subscribe {
    fn parse(args: Args) -> Result<Self, CmdError> {
        Ok(Subscribe { channels: args.rest_min(1)? })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let client = ctx.conn.id();
        for channel in &self.channels {
            ctx.server.pubsub.subscribe(client, ctx.conn.writer.clone(), channel);
        }
        let subscriptions = &mut ctx.conn.subscriptions;
        // One reply per channel: ["subscribe", channel, how many channels
        // this client is subscribed to]. A channel is only counted once.
        let replies: Vec<RespValue> = self
            .channels
            .into_iter()
            .map(|channel| {
                if !subscriptions.contains(&channel) {
                    subscriptions.push(channel.clone());
                }
                RespValue::Array(vec![
                    RespValue::bulk("subscribe"),
                    RespValue::bulk(channel),
                    RespValue::Integer(subscriptions.len() as i64),
                ])
            })
            .collect();
        Ok(match <[RespValue; 1]>::try_from(replies) {
            Ok([single]) => single,
            Err(replies) => RespValue::sequence(replies),
        })
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

    #[test]
    fn counts_are_per_client_and_unique() {
        let mut a = Client::default();
        assert_eq!(a.cmd(&["SUBSCRIBE", "foo"]), subscribed("foo", 1));
        assert_eq!(a.cmd(&["SUBSCRIBE", "bar"]), subscribed("bar", 2));
        assert_eq!(a.cmd(&["SUBSCRIBE", "bar"]), subscribed("bar", 2));

        let mut b = Client::default();
        assert_eq!(b.cmd(&["SUBSCRIBE", "bar"]), subscribed("bar", 1));
    }

    #[test]
    fn several_channels_in_one_command() {
        let mut c = Client::default();
        assert_eq!(
            c.cmd(&["SUBSCRIBE", "a", "b", "a"]),
            RespValue::sequence([subscribed("a", 1), subscribed("b", 2), subscribed("a", 2)])
        );
    }
}
