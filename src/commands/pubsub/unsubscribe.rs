use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// UNSUBSCRIBE [channel ...]   (no channel: all of them)
pub struct Unsubscribe {
    channels: Vec<String>,
}

impl Command for Unsubscribe {
    fn parse(args: Args) -> Result<Self, CmdError> {
        Ok(Unsubscribe { channels: args.rest_min(0)? })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let channels = match self.channels.is_empty() {
            true => ctx.conn.subscriptions.clone(),
            false => self.channels,
        };
        if channels.is_empty() {
            // Not subscribed to anything, and no channel given.
            return Ok(unsubscribed(RespValue::NullBulk, 0));
        }
        let client = ctx.conn.id();
        // One reply per channel: ["unsubscribe", channel, channels left].
        let replies: Vec<RespValue> = channels
            .into_iter()
            .map(|channel| {
                ctx.server.pubsub.unsubscribe(client, &channel);
                ctx.conn.subscriptions.retain(|c| *c != channel);
                unsubscribed(RespValue::bulk(channel), ctx.conn.subscriptions.len())
            })
            .collect();
        Ok(match <[RespValue; 1]>::try_from(replies) {
            Ok([single]) => single,
            Err(replies) => RespValue::sequence(replies),
        })
    }
}

fn unsubscribed(channel: RespValue, left: usize) -> RespValue {
    RespValue::Array(vec![RespValue::bulk("unsubscribe"), channel, RespValue::Integer(left as i64)])
}

#[cfg(test)]
mod tests {
    use super::unsubscribed;
    use crate::commands::test_util::{run, Client};
    use crate::connection::ConnState;
    use crate::resp::RespValue;

    fn left(channel: &str, count: usize) -> RespValue {
        unsubscribed(RespValue::bulk(channel), count)
    }

    #[test]
    fn unsubscribe_from_channels() {
        let mut c = Client::default();
        c.cmd(&["SUBSCRIBE", "foo"]);
        c.cmd(&["SUBSCRIBE", "bar"]);
        assert_eq!(c.cmd(&["UNSUBSCRIBE", "foo"]), left("foo", 1));
        assert_eq!(c.cmd(&["UNSUBSCRIBE", "baz"]), left("baz", 1), "never subscribed: count unchanged");
        assert_eq!(c.cmd(&["UNSUBSCRIBE", "bar"]), left("bar", 0));
        // No subscriptions left: out of subscribed mode.
        assert_eq!(c.cmd(&["SET", "k", "v"]), RespValue::ok());
    }

    #[test]
    fn unsubscribe_from_everything() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["UNSUBSCRIBE"]), unsubscribed(RespValue::NullBulk, 0));
        c.cmd(&["SUBSCRIBE", "a", "b"]);
        assert_eq!(c.cmd(&["UNSUBSCRIBE"]), RespValue::sequence([left("a", 1), left("b", 0)]));
    }

    #[test]
    fn unsubscribed_clients_no_longer_count() {
        let mut c = Client::default();
        let mut other = ConnState::default();
        c.cmd(&["SUBSCRIBE", "foo"]);
        run(&c.server, &mut other, &["SUBSCRIBE", "foo"]);
        assert_eq!(c.other(&["PUBLISH", "foo", "x"]), RespValue::Integer(2));
        c.cmd(&["UNSUBSCRIBE", "foo"]);
        assert_eq!(c.other(&["PUBLISH", "foo", "x"]), RespValue::Integer(1));
    }
}
