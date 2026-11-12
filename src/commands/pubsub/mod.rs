pub mod publish;
pub mod subscribe;
pub mod unsubscribe;

/// Commands a client can still run once it has subscribed to a channel.
pub fn allowed_in_subscribed_mode(name: &str) -> bool {
    matches!(
        name,
        "subscribe" | "unsubscribe" | "psubscribe" | "punsubscribe" | "ssubscribe" | "sunsubscribe"
            | "ping" | "quit" | "reset"
    )
}

#[cfg(test)]
mod tests {
    use super::subscribe::tests::subscribed;
    use crate::commands::test_util::{err, Client};

    #[test]
    fn subscribed_mode_allows_only_some_commands() {
        let mut c = Client::default();
        c.cmd(&["SET", "key", "value"]); // fine before subscribing
        c.cmd(&["SUBSCRIBE", "foo"]);
        for (command, name) in [(&["SET", "key", "value"][..], "set"), (&["GET", "key"], "get"), (&["ECHO", "hey"], "echo")] {
            assert_eq!(
                c.cmd(command),
                err(&format!(
                    "ERR Can't execute '{name}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
                ))
            );
        }
        assert_eq!(c.cmd(&["SUBSCRIBE", "bar"]), subscribed("bar", 2));
        // Other clients aren't affected.
        assert_eq!(c.other(&["GET", "key"]), crate::resp::RespValue::bulk("value"));
    }
}
