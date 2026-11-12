use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// KEYS pattern   (glob-style: `*` any run of characters, `?` one character)
pub struct Keys {
    pattern: String,
}

impl Command for Keys {
    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let pattern = args.next_string()?;
        args.finish()?;
        Ok(Keys { pattern })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let pattern: Vec<char> = self.pattern.chars().collect();
        let keys = ctx.store().keys();
        Ok(RespValue::bulk_array(keys.into_iter().filter(|key| {
            glob_match(&pattern, &key.chars().collect::<Vec<_>>())
        })))
    }
}

fn glob_match(pattern: &[char], text: &[char]) -> bool {
    match pattern.split_first() {
        None => text.is_empty(),
        Some(('*', rest)) => (0..=text.len()).any(|i| glob_match(rest, &text[i..])),
        Some(('?', rest)) => !text.is_empty() && glob_match(rest, &text[1..]),
        Some((c, rest)) => text.first() == Some(c) && glob_match(rest, &text[1..]),
    }
}

#[cfg(test)]
mod tests {
    use super::glob_match;
    use crate::commands::test_util::Client;
    use crate::resp::RespValue;

    fn matches(pattern: &str, text: &str) -> bool {
        glob_match(&pattern.chars().collect::<Vec<_>>(), &text.chars().collect::<Vec<_>>())
    }

    #[test]
    fn glob_patterns() {
        assert!(matches("*", "anything"));
        assert!(matches("f*", "foo"));
        assert!(!matches("f*", "bar"));
        assert!(matches("h?llo", "hello"));
        assert!(matches("*o*", "foo"));
        assert!(!matches("foo", "fooo"));
    }

    #[test]
    fn keys() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["KEYS", "*"]), RespValue::Array(vec![]));
        c.cmd(&["SET", "foo", "bar"]);
        c.cmd(&["SET", "baz", "qux"]);
        c.cmd(&["RPUSH", "list", "x"]);
        let RespValue::Array(mut all) = c.cmd(&["KEYS", "*"]) else { panic!() };
        all.sort_by_key(|v| format!("{v:?}"));
        assert_eq!(RespValue::Array(all), RespValue::bulk_array(["baz", "foo", "list"]));
        assert_eq!(c.cmd(&["KEYS", "f*"]), RespValue::bulk_array(["foo"]));
    }
}
