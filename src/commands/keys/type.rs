use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// TYPE key
pub struct Type {
    key: String,
}

impl Command for Type {
    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let key = args.next_string()?;
        args.finish()?;
        Ok(Type { key })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let mut store = ctx.server.store();
        let name = store.get(&self.key).map_or("none", |v| v.type_name());
        Ok(RespValue::Simple(name.into()))
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::Client;
    use crate::resp::RespValue;

    #[test]
    fn reports_value_type() {
        let mut c = Client::default();
        c.cmd(&["SET", "s", "v"]);
        c.cmd(&["RPUSH", "l", "v"]);
        c.cmd(&["XADD", "x", "1-1", "f", "v"]);
        for (key, expected) in [("s", "string"), ("l", "list"), ("x", "stream"), ("missing", "none")] {
            assert_eq!(c.cmd(&["TYPE", key]), RespValue::Simple(expected.into()), "key {key}");
        }
    }
}
