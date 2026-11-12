use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// CONFIG GET parameter [parameter ...]
pub struct Config {
    parameters: Vec<String>,
}

impl Command for Config {
    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let subcommand = args.next_string()?;
        if !subcommand.eq_ignore_ascii_case("GET") {
            return Err(CmdError::Other(format!("ERR unknown subcommand '{subcommand}'")));
        }
        let parameters = args.rest_min(1)?;
        Ok(Config { parameters })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        // Name/value pairs; unknown parameters are left out.
        let mut pairs = Vec::new();
        for name in self.parameters {
            let name = name.to_ascii_lowercase();
            if let Some(value) = ctx.server.config.get(&name) {
                pairs.extend([name, value]);
            }
        }
        Ok(RespValue::bulk_array(pairs))
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::Client;
    use crate::config::Config;
    use crate::resp::RespValue;
    use crate::server::ServerState;

    #[test]
    fn config_get() {
        let config = Config { dir: "/tmp/redis-files".into(), dbfilename: "dump.rdb".into(), ..Config::default() };
        let mut c = Client { server: ServerState::new(&config), ..Client::default() };
        assert_eq!(c.cmd(&["CONFIG", "GET", "dir"]), RespValue::bulk_array(["dir", "/tmp/redis-files"]));
        assert_eq!(c.cmd(&["config", "get", "DBFILENAME"]), RespValue::bulk_array(["dbfilename", "dump.rdb"]));
        assert_eq!(c.cmd(&["CONFIG", "GET", "nope"]), RespValue::Array(vec![]));
        assert!(matches!(c.cmd(&["CONFIG", "SET", "dir", "x"]), RespValue::Error(_)));
    }
}
