use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// REPLCONF <option> <value> [<option> <value> ...]
/// Sent by a replica during the handshake (listening-port, capa).
pub struct Replconf;

impl Command for Replconf {
    fn parse(args: Args) -> Result<Self, CmdError> {
        let wrong_args = args.wrong_args();
        let options = args.rest_min(2)?;
        if options.len() % 2 != 0 {
            return Err(wrong_args);
        }
        Ok(Replconf)
    }

    fn execute(self, _ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        Ok(RespValue::ok())
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::{err, Client};
    use crate::resp::RespValue;

    #[test]
    fn handshake_options_are_accepted() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["REPLCONF", "listening-port", "6380"]), RespValue::ok());
        assert_eq!(c.cmd(&["REPLCONF", "capa", "psync2"]), RespValue::ok());
        assert_eq!(c.cmd(&["REPLCONF", "capa"]), err("ERR wrong number of arguments for 'replconf' command"));
    }
}
