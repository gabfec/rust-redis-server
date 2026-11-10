use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// REPLCONF <option> <value> [<option> <value> ...]
/// - From a replica during the handshake (listening-port, capa): OK.
/// - From the master, `GETACK *`: the replica answers
///   `REPLCONF ACK <bytes of commands processed>`.
pub struct Replconf {
    options: Vec<(String, String)>,
}

impl Command for Replconf {
    fn parse(args: Args) -> Result<Self, CmdError> {
        let wrong_args = args.wrong_args();
        let words = args.rest_min(2)?;
        if words.len() % 2 != 0 {
            return Err(wrong_args);
        }
        let options = words.chunks(2).map(|kv| (kv[0].to_ascii_lowercase(), kv[1].clone())).collect();
        Ok(Replconf { options })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        if self.options.iter().any(|(option, _)| option == "getack") {
            let offset = ctx.conn.master_offset.to_string();
            return Ok(RespValue::bulk_array(["REPLCONF", "ACK", &offset]));
        }
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

    #[test]
    fn getack_answers_with_the_offset() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["REPLCONF", "GETACK", "*"]), RespValue::bulk_array(["REPLCONF", "ACK", "0"]));
    }
}
