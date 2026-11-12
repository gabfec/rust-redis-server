use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// ACL WHOAMI | GETUSER username
pub enum Acl {
    /// The user this connection is authenticated as.
    Whoami,
    /// A user's properties, as [name, value, ...]; nil if no such user.
    Getuser(String),
}

impl Command for Acl {
    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let subcommand = args.next_string()?.to_ascii_uppercase();
        let acl = match subcommand.as_str() {
            "WHOAMI" => Acl::Whoami,
            "GETUSER" => Acl::Getuser(args.next_string()?),
            _ => {
                return Err(CmdError::Other(format!(
                    "ERR unknown subcommand '{}'. Try ACL HELP.",
                    subcommand.to_ascii_lowercase()
                )));
            }
        };
        args.finish()?;
        Ok(acl)
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        match self {
            Acl::Whoami => Ok(RespValue::bulk("default")),
            Acl::Getuser(name) => Ok(match ctx.server.users.get(&name) {
                Some(user) => RespValue::Array(vec![RespValue::bulk("flags"), RespValue::bulk_array(user.flags())]),
                None => RespValue::NullBulk,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::{err, Client};
    use crate::resp::RespValue;

    #[test]
    fn whoami_is_the_default_user() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["ACL", "WHOAMI"]), RespValue::bulk("default"));
        assert_eq!(c.cmd(&["acl", "whoami"]), RespValue::bulk("default"));
    }

    #[test]
    fn getuser() {
        let mut c = Client::default();
        assert_eq!(
            c.cmd(&["ACL", "GETUSER", "default"]),
            RespValue::Array(vec![RespValue::bulk("flags"), RespValue::Array(vec![])])
        );
        assert_eq!(c.cmd(&["ACL", "GETUSER", "nobody"]), RespValue::NullBulk);
    }

    #[test]
    fn bad_arguments() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["ACL", "NOPE"]), err("ERR unknown subcommand 'nope'. Try ACL HELP."));
        assert_eq!(c.cmd(&["ACL"]), err("ERR wrong number of arguments for 'acl' command"));
        assert_eq!(c.cmd(&["ACL", "WHOAMI", "x"]), err("ERR wrong number of arguments for 'acl' command"));
    }
}
