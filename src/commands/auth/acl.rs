use crate::acl::Rule;
use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// ACL WHOAMI | GETUSER username | SETUSER username [rule ...]
pub enum Acl {
    /// The user this connection is authenticated as.
    Whoami,
    /// A user's properties, as [name, value, ...]; nil if no such user.
    Getuser(String),
    /// Creates or changes a user. Rules: `>password`, `<password`,
    /// `nopass`, `resetpass`.
    Setuser(String, Vec<Rule>),
}

impl Command for Acl {
    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let subcommand = args.next_string()?.to_ascii_uppercase();
        let acl = match subcommand.as_str() {
            "WHOAMI" => Acl::Whoami,
            "GETUSER" => Acl::Getuser(args.next_string()?),
            "SETUSER" => {
                let name = args.next_string()?;
                let mut rules = Vec::new();
                while let Some(rule) = args.next_opt() {
                    rules.push(Rule::parse(&rule).ok_or_else(|| {
                        CmdError::Other(format!("ERR Error in ACL SETUSER modifier '{rule}': Syntax error"))
                    })?);
                }
                Acl::Setuser(name, rules)
            }
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
            // Only authenticated connections get this far.
            Acl::Whoami => Ok(RespValue::bulk(ctx.conn.login.user().unwrap_or("default"))),
            Acl::Getuser(name) => Ok(match ctx.server.users.get(&name) {
                Some(user) => RespValue::Array(vec![
                    RespValue::bulk("flags"),
                    RespValue::bulk_array(user.flags()),
                    RespValue::bulk("passwords"),
                    RespValue::bulk_array(user.passwords),
                ]),
                None => RespValue::NullBulk,
            }),
            Acl::Setuser(name, rules) => {
                ctx.server.users.set(&name, rules);
                Ok(RespValue::ok())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::{err, run, Client};
    use crate::connection::ConnState;
    use crate::resp::RespValue;

    #[test]
    fn whoami_is_the_default_user() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["ACL", "WHOAMI"]), RespValue::bulk("default"));
        assert_eq!(c.cmd(&["acl", "whoami"]), RespValue::bulk("default"));
    }

    fn getuser(c: &mut Client, name: &str) -> RespValue {
        c.cmd(&["ACL", "GETUSER", name])
    }

    fn user(flags: &[&str], passwords: &[&str]) -> RespValue {
        RespValue::Array(vec![
            RespValue::bulk("flags"),
            RespValue::bulk_array(flags.iter().copied()),
            RespValue::bulk("passwords"),
            RespValue::bulk_array(passwords.iter().copied()),
        ])
    }

    #[test]
    fn getuser_of_the_default_user() {
        let mut c = Client::default();
        assert_eq!(getuser(&mut c, "default"), user(&["nopass"], &[]));
        assert_eq!(getuser(&mut c, "nobody"), RespValue::NullBulk);
    }

    #[test]
    fn a_password_on_the_default_user_requires_auth_for_new_connections() {
        let mut c = Client::default();
        let mut before = ConnState::new(&c.server);
        assert_eq!(c.cmd(&["ACL", "SETUSER", "default", ">newpassword"]), RespValue::ok());
        assert_eq!(c.cmd(&["ACL", "WHOAMI"]), RespValue::bulk("default"), "still authenticated");
        assert_eq!(run(&c.server, &mut before, &["ACL", "WHOAMI"]), RespValue::bulk("default"));

        let mut after = ConnState::new(&c.server);
        let noauth = err("NOAUTH Authentication required.");
        assert_eq!(run(&c.server, &mut after, &["ACL", "WHOAMI"]), noauth);
        assert_eq!(run(&c.server, &mut after, &["PING"]), noauth);
        assert_eq!(run(&c.server, &mut after, &["NOSUCHCOMMAND"]), noauth);

        c.cmd(&["ACL", "SETUSER", "default", "nopass"]);
        let mut later = ConnState::new(&c.server);
        assert_eq!(run(&c.server, &mut later, &["PING"]), RespValue::Simple("PONG".into()));
    }

    const MYPASSWORD: &str = "89e01536ac207279409d4de1e5253e01f4a1769e696db0d6062ca9b8f56767c8";

    #[test]
    fn setuser_adds_a_password() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["ACL", "SETUSER", "default", ">mypassword"]), RespValue::ok());
        assert_eq!(getuser(&mut c, "default"), user(&[], &[MYPASSWORD]));
        c.cmd(&["ACL", "SETUSER", "default", ">mypassword"]);
        assert_eq!(getuser(&mut c, "default"), user(&[], &[MYPASSWORD]), "no duplicates");
    }

    #[test]
    fn setuser_rules() {
        let mut c = Client::default();
        c.cmd(&["ACL", "SETUSER", "default", ">a", ">b", "<a"]);
        assert_eq!(getuser(&mut c, "default"), user(&[], &[&crate::sha256::hex_digest(b"b")]));
        c.cmd(&["ACL", "SETUSER", "default", "nopass"]);
        assert_eq!(getuser(&mut c, "default"), user(&["nopass"], &[]));
        c.cmd(&["ACL", "SETUSER", "default", "resetpass"]);
        assert_eq!(getuser(&mut c, "default"), user(&[], &[]));
        assert_eq!(c.cmd(&["ACL", "SETUSER", "alice", ">mypassword"]), RespValue::ok());
        assert_eq!(getuser(&mut c, "alice"), user(&[], &[MYPASSWORD]), "a new user");
        assert_eq!(
            c.cmd(&["ACL", "SETUSER", "alice", "~*"]),
            err("ERR Error in ACL SETUSER modifier '~*': Syntax error")
        );
    }

    #[test]
    fn bad_arguments() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["ACL", "NOPE"]), err("ERR unknown subcommand 'nope'. Try ACL HELP."));
        assert_eq!(c.cmd(&["ACL"]), err("ERR wrong number of arguments for 'acl' command"));
        assert_eq!(c.cmd(&["ACL", "WHOAMI", "x"]), err("ERR wrong number of arguments for 'acl' command"));
    }
}
