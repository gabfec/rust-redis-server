use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// AUTH [username] password
/// Checks a username and password; the username defaults to "default".
pub struct Auth {
    username: String,
    password: String,
}

impl Command for Auth {
    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let first = args.next_string()?;
        let auth = match args.next_opt() {
            Some(password) => Auth { username: first, password },
            None => Auth { username: "default".into(), password: first },
        };
        if args.next_opt().is_some() {
            return Err(CmdError::Syntax);
        }
        Ok(auth)
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        if !ctx.server.users.check(&self.username, &self.password) {
            return Err(CmdError::Other(
                "WRONGPASS invalid username-password pair or user is disabled.".into(),
            ));
        }
        Ok(RespValue::ok())
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::{err, Client};
    use crate::resp::RespValue;

    const WRONGPASS: &str = "WRONGPASS invalid username-password pair or user is disabled.";

    #[test]
    fn checks_the_password() {
        let mut c = Client::default();
        c.cmd(&["ACL", "SETUSER", "default", ">mypassword"]);
        assert_eq!(c.cmd(&["AUTH", "default", "wrongpassword"]), err(WRONGPASS));
        assert_eq!(c.cmd(&["AUTH", "default", "mypassword"]), RespValue::ok());
        assert_eq!(c.cmd(&["AUTH", "mypassword"]), RespValue::ok(), "the default user");
        assert_eq!(c.cmd(&["AUTH", "nobody", "mypassword"]), err(WRONGPASS));
    }

    #[test]
    fn nopass_accepts_any_password() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["AUTH", "default", "anything"]), RespValue::ok());
    }
}
