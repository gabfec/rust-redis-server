use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// PSYNC <replid> <offset>
/// Last step of a replica's handshake. We always answer with a full
/// resync: FULLRESYNC <our replid> <our offset>.
pub struct Psync;

impl Command for Psync {
    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let _replid = args.next_string()?;
        let _offset: i64 = args.next_parse()?;
        args.finish()?;
        Ok(Psync)
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let replid = &ctx.server.replication.replid;
        Ok(RespValue::Simple(format!("FULLRESYNC {replid} 0")))
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::Client;
    use crate::resp::RespValue;

    #[test]
    fn replies_fullresync_with_our_replid() {
        let mut c = Client::default();
        let replid = c.server.replication.replid.clone();
        assert_eq!(
            c.cmd(&["PSYNC", "?", "-1"]),
            RespValue::Simple(format!("FULLRESYNC {replid} 0"))
        );
    }
}
