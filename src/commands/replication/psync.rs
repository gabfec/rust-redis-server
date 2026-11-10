use std::io::Write;

use crate::commands::{Args, CmdError, Command, Ctx};
use crate::replication::rdb;
use crate::resp::RespValue;

/// PSYNC <replid> <offset>
/// Last step of a replica's handshake. We always answer with a full
/// resync: FULLRESYNC <our replid> <our offset>, followed by a snapshot
/// of our data as an RDB file (`$<len>\r\n<bytes>`, no trailing CRLF).
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
        let mut out = Vec::new();
        RespValue::Simple(format!("FULLRESYNC {replid} 0")).encode(&mut out);
        // TODO: send the real data once there is RDB persistence.
        let rdb = rdb::empty();
        write!(out, "${}\r\n", rdb.len()).unwrap();
        out.extend_from_slice(&rdb);
        // From now on this connection gets every write (see connection.rs).
        ctx.conn.is_replica = true;
        Ok(RespValue::Raw(out))
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::Client;
    use crate::replication::rdb;
    use crate::resp::RespValue;

    #[test]
    fn replies_fullresync_then_the_rdb_file() {
        let mut c = Client::default();
        let replid = c.server.replication.replid.clone();
        let RespValue::Raw(reply) = c.cmd(&["PSYNC", "?", "-1"]) else { panic!() };

        let rdb = rdb::empty();
        let mut expected = format!("+FULLRESYNC {replid} 0\r\n${}\r\n", rdb.len()).into_bytes();
        expected.extend_from_slice(&rdb);
        assert_eq!(reply, expected);
    }
}
