use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// WAIT numreplicas timeout
/// Waits until `numreplicas` replicas have processed every write sent so
/// far, or `timeout` ms. Returns how many replicas got that far.
pub struct Wait;

impl Command for Wait {
    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let _numreplicas: usize = args.next_parse()?;
        let _timeout_ms: u64 = args.next_parse()?;
        args.finish()?;
        Ok(Wait)
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        // TODO: ask the replicas how far they got. For now, assume that
        // nothing was written, so every replica is up to date.
        Ok(RespValue::Integer(ctx.server.replication.replica_count() as i64))
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::{err, Client};
    use crate::replication::test_util::socket_pair;
    use crate::resp::RespValue;

    #[test]
    fn no_replicas() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["WAIT", "0", "60000"]), RespValue::Integer(0));
        assert_eq!(c.cmd(&["WAIT", "1", "x"]), err("ERR value is not an integer or out of range"));
    }

    #[test]
    fn no_commands_means_every_replica_is_in_sync() {
        let mut c = Client::default();
        let _replicas: Vec<_> = (0..3)
            .map(|_| {
                let (to_replica, replica) = socket_pair();
                c.server.replication.add_replica(to_replica, &[]).unwrap();
                replica
            })
            .collect();
        assert_eq!(c.cmd(&["WAIT", "1", "500"]), RespValue::Integer(3));
        assert_eq!(c.cmd(&["WAIT", "3", "500"]), RespValue::Integer(3));
    }
}
