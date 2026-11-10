use std::time::Duration;

use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// WAIT numreplicas timeout
/// Waits until `numreplicas` replicas have processed every write sent so
/// far, or `timeout` ms. Returns how many replicas got that far.
pub struct Wait {
    numreplicas: usize,
    /// 0 waits forever.
    timeout_ms: u64,
}

impl Command for Wait {
    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let numreplicas = args.next_parse()?;
        let timeout_ms = args.next_parse()?;
        args.finish()?;
        Ok(Wait { numreplicas, timeout_ms })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let timeout = (self.timeout_ms > 0).then(|| Duration::from_millis(self.timeout_ms));
        let count = ctx.server.replication.wait_for_acks(self.numreplicas, timeout);
        Ok(RespValue::Integer(count as i64))
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::{err, Client};
    use crate::replication::test_util::{read_available, socket_pair};
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
        let mut replicas: Vec<_> = (0..7)
            .map(|_| {
                let (to_replica, replica) = socket_pair();
                c.server.replication.add_replica(to_replica, &[]).unwrap();
                replica
            })
            .collect();
        // Fewer, as many, or more than connected: always the connected
        // count, right away, without asking the replicas anything.
        let start = std::time::Instant::now();
        for wanted in ["3", "7", "9"] {
            assert_eq!(c.cmd(&["WAIT", wanted, "500"]), RespValue::Integer(7), "WAIT {wanted}");
        }
        assert!(start.elapsed() < std::time::Duration::from_millis(500));
        for replica in &mut replicas {
            assert_eq!(read_available(replica), "", "no GETACK was sent");
        }
    }
}
