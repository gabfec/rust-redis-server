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

    fn execute(self, _ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        // TODO: count the replicas. There are none so far.
        Ok(RespValue::Integer(0))
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::{err, Client};
    use crate::resp::RespValue;

    #[test]
    fn no_replicas() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["WAIT", "0", "60000"]), RespValue::Integer(0));
        assert_eq!(c.cmd(&["WAIT", "1", "x"]), err("ERR value is not an integer or out of range"));
    }
}
