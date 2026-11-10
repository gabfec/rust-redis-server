use std::time::Duration;

use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// BLPOP key [key ...] timeout   (timeout in seconds, 0 = forever)
pub struct Blpop {
    keys: Vec<String>,
    timeout: Option<Duration>,
}

impl Command for Blpop {
    const IS_WRITE: bool = true;
    // TODO: propagate as LPOP; replaying BLPOP on a replica could block it.
    const PROPAGATE: bool = false;

    fn parse(args: Args) -> Result<Self, CmdError> {
        let mut keys = args.rest_min(2)?;
        let secs: f64 = keys.pop().unwrap().parse().map_err(|_| {
            CmdError::Other("ERR timeout is not a float or out of range".into())
        })?;
        if !secs.is_finite() || secs < 0.0 {
            return Err(CmdError::Other("ERR timeout is negative".into()));
        }
        let timeout = (secs > 0.0).then(|| Duration::from_secs_f64(secs));
        Ok(Blpop { keys, timeout })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let popped = ctx.wait_for(self.timeout, |store| {
            self.keys.iter().find_map(|key| {
                // Check first: taking the list mutably counts as a change (WATCH).
                if store.get_list(key).ok()??.is_empty() {
                    return None;
                }
                let list = store.get_list_mut(key).ok()??;
                let value = list.pop_front()?;
                Some(RespValue::bulk_array([key.clone(), value]))
            })
        });
        Ok(popped.unwrap_or(RespValue::NullArray))
    }
}

#[cfg(test)]
mod tests {
    use std::thread::{scope, sleep};
    use std::time::{Duration, Instant};

    use crate::commands::test_util::{err, run, Client};
    use crate::connection::ConnState;
    use crate::resp::RespValue;
    use crate::server::ServerState;

    #[test]
    fn returns_immediately_when_data_is_there() {
        let mut c = Client::default();
        c.cmd(&["RPUSH", "b", "x"]);
        assert_eq!(c.cmd(&["BLPOP", "a", "b", "0"]), RespValue::bulk_array(["b", "x"]));
    }

    #[test]
    fn times_out() {
        let mut c = Client::default();
        let start = Instant::now();
        assert_eq!(c.cmd(&["BLPOP", "l", "0.05"]), RespValue::NullArray);
        assert!(start.elapsed() >= Duration::from_millis(50));
    }

    #[test]
    fn woken_by_push_from_another_client() {
        let server = ServerState::default();
        scope(|s| {
            // 2 s timeout, so a bug fails the test instead of hanging it.
            let waiter = s.spawn(|| run(&server, &mut ConnState::default(), &["BLPOP", "l", "2"]));
            sleep(Duration::from_millis(50));
            run(&server, &mut ConnState::default(), &["RPUSH", "l", "x"]);
            assert_eq!(waiter.join().unwrap(), RespValue::bulk_array(["l", "x"]));
        });
    }

    #[test]
    fn bad_timeout() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["BLPOP", "l", "-1"]), err("ERR timeout is negative"));
        assert_eq!(c.cmd(&["BLPOP", "l", "x"]), err("ERR timeout is not a float or out of range"));
        assert_eq!(c.cmd(&["BLPOP", "l"]), err("ERR wrong number of arguments for 'blpop' command"));
    }
}
