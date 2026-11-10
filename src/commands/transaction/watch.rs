use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// WATCH key [key ...]
pub struct Watch {
    keys: Vec<String>,
}

impl Command for Watch {
    fn parse(args: Args) -> Result<Self, CmdError> {
        let keys = args.rest_min(1)?;
        Ok(Watch { keys })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        if ctx.conn.transaction.is_some() {
            return Err(CmdError::Other("ERR WATCH inside MULTI is not allowed".into()));
        }
        // EXEC aborts if any of these versions has changed by then.
        let watched: Vec<(String, u64)> = {
            let mut store = ctx.store();
            self.keys
                .into_iter()
                .map(|key| {
                    let version = store.version(&key);
                    (key, version)
                })
                .collect()
        };
        ctx.conn.watched.extend(watched);
        Ok(RespValue::ok())
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::{err, Client};
    use crate::resp::RespValue;

    #[test]
    fn watch() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["WATCH", "a"]), RespValue::ok());
        assert_eq!(c.cmd(&["WATCH", "a", "b"]), RespValue::ok());
        assert_eq!(c.cmd(&["WATCH"]), err("ERR wrong number of arguments for 'watch' command"));
    }

    #[test]
    fn watch_inside_multi_is_an_error() {
        let mut c = Client::default();
        c.cmd(&["MULTI"]);
        assert_eq!(c.cmd(&["WATCH", "a"]), err("ERR WATCH inside MULTI is not allowed"));
        // The transaction itself carries on.
        c.cmd(&["SET", "a", "1"]);
        assert_eq!(c.cmd(&["EXEC"]), RespValue::Array(vec![RespValue::ok()]));
    }

    fn watched_transaction(c: &mut Client, key: &str) {
        c.cmd(&["WATCH", key]);
        c.cmd(&["MULTI"]);
        c.cmd(&["SET", key, "from transaction"]);
    }

    #[test]
    fn exec_runs_if_the_watched_key_did_not_change() {
        let mut c = Client::default();
        c.cmd(&["SET", "k", "1"]);
        watched_transaction(&mut c, "k");
        c.other(&["GET", "k"]); // reading is not a change
        c.other(&["SET", "other", "x"]);
        assert_eq!(c.cmd(&["EXEC"]), RespValue::Array(vec![RespValue::ok()]));
        assert_eq!(c.cmd(&["GET", "k"]), RespValue::bulk("from transaction"));
    }

    #[test]
    fn exec_aborts_if_another_client_changed_the_key() {
        for change in [&["SET", "k", "2"][..], &["INCR", "k"], &["SET", "k", "1"]] {
            let mut c = Client::default();
            c.cmd(&["SET", "k", "1"]);
            watched_transaction(&mut c, "k");
            c.other(change);
            assert_eq!(c.cmd(&["EXEC"]), RespValue::NullArray, "after {change:?}");
            assert_ne!(c.cmd(&["GET", "k"]), RespValue::bulk("from transaction"));
        }
    }

    #[test]
    fn exec_aborts_on_list_and_stream_changes() {
        let mut c = Client::default();
        c.cmd(&["RPUSH", "l", "a"]);
        c.cmd(&["XADD", "s", "1-1", "f", "v"]);
        c.cmd(&["WATCH", "l", "s"]);
        c.other(&["LPOP", "l"]);
        c.cmd(&["MULTI"]);
        c.cmd(&["PING"]);
        assert_eq!(c.cmd(&["EXEC"]), RespValue::NullArray);
    }

    #[test]
    fn own_change_after_watch_also_aborts() {
        let mut c = Client::default();
        c.cmd(&["WATCH", "k"]);
        c.cmd(&["SET", "k", "mine"]);
        c.cmd(&["MULTI"]);
        c.cmd(&["SET", "k", "tx"]);
        assert_eq!(c.cmd(&["EXEC"]), RespValue::NullArray);
        assert_eq!(c.cmd(&["GET", "k"]), RespValue::bulk("mine"));
    }
}
