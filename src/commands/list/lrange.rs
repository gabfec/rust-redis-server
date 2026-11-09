use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// LRANGE key start stop   (inclusive, negative indexes count from the end)
pub struct Lrange {
    key: String,
    start: i64,
    stop: i64,
}

impl Command for Lrange {
    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let key = args.next_string()?;
        let start = args.next_parse()?;
        let stop = args.next_parse()?;
        args.finish()?;
        Ok(Lrange { key, start, stop })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let mut store = ctx.store();
        let Some(list) = store.get_list(&self.key)? else {
            return Ok(RespValue::Array(vec![]));
        };

        let len = list.len() as i64;
        let normalize = |i: i64| if i < 0 { (len + i).max(0) } else { i };
        let start = normalize(self.start);
        let stop = normalize(self.stop).min(len - 1);
        if start > stop {
            return Ok(RespValue::Array(vec![]));
        }
        Ok(RespValue::bulk_array(list.range(start as usize..=stop as usize).cloned()))
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::Client;
    use crate::resp::RespValue;

    fn client_with_list() -> Client {
        let mut c = Client::default();
        c.cmd(&["RPUSH", "l", "a", "b", "c", "d", "e"]);
        c
    }

    #[test]
    fn index_ranges() {
        let mut c = client_with_list();
        let cases: &[(&str, &str, &[&str])] = &[
            ("0", "1", &["a", "b"]),
            ("2", "4", &["c", "d", "e"]),
            ("1", "100", &["b", "c", "d", "e"]),
            ("-2", "-1", &["d", "e"]),
            ("0", "-3", &["a", "b", "c"]),
            ("-100", "0", &["a"]),
            ("3", "1", &[]),
            ("5", "10", &[]),
        ];
        for &(start, stop, expected) in cases {
            assert_eq!(
                c.cmd(&["LRANGE", "l", start, stop]),
                RespValue::bulk_array(expected.iter().copied()),
                "LRANGE {start} {stop}"
            );
        }
    }

    #[test]
    fn missing_or_emptied_list() {
        let mut c = client_with_list();
        assert_eq!(c.cmd(&["LRANGE", "missing", "0", "-1"]), RespValue::Array(vec![]));
        c.cmd(&["LPOP", "l", "5"]);
        assert_eq!(c.cmd(&["LRANGE", "l", "0", "-1"]), RespValue::Array(vec![]));
    }
}
