use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;
use crate::store::Value;

/// BITOP AND|OR destkey key [key ...]
/// Combines strings byte by byte and stores the result at `destkey`.
/// Shorter strings (and missing keys) count as zero-padded to the longest
/// one. Returns the length of the result.
pub struct Bitop {
    op: Op,
    dest: String,
    keys: Vec<String>,
}

enum Op {
    And,
    Or,
}

impl Op {
    fn apply(&self, a: u8, b: u8) -> u8 {
        match self {
            Op::And => a & b,
            Op::Or => a | b,
        }
    }
}

impl Command for Bitop {
    const IS_WRITE: bool = true;

    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let op = match args.next_string()?.to_ascii_uppercase().as_str() {
            "AND" => Op::And,
            "OR" => Op::Or,
            _ => return Err(CmdError::Syntax),
        };
        let dest = args.next_string()?;
        let keys = args.rest_min(1)?;
        Ok(Bitop { op, dest, keys })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let mut store = ctx.store();
        let mut sources = Vec::with_capacity(self.keys.len());
        for key in &self.keys {
            sources.push(store.get_string(key)?.cloned().unwrap_or_default());
        }
        let len = sources.iter().map(Vec::len).max().unwrap_or(0);
        let byte = |source: &Vec<u8>, i: usize| source.get(i).copied().unwrap_or(0);
        let result: Vec<u8> = (0..len)
            .map(|i| sources[1..].iter().fold(byte(&sources[0], i), |acc, s| self.op.apply(acc, byte(s, i))))
            .collect();
        // Like Redis, an empty result deletes the destination.
        if result.is_empty() {
            store.remove(&self.dest);
        } else {
            store.set(self.dest, Value::String(result), None);
        }
        Ok(RespValue::Integer(len as i64))
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::{err, Client};
    use crate::resp::RespValue;

    fn bits(c: &mut Client, key: &str, offsets: &[u32]) {
        for offset in offsets {
            c.cmd(&["SETBIT", key, &offset.to_string(), "1"]);
        }
    }

    fn get_bits(c: &mut Client, key: &str, offsets: &[u32]) -> Vec<RespValue> {
        offsets.iter().map(|offset| c.cmd(&["GETBIT", key, &offset.to_string()])).collect()
    }

    #[test]
    fn and_of_equal_lengths() {
        let mut c = Client::default();
        bits(&mut c, "key1", &[0, 4]); // 10001000
        bits(&mut c, "key2", &[0, 6]); // 10000010
        assert_eq!(c.cmd(&["BITOP", "AND", "dest", "key1", "key2"]), RespValue::Integer(1));
        let [one, zero] = [RespValue::Integer(1), RespValue::Integer(0)];
        assert_eq!(get_bits(&mut c, "dest", &[0, 4, 6]), [one, zero.clone(), zero]);
    }

    #[test]
    fn and_pads_shorter_strings_with_zeros() {
        let mut c = Client::default();
        bits(&mut c, "key1", &[1, 10]); // 01000000 00100000
        bits(&mut c, "key2", &[1]); // 01000000
        assert_eq!(c.cmd(&["BITOP", "AND", "dest", "key1", "key2"]), RespValue::Integer(2));
        assert_eq!(get_bits(&mut c, "dest", &[1, 10]), [RespValue::Integer(1), RespValue::Integer(0)]);
        assert_eq!(c.cmd(&["STRLEN", "dest"]), RespValue::Integer(2));
        // A missing key is all zeros, as long as the longest source.
        assert_eq!(c.cmd(&["BITOP", "AND", "dest", "key1", "missing"]), RespValue::Integer(2));
        assert_eq!(c.cmd(&["BITCOUNT", "dest"]), RespValue::Integer(0));
    }

    #[test]
    fn or() {
        let mut c = Client::default();
        bits(&mut c, "key1", &[0, 4]); // 10001000
        bits(&mut c, "key2", &[0, 6]); // 10000010
        assert_eq!(c.cmd(&["BITOP", "OR", "dest", "key1", "key2"]), RespValue::Integer(1));
        let [one, zero] = [RespValue::Integer(1), RespValue::Integer(0)];
        assert_eq!(get_bits(&mut c, "dest", &[0, 4, 6, 7]), [one.clone(), one.clone(), one.clone(), zero]);

        bits(&mut c, "key3", &[1, 10]); // 01000000 00100000
        bits(&mut c, "key4", &[1]); // 01000000
        assert_eq!(c.cmd(&["bitop", "or", "dest2", "key3", "key4"]), RespValue::Integer(2));
        assert_eq!(get_bits(&mut c, "dest2", &[1, 10]), [one.clone(), one]);
    }

    #[test]
    fn empty_result_deletes_the_destination() {
        let mut c = Client::default();
        c.cmd(&["SET", "dest", "x"]);
        assert_eq!(c.cmd(&["BITOP", "AND", "dest", "missing1", "missing2"]), RespValue::Integer(0));
        assert_eq!(c.cmd(&["TYPE", "dest"]), RespValue::Simple("none".into()));
    }

    #[test]
    fn bad_arguments() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["BITOP", "NAND", "dest", "k"]), err("ERR syntax error"));
        assert_eq!(c.cmd(&["BITOP", "AND", "dest"]), err("ERR wrong number of arguments for 'bitop' command"));
        c.cmd(&["RPUSH", "l", "x"]);
        assert_eq!(
            c.cmd(&["BITOP", "AND", "dest", "l"]),
            err("WRONGTYPE Operation against a key holding the wrong kind of value")
        );
    }
}
