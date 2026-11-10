//! RDB snapshot files. Only the empty one so far, which the master sends
//! to a replica after FULLRESYNC.

/// An RDB file with no keys (Redis 7.2 format).
const EMPTY_RDB_HEX: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

pub fn empty() -> Vec<u8> {
    (0..EMPTY_RDB_HEX.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&EMPTY_RDB_HEX[i..i + 2], 16).unwrap())
        .collect()
}

#[cfg(test)]
mod tests {
    #[test]
    fn empty_rdb_has_the_redis_header() {
        let rdb = super::empty();
        assert!(rdb.starts_with(b"REDIS0011"));
        assert_eq!(rdb.len(), 88);
    }
}
