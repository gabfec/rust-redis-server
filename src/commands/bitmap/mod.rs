pub mod bitcount;
pub mod getbit;
pub mod setbit;

use crate::commands::CmdError;

/// The largest bit offset: strings are at most 512 MB, like in Redis.
const MAX_OFFSET: u64 = 512 * 1024 * 1024 * 8 - 1;

/// A bit offset argument.
fn parse_offset(s: &str) -> Result<u64, CmdError> {
    match s.parse::<u64>() {
        Ok(offset) if offset <= MAX_OFFSET => Ok(offset),
        _ => Err(CmdError::Other("ERR bit offset is not an integer or out of range".into())),
    }
}

/// Mask for the bit at `offset` within its byte: offset 0 is the most
/// significant bit of the first byte.
fn mask(offset: u64) -> u8 {
    0x80 >> (offset % 8)
}
