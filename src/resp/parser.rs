use std::fmt;

use super::RespValue;

#[derive(Debug, PartialEq)]
pub struct ParseError(String);

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Parses one RESP value from the start of `buf`.
/// Returns `Ok(None)` when `buf` doesn't hold a complete value yet,
/// otherwise the value and the number of bytes it used.
pub fn parse(buf: &[u8]) -> Result<Option<(RespValue, usize)>, ParseError> {
    parse_at(buf, 0)
}

fn parse_at(buf: &[u8], pos: usize) -> Result<Option<(RespValue, usize)>, ParseError> {
    let Some(&tag) = buf.get(pos) else { return Ok(None) };
    let Some(eol) = find_crlf(buf, pos) else { return Ok(None) };
    let next = eol + 2;
    let header = || {
        std::str::from_utf8(&buf[pos + 1..eol]).map_err(|_| ParseError("invalid header".into()))
    };

    let value = match tag {
        b'+' => RespValue::Simple(header()?.to_string()),
        b'-' => RespValue::Error(header()?.to_string()),
        b':' => RespValue::Integer(parse_int(header()?)?),
        b'$' => {
            let len = parse_int(header()?)?;
            if len < 0 {
                return Ok(Some((RespValue::NullBulk, next)));
            }
            let end = next + len as usize;
            if buf.len() < end + 2 {
                return Ok(None);
            }
            if &buf[end..end + 2] != b"\r\n" {
                return Err(ParseError("bulk string missing CRLF".into()));
            }
            let s = String::from_utf8_lossy(&buf[next..end]).into_owned();
            return Ok(Some((RespValue::Bulk(s), end + 2)));
        }
        b'*' => {
            let len = parse_int(header()?)?;
            if len < 0 {
                return Ok(Some((RespValue::NullArray, next)));
            }
            let mut items = Vec::with_capacity(len.min(1024) as usize);
            let mut cur = next;
            for _ in 0..len {
                let Some((item, after)) = parse_at(buf, cur)? else { return Ok(None) };
                items.push(item);
                cur = after;
            }
            return Ok(Some((RespValue::Array(items), cur)));
        }
        // Inline command (e.g. typed by hand via `nc`): "PING\r\n"
        _ => RespValue::Array(
            String::from_utf8_lossy(&buf[pos..eol])
                .split_whitespace()
                .map(RespValue::bulk)
                .collect(),
        ),
    };
    Ok(Some((value, next)))
}

fn find_crlf(buf: &[u8], from: usize) -> Option<usize> {
    buf[from..].windows(2).position(|w| w == b"\r\n").map(|i| from + i)
}

fn parse_int(s: &str) -> Result<i64, ParseError> {
    s.parse().map_err(|_| ParseError(format!("invalid integer '{s}'")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_command_array() {
        let input = b"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n";
        let (value, used) = parse(input).unwrap().unwrap();
        assert_eq!(value, RespValue::bulk_array(["ECHO", "hey"]));
        assert_eq!(used, input.len());
    }

    #[test]
    fn incomplete_input_needs_more_bytes() {
        assert_eq!(parse(b"*2\r\n$4\r\nECHO\r\n$3\r\nhe"), Ok(None));
        assert_eq!(parse(b"*2\r\n$4"), Ok(None));
        assert_eq!(parse(b""), Ok(None));
    }

    #[test]
    fn pipelined_commands_parse_one_at_a_time() {
        let input = b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n";
        let (_, used) = parse(input).unwrap().unwrap();
        assert_eq!(used, input.len() / 2);
    }

    #[test]
    fn bulk_may_contain_crlf() {
        let (value, _) = parse(b"$4\r\na\r\nb\r\n").unwrap().unwrap();
        assert_eq!(value, RespValue::bulk("a\r\nb"));
    }

    #[test]
    fn inline_command() {
        let (value, _) = parse(b"ECHO hi\r\n").unwrap().unwrap();
        assert_eq!(value, RespValue::bulk_array(["ECHO", "hi"]));
    }
}
