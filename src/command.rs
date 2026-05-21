use std::collections::HashMap;

#[derive(Debug)]
pub enum Command {
    Ping,
    Echo(String),
    Set {
        key: String,
        value: String,
        px: Option<u64>, // Expiry in milliseconds
    },
    Get(String), // Key
    Rpush {
        key: String,
        values: Vec<String>,
    },
    Lpush {
        key: String,
        values: Vec<String>,
    },
    Lrange {
        key: String,
        start: i64,
        stop: i64,
    },
    Llen(String),
    Lpop {
        key: String,
        count: Option<usize>,
    },
    Blpop {
        keys: Vec<String>,
        timeout: f64,
    },
    Xadd {
        key: String,
        id: String,
        fields: HashMap<String, String>,
    },
    Xrange {
        key: String,
        start: String,
        end: String,
    },
    Xread {
        keys: Vec<String>,
        ids: Vec<String>,
        block_timeout: Option<u64>, // Block timeout in milliseconds
    },
    Type(String),
}

pub fn parse_message(input: &str) -> Option<Command> {
    let lines: Vec<&str> = input.split("\r\n").collect();

    // Simple check: Is this an array?
    if !lines[0].starts_with('*') {
        return None;
    }

    // Redis commands are usually the 3rd element in the array
    // (*2, $4, ECHO...) -> index 2 is the command name
    let command_name = lines.get(2)?.to_uppercase();

    match command_name.as_str() {
        "PING" => Some(Command::Ping),
        "ECHO" => {
            // The value for ECHO is at index 4
            let content = lines.get(4)?;
            Some(Command::Echo(content.to_string()))
        }
        "SET" => {
            let key = lines.get(4)?.to_string();
            let value = lines.get(6)?.to_string();
            let mut px = None;

            if let Some(pos) = lines.iter().position(|&p| p.to_uppercase() == "PX") {
                // Skip the next line ($3) and get the one after (number)
                if let Some(ms_str) = lines.get(pos + 2) {
                    px = ms_str.parse::<u64>().ok();
                }
            }

            Some(Command::Set { key, value, px })
        }
        "GET" => {
            let key = lines.get(4)?.to_string();
            Some(Command::Get(key))
        }
        "RPUSH" => {
            let key = lines.get(4)?.to_string();
            let mut values = Vec::new();
            // Starting from index 6, every 2nd line is a new value (skip the $ metadata)
            let mut i = 6;
            while let Some(val) = lines.get(i) {
                values.push(val.to_string());
                i += 2;
            }
            Some(Command::Rpush { key, values })
        }
        "LPUSH" => {
            let key = lines.get(4)?.to_string();
            let mut values = Vec::new();
            // Starting from index 6, every 2nd line is a new value (skip the $ metadata)
            let mut i = 6;
            while let Some(val) = lines.get(i) {
                values.push(val.to_string());
                i += 2;
            }
            Some(Command::Lpush { key, values })
        }
        "LRANGE" => {
            let key = lines.get(4)?.to_string();
            let start = lines.get(6)?.parse::<i64>().ok()?;
            let stop = lines.get(8)?.parse::<i64>().ok()?;
            Some(Command::Lrange { key, start, stop })
        }
        "LLEN" => {
            let key = lines.get(4)?.to_string();
            Some(Command::Llen(key))
        }
        "LPOP" => {
            let key = lines.get(4)?.to_string();
            let count = lines.get(6).and_then(|s| s.parse::<usize>().ok());
            Some(Command::Lpop { key, count })
        }
        "BLPOP" => {
            let mut keys = Vec::new();
            let mut i = 4;

            // Filter out empty lines caused by the split at the end
            let filtered_lines: Vec<&str> =
                lines.iter().filter(|s| !s.is_empty()).cloned().collect();

            // The timeout is the very last valid element
            let timeout_str = filtered_lines.last()?;
            let timeout = timeout_str.parse::<f64>().ok()?;

            // Keys are between index 4 and the last element
            // In filtered_lines, indices are 0: *N, 1: $len, 2: BLPOP, 3: $len, 4: key1...
            while i < filtered_lines.len() - 1 {
                keys.push(filtered_lines.get(i)?.to_string());
                i += 2;
            }

            Some(Command::Blpop { keys, timeout })
        }
        "XADD" => {
            let key = lines.get(4)?.to_string();
            let id = lines.get(6)?.to_string();
            let mut fields = HashMap::new();
            let mut i = 8;
            while i < lines.len() - 1 {
                let field = lines.get(i)?.to_string();
                let value = lines.get(i + 2)?.to_string();
                fields.insert(field, value);
                i += 4; // Skip field metadata, field, value metadata, value
            }
            Some(Command::Xadd { key, id, fields })
        }
        "XRANGE" => {
            let key = lines.get(4)?.to_string();
            let start = lines.get(6)?.to_string();
            let end = lines.get(8)?.to_string();
            Some(Command::Xrange { key, start, end })
        }
        "XREAD" => {
            let mut block_timeout = None;
            let mut streams_index = 4; // Default index for STREAMS if no BLOCK is present

            // Check if the next argument is "BLOCK"
            if lines.get(4).map(|s| s.to_uppercase()) == Some("BLOCK".to_string()) {
                if let Some(block_ms_str) = lines.get(6) {
                    block_timeout = block_ms_str.parse::<u64>().ok();
                }
                streams_index = 8; // "STREAMS" moves further down the array
            }

            // Collect all remaining keys and IDs after "STREAMS"
            let mut args = Vec::new();
            let mut i = streams_index + 2;
            while let Some(arg) = lines.get(i) {
                args.push(arg.to_string());
                i += 2; // RESP layout: $len\r\nValue\r\n -> skip 2 to get next value
            }

            let num_streams = args.len() / 2;
            let (keys, ids) = args.split_at(num_streams);

            Some(Command::Xread {
                keys: keys.to_vec(),
                ids: ids.to_vec(),
                block_timeout,
            })
        }
        "TYPE" => {
            let key = lines.get(4)?.to_string();
            Some(Command::Type(key))
        }
        _ => None,
    }
}
