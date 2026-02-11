#![allow(unused_imports)]
use std::collections::HashMap;
use std::io::{Read, Result as IoResult, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug)]
enum RedisValue {
    String(String),
    List(Vec<String>),
}

#[derive(Debug)]
struct Entry {
    value: RedisValue,
    created_at: Instant,
    expires_in: Option<Duration>,
}

type Db = Arc<Mutex<HashMap<String, Entry>>>;

#[derive(Debug)]
enum Command {
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
    Lrange {
        key: String,
        start: usize,
        stop: usize,
    },
}

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let db: Db = Arc::new(Mutex::new(HashMap::new()));

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                let db_clone = Arc::clone(&db);
                std::thread::spawn(|| handle_connection(stream, db_clone).unwrap());
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_connection(mut stream: TcpStream, db: Db) -> IoResult<()> {
    let mut buffer = [0; 1024];
    loop {
        let bytes_read = stream.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }

        let input = String::from_utf8_lossy(&buffer[..bytes_read]);

        if let Some(command) = parse_message(&input) {
            println!("Received command: {:?}", command);

            match command {
                Command::Ping => {
                    stream.write_all(b"+PONG\r\n")?;
                }
                Command::Echo(content) => {
                    // RESP Bulk String format: "$length\r\ncontent\r\n"
                    let response = format!("${}\r\n{}\r\n", content.len(), content);
                    stream.write_all(response.as_bytes())?;
                }
                Command::Set { key, value, px } => {
                    let mut db_lock = db.lock().unwrap();

                    db_lock.insert(
                        key,
                        Entry {
                            value: RedisValue::String(value),
                            created_at: Instant::now(),
                            expires_in: px.map(Duration::from_millis),
                        },
                    );
                    stream.write_all(b"+OK\r\n")?;
                }
                Command::Get(key) => {
                    let mut db_lock = db.lock().unwrap();

                    let is_expired = if let Some(entry) = db_lock.get(&key) {
                        if let Some(duration) = entry.expires_in {
                            entry.created_at.elapsed() > duration
                        } else {
                            false
                        }
                    } else {
                        false
                    };

                    if is_expired {
                        db_lock.remove(&key);
                    }

                    match db_lock.get(&key) {
                        Some(entry) => {
                            // We must match on the type of value stored
                            match &entry.value {
                                RedisValue::String(s) => {
                                    let response = format!("${}\r\n{}\r\n", s.len(), s);
                                    stream.write_all(response.as_bytes())?;
                                }
                                RedisValue::List(_) => {
                                    // Redis returns a specific error when calling GET on a List
                                    stream.write_all(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")?;
                                }
                            }
                        }
                        None => {
                            // RESP Null Bulk String (-1)
                            stream.write_all(b"$-1\r\n")?;
                        }
                    }
                }
                Command::Rpush { key, values } => {
                    let mut map = db.lock().unwrap();

                    let entry = map.entry(key).or_insert(Entry {
                        value: RedisValue::List(Vec::new()),
                        created_at: Instant::now(),
                        expires_in: None,
                    });

                    if let RedisValue::List(ref mut list) = entry.value {
                        for val in values {
                            list.push(val);
                        }
                        let length = list.len();
                        // RESP Integer format: ":<number>\r\n"
                        let response = format!(":{}\r\n", length);
                        stream.write_all(response.as_bytes())?;
                    } else {
                        // Technically Redis returns an error if you RPUSH to a key
                        // that already holds a String, but for now, we can just return an error.
                        stream.write_all(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")?;
                    }
                }
                Command::Lrange { key, start, stop } => {
                    let db_lock = db.lock().unwrap();

                    match db_lock.get(&key) {
                        Some(entry) => {
                            if let RedisValue::List(ref list) = entry.value {
                                // If start >= length, return empty
                                if start >= list.len() || start > stop {
                                    stream.write_all(b"*0\r\n")?;
                                } else {
                                    // If stop >= length, treat as last element
                                    let actual_stop = std::cmp::min(stop, list.len() - 1);

                                    // The slice range is [start..actual_stop + 1] because Rust ranges are exclusive
                                    let elements = &list[start..=actual_stop];

                                    // Encode as RESP Array: *<count>\r\n
                                    let mut response = format!("*{}\r\n", elements.len());
                                    for el in elements {
                                        response.push_str(&format!("${}\r\n{}\r\n", el.len(), el));
                                    }
                                    stream.write_all(response.as_bytes())?;
                                }
                            } else {
                                // If the key is a String, Redis returns an error
                                stream.write_all(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")?;
                            }
                        }
                        None => {
                            // If list doesn't exist, return empty array
                            stream.write_all(b"*0\r\n")?;
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

fn parse_message(input: &str) -> Option<Command> {
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
            // For now, we just grab the first value at index 6
            let mut values = Vec::new();
            // Starting from index 6, every 2nd line is a new value (skip the $ metadata)
            let mut i = 6;
            while let Some(val) = lines.get(i) {
                values.push(val.to_string());
                i += 2;
            }
            Some(Command::Rpush { key, values })
        }
        "LRANGE" => {
            let key = lines.get(4)?.to_string();
            let start = lines.get(6)?.parse::<usize>().ok()?;
            let stop = lines.get(8)?.parse::<usize>().ok()?;
            Some(Command::Lrange { key, start, stop })
        }
        _ => None,
    }
}
