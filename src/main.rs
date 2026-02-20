#![allow(unused_imports)]
use std::collections::HashMap;
use std::io::{Read, Result as IoResult, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Condvar, Mutex};
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
type Cv = Arc<Condvar>;

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
}

struct Resp;

impl Resp {
    fn string(s: &str) -> String {
        format!("+{}\r\n", s)
    }
    fn error(msg: &str) -> String {
        format!("-{}\r\n", msg)
    }
    fn integer(i: usize) -> String {
        format!(":{}\r\n", i)
    }
    fn bulk_string(s: &str) -> String {
        format!("${}\r\n{}\r\n", s.len(), s)
    }
    fn null_bulk() -> &'static str {
        "$-1\r\n"
    }
    fn null_array() -> &'static str {
        "*-1\r\n"
    }
    fn array(len: usize) -> String {
        format!("*{}\r\n", len)
    }
}

trait RedisWrite {
    fn write_resp(&mut self, resp: impl AsRef<[u8]>) -> IoResult<()>;
}

impl RedisWrite for TcpStream {
    fn write_resp(&mut self, resp: impl AsRef<[u8]>) -> IoResult<()> {
        self.write_all(resp.as_ref())
    }
}

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let db: Db = Arc::new(Mutex::new(HashMap::new()));
    let cv = Arc::new(Condvar::new());

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                let db_clone = Arc::clone(&db);
                let cv_clone = Arc::clone(&cv);
                std::thread::spawn(|| handle_connection(stream, db_clone, cv_clone).unwrap());
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_connection(mut stream: TcpStream, db: Db, cv: Cv) -> IoResult<()> {
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
                    stream.write_resp(Resp::string("PONG"))?;
                }
                Command::Echo(content) => {
                    stream.write_resp(Resp::bulk_string(&content))?;
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
                    stream.write_resp(Resp::string("OK"))?;
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
                                    stream.write_resp(Resp::bulk_string(&s))?;
                                }
                                RedisValue::List(_) => {
                                    // Redis returns a specific error when calling GET on a List
                                    stream.write_resp(Resp::error("WRONGTYPE Operation against a key holding the wrong kind of value"))?;
                                }
                            }
                        }
                        None => {
                            // RESP Null Bulk String (-1)
                            stream.write_resp(Resp::null_bulk())?;
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
                        // RESP Integer format: ":<number>\r\n"
                        stream.write_resp(Resp::integer(list.len()))?;

                        cv.notify_all(); // Wake up any BLPOP waiters
                    } else {
                        // Technically Redis returns an error if you RPUSH to a key
                        // that already holds a String, but for now, we can just return an error.
                        stream.write_resp(Resp::error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        ))?;
                    }
                }
                Command::Lpush { key, values } => {
                    let mut map = db.lock().unwrap();

                    let entry = map.entry(key).or_insert(Entry {
                        value: RedisValue::List(Vec::new()),
                        created_at: Instant::now(),
                        expires_in: None,
                    });

                    if let RedisValue::List(ref mut list) = entry.value {
                        for val in values {
                            list.insert(0, val);
                        }
                        stream.write_resp(Resp::integer(list.len()))?;

                        cv.notify_all(); // Wake up any BLPOP waiters
                    } else {
                        stream.write_resp(Resp::error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        ))?;
                    }
                }
                Command::Lrange { key, start, stop } => {
                    let db_lock = db.lock().unwrap();

                    match db_lock.get(&key) {
                        Some(entry) => {
                            if let RedisValue::List(ref list) = entry.value {
                                let len = list.len() as i64;

                                // Normalize and clamp in one step per variable
                                let start_idx = (if start < 0 { len + start } else { start })
                                    .clamp(0, len)
                                    as usize;
                                let stop_idx = (if stop < 0 { len + stop } else { stop })
                                    .clamp(0, len - 1)
                                    as usize;

                                if start_idx >= list.len() || start_idx > stop_idx {
                                    stream.write_resp(Resp::array(0))?;
                                } else {
                                    let elements = &list[start_idx..=stop_idx];

                                    // Encode as RESP Array: *<count>\r\n
                                    let mut response = Resp::array(elements.len());
                                    for el in elements {
                                        response.push_str(&Resp::bulk_string(el));
                                    }
                                    stream.write_resp(response)?;
                                }
                            } else {
                                // If the key is a String, Redis returns an error
                                stream.write_resp(Resp::error("WRONGTYPE Operation against a key holding the wrong kind of value"))?;
                            }
                        }
                        None => {
                            // If list doesn't exist, return empty array
                            stream.write_resp(Resp::array(0))?;
                        }
                    }
                }
                Command::Llen(key) => {
                    let db_lock = db.lock().unwrap();

                    match db_lock.get(&key) {
                        Some(entry) => {
                            if let RedisValue::List(ref list) = entry.value {
                                stream.write_resp(Resp::integer(list.len()))?;
                            } else {
                                stream.write_resp(Resp::error("WRONGTYPE Operation against a key holding the wrong kind of value"))?;
                            }
                        }
                        None => {
                            // Redis returns 0 for non-existent keys
                            stream.write_resp(Resp::integer(0))?;
                        }
                    }
                }
                Command::Lpop { key, count } => {
                    let mut db_lock = db.lock().unwrap();

                    match db_lock.get_mut(&key) {
                        Some(entry) => {
                            if let RedisValue::List(ref mut list) = entry.value {
                                match count {
                                    None => {
                                        // LPOP without count
                                        if list.is_empty() {
                                            // List exists but is empty
                                            stream.write_resp(Resp::null_bulk())?;
                                        } else {
                                            // Remove the first element
                                            let val = list.remove(0);
                                            stream.write_resp(Resp::bulk_string(&val))?;
                                        }
                                    }
                                    Some(num) => {
                                        // LPOP with count
                                        let take_count = std::cmp::min(num, list.len());
                                        if take_count == 0 {
                                            stream.write_resp(Resp::null_array())?; // Or *0\r\n depending on Redis version
                                        } else {
                                            // Remove the first 'n' elements from the vector
                                            let popped_elements: Vec<String> =
                                                list.drain(0..take_count).collect();

                                            let mut response = Resp::array(popped_elements.len());
                                            for el in popped_elements {
                                                response.push_str(&Resp::bulk_string(&el));
                                            }
                                            stream.write_resp(response)?;
                                        }
                                    }
                                }
                            } else {
                                stream.write_resp(Resp::error("WRONGTYPE Operation against a key holding the wrong kind of value"))?;
                            }
                        }
                        None => {
                            stream.write_resp(Resp::null_bulk())?;
                        }
                    }
                }
                Command::Blpop { keys, timeout } => {
                    let mut map = db.lock().unwrap();

                    let timeout_duration = Duration::from_secs_f64(timeout);
                    let start_time = Instant::now();

                    loop {
                        // 1. Try to find a non-empty list
                        for key in &keys {
                            if let Some(Entry {
                                value: RedisValue::List(list),
                                ..
                            }) = map.get_mut(key)
                            {
                                if !list.is_empty() {
                                    let val = list.remove(0);
                                    // BLPOP returns a 2-element array: [key, value]
                                    let response = format!(
                                        "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                                        key.len(),
                                        key,
                                        val.len(),
                                        val
                                    );
                                    stream.write_resp(response)?;
                                    return Ok(());
                                }
                            }
                        }

                        // 2. Check if we already timed out
                        let elapsed = start_time.elapsed();
                        if timeout > 0.0 && elapsed >= timeout_duration {
                            stream.write_resp(Resp::null_array())?; // Redis returns Null Bulk String on timeout
                            return Ok(());
                        }

                        // 3. Wait to be notified or for timeout
                        if timeout == 0.0 {
                            map = cv.wait(map).unwrap();
                        } else {
                            let remaining = timeout_duration - elapsed;
                            let (new_map, _) = cv.wait_timeout(map, remaining).unwrap();
                            map = new_map;
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
        _ => None,
    }
}
