#![allow(unused_imports)]
use std::collections::HashMap;
use std::io::{Read, Result as IoResult, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug)]
struct Entry {
    value: String,
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
                            value,
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
                        Some(Entry { value, .. }) => {
                            let response = format!("${}\r\n{}\r\n", value.len(), value);
                            stream.write_all(response.as_bytes())?;
                        }
                        None => {
                            // RESP Null Bulk String (-1)
                            stream.write_all(b"$-1\r\n")?;
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
        _ => None,
    }
}
