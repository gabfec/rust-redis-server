#![allow(unused_imports)]
use std::collections::HashMap;
use std::io::{Read, Result as IoResult, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

type Db = Arc<Mutex<HashMap<String, String>>>;

enum Command {
    Ping,
    Echo(String),
    Set(String, String), // Key, Value
    Get(String),         // Key
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
            match command {
                Command::Ping => {
                    stream.write_all(b"+PONG\r\n")?;
                }
                Command::Echo(content) => {
                    // RESP Bulk String format: "$length\r\ncontent\r\n"
                    let response = format!("${}\r\n{}\r\n", content.len(), content);
                    stream.write_all(response.as_bytes())?;
                }
                Command::Set(key, value) => {
                    let mut map = db.lock().unwrap(); // Lock the mutex
                    map.insert(key, value);
                    stream.write_all(b"+OK\r\n")?;
                }
                Command::Get(key) => {
                    let map = db.lock().unwrap();
                    match map.get(&key) {
                        Some(value) => {
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
            Some(Command::Set(key, value))
        }
        "GET" => {
            let key = lines.get(4)?.to_string();
            Some(Command::Get(key))
        }
        _ => None,
    }
}
