#![allow(unused_imports)]
use std::io::{Read, Result as IoResult, Write};
use std::net::{TcpListener, TcpStream};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                std::thread::spawn(|| handle_connection(stream).unwrap());
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_connection(mut stream: TcpStream) -> IoResult<()> {
    let mut buffer = [0; 1024];
    loop {
        let bytes_read = stream.read(&mut buffer);
        match bytes_read {
            Ok(0) => break,
            Ok(_) => stream.write_all(b"+PONG\r\n").unwrap(),
            Err(_) => break,
        }
    }
    Ok(())
}
