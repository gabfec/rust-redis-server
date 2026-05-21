#![allow(unused_imports)]
use std::net::TcpListener;
use std::sync::{Arc, Condvar, Mutex};

mod command;
mod db;
mod handlers;
mod resp;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let db: db::Db = Arc::new(Mutex::new(std::collections::HashMap::new()));
    let cv = Arc::new(Condvar::new());

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                let db_clone = Arc::clone(&db);
                let cv_clone = Arc::clone(&cv);
                std::thread::spawn(|| handlers::handle_connection(stream, db_clone, cv_clone).unwrap());
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
