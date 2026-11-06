mod commands;
mod connection;
mod resp;
mod server;
mod store;

use std::net::TcpListener;
use std::sync::Arc;

use server::ServerState;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let server = Arc::new(ServerState::new());

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                let server = Arc::clone(&server);
                std::thread::spawn(move || {
                    if let Err(e) = connection::handle(stream, &server) {
                        eprintln!("connection error: {e}");
                    }
                });
            }
            Err(e) => eprintln!("error: {e}"),
        }
    }
}
