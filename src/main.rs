mod commands;
mod config;
mod connection;
mod rdb;
mod replication;
mod resp;
mod server;
mod store;

use std::net::TcpListener;
use std::sync::Arc;

use config::Config;
use server::ServerState;

fn main() {
    let config = Config::from_args(std::env::args().skip(1)).unwrap_or_else(|e| {
        eprintln!("error: {e}");
        std::process::exit(1);
    });
    let listener = TcpListener::bind(("127.0.0.1", config.port)).unwrap_or_else(|e| {
        eprintln!("error: cannot listen on port {}: {e}", config.port);
        std::process::exit(1);
    });
    println!("listening on port {}", config.port);
    let server = Arc::new(ServerState::new(&config));
    replication::replica::start(Arc::clone(&server), config.port);

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
