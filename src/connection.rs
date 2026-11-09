use std::io::{self, Read, Write};
use std::net::TcpStream;

use crate::commands::{self, Ctx, Transaction};
use crate::resp::{self, RespValue};
use crate::server::ServerState;

/// Per-connection state (subscriptions, replica flag, ... go here too).
#[derive(Default)]
pub struct ConnState {
    /// `Some` between MULTI and EXEC/DISCARD.
    pub transaction: Option<Transaction>,
}

pub fn handle(mut stream: TcpStream, server: &ServerState) -> io::Result<()> {
    let mut conn = ConnState::default();
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];

    loop {
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);

        // A single read may hold several (pipelined) commands, or only part of one.
        let mut out = Vec::new();
        loop {
            match resp::parse(&buf) {
                Ok(Some((value, used))) => {
                    buf.drain(..used);
                    if let Some(reply) = execute(value, server, &mut conn) {
                        reply.encode(&mut out);
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    RespValue::Error(format!("ERR Protocol error: {e}")).encode(&mut out);
                    buf.clear();
                    break;
                }
            }
        }
        if !out.is_empty() {
            stream.write_all(&out)?;
        }
    }
}

fn execute(value: RespValue, server: &ServerState, conn: &mut ConnState) -> Option<RespValue> {
    let words = match value {
        RespValue::Array(items) => items
            .into_iter()
            .map(|item| match item {
                RespValue::Bulk(s) | RespValue::Simple(s) => Some(s),
                _ => None,
            })
            .collect::<Option<Vec<String>>>(),
        _ => None,
    };
    match words {
        Some(words) if words.is_empty() => None,
        Some(words) => Some(commands::dispatch(words, &mut Ctx::new(server, conn))),
        None => Some(RespValue::Error("ERR Protocol error: expected an array of bulk strings".into())),
    }
}
