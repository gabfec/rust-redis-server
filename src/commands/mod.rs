//! One file per command. To add a command:
//!   1. create `<family>/<name>.rs` with a struct implementing [`Command`],
//!   2. add `pub mod <name>;` to `<family>/mod.rs`,
//!   3. add one line to the table in [`lookup`].

mod bitmap;
mod connection;
mod geo;
mod keys;
mod list;
mod pubsub;
mod replication;
mod server;
mod stream;
mod string;
mod transaction;
mod zset;

pub use transaction::Transaction;

use std::ops::{Deref, DerefMut};
use std::str::FromStr;
use std::sync::MutexGuard;
use std::time::Duration;

use crate::connection::ConnState;
use crate::resp::RespValue;
use crate::server::ServerState;
use crate::store::{Store, WrongType};

/// Runs (or, inside MULTI, queues) one command. `words` includes the
/// command name.
pub fn dispatch(words: Vec<String>, ctx: &mut Ctx) -> RespValue {
    let name = words[0].to_ascii_lowercase();
    let Some(spec) = lookup(&name) else {
        // Like Redis, an unknown command makes the whole transaction fail.
        if let Some(tx) = &mut ctx.conn.transaction {
            tx.aborted = true;
        }
        return RespValue::Error(format!("ERR unknown command '{name}'"));
    };

    // A client with subscriptions may only run a few commands.
    if !ctx.conn.subscriptions.is_empty() && !pubsub::allowed_in_subscribed_mode(&name) {
        return RespValue::Error(format!(
            "ERR Can't execute '{name}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
        ));
    }

    // Between MULTI and EXEC, commands are queued instead of run.
    if let Some(tx) = &mut ctx.conn.transaction
        && !transaction::runs_immediately(&name)
    {
        tx.queued.push((spec, words));
        return RespValue::Simple("QUEUED".into());
    }
    execute(spec, words, ctx)
}

/// Runs a command that was already looked up. `words` includes the
/// command name, as the client sent it.
pub fn execute(spec: Spec, mut words: Vec<String>, ctx: &mut Ctx) -> RespValue {
    if !spec.propagate {
        let name = words.remove(0).to_ascii_lowercase();
        return (spec.run)(Args::new(name, words), ctx);
    }
    // Run it, forward it to the replicas and log it to the AOF under one
    // lock, so they get the writes in exactly the order they happened.
    ctx.atomically(|ctx| {
        let name = words[0].to_ascii_lowercase();
        let reply = (spec.run)(Args::new(name, words[1..].to_vec()), ctx);
        if !matches!(reply, RespValue::Error(_)) {
            ctx.server.replication.propagate(&words);
            if let Some(aof) = ctx.server.aof.get()
                && let Err(e) = aof.append(&words)
            {
                eprintln!("error: cannot write to the append-only file: {e}");
            }
        }
        reply
    })
}

/// Parses and runs one command.
pub type Handler = fn(Args, &mut Ctx) -> RespValue;

/// An entry of the command table.
#[derive(Clone, Copy)]
pub struct Spec {
    run: Handler,
    /// Forward to replicas when it succeeds.
    propagate: bool,
}

fn spec<C: Command>() -> Spec {
    Spec { run: run::<C>, propagate: C::PROPAGATE }
}

/// The command table.
fn lookup(name: &str) -> Option<Spec> {
    let spec = match name {
        // connection
        "ping" => spec::<connection::ping::Ping>(),
        "echo" => spec::<connection::echo::Echo>(),
        // keys
        "type" => spec::<keys::r#type::Type>(),
        "keys" => spec::<keys::keys::Keys>(),
        // strings
        "get" => spec::<string::get::Get>(),
        "set" => spec::<string::set::Set>(),
        "incr" => spec::<string::incr::Incr>(),
        "strlen" => spec::<string::strlen::Strlen>(),
        // bitmaps
        "setbit" => spec::<bitmap::setbit::Setbit>(),
        "getbit" => spec::<bitmap::getbit::Getbit>(),
        "bitcount" => spec::<bitmap::bitcount::Bitcount>(),
        "bitop" => spec::<bitmap::bitop::Bitop>(),
        // geospatial
        "geoadd" => spec::<geo::geoadd::Geoadd>(),
        "geopos" => spec::<geo::geopos::Geopos>(),
        "geodist" => spec::<geo::geodist::Geodist>(),
        "geosearch" => spec::<geo::geosearch::Geosearch>(),
        // lists
        "rpush" => spec::<list::rpush::Rpush>(),
        "lpush" => spec::<list::lpush::Lpush>(),
        "lrange" => spec::<list::lrange::Lrange>(),
        "llen" => spec::<list::llen::Llen>(),
        "lpop" => spec::<list::lpop::Lpop>(),
        "blpop" => spec::<list::blpop::Blpop>(),
        // streams
        "xadd" => spec::<stream::xadd::Xadd>(),
        "xrange" => spec::<stream::xrange::Xrange>(),
        "xread" => spec::<stream::xread::Xread>(),
        // pub/sub
        "subscribe" => spec::<pubsub::subscribe::Subscribe>(),
        "publish" => spec::<pubsub::publish::Publish>(),
        "unsubscribe" => spec::<pubsub::unsubscribe::Unsubscribe>(),
        // replication
        "replconf" => spec::<replication::replconf::Replconf>(),
        "psync" => spec::<replication::psync::Psync>(),
        "wait" => spec::<replication::wait::Wait>(),
        // server
        "info" => spec::<server::info::Info>(),
        "config" => spec::<server::config::Config>(),
        // sorted sets
        "zadd" => spec::<zset::zadd::Zadd>(),
        "zrank" => spec::<zset::zrank::Zrank>(),
        "zrange" => spec::<zset::zrange::Zrange>(),
        "zcard" => spec::<zset::zcard::Zcard>(),
        "zscore" => spec::<zset::zscore::Zscore>(),
        "zrem" => spec::<zset::zrem::Zrem>(),
        // transactions
        "multi" => spec::<transaction::multi::Multi>(),
        "exec" => spec::<transaction::exec::Exec>(),
        "discard" => spec::<transaction::discard::Discard>(),
        "watch" => spec::<transaction::watch::Watch>(),
        "unwatch" => spec::<transaction::unwatch::Unwatch>(),

        _ => return None,
    };
    Some(spec)
}

/// Everything a command may touch while executing.
pub struct Ctx<'a> {
    server: &'a ServerState,
    conn: &'a mut ConnState,
    /// Set while the store is already locked (inside EXEC).
    held: Option<&'a mut Store>,
}

impl<'a> Ctx<'a> {
    pub fn new(server: &'a ServerState, conn: &'a mut ConnState) -> Self {
        Ctx { server, conn, held: None }
    }

    /// The keyspace. Commands must go through this (not `ServerState`).
    pub fn store(&mut self) -> StoreRef<'_> {
        match &mut self.held {
            Some(store) => StoreRef::Held(store),
            None => StoreRef::Locked(self.server.store()),
        }
    }

    /// See [`ServerState::wait_for`]. Inside EXEC this checks once and
    /// never blocks, like Redis.
    pub fn wait_for<T>(
        &mut self,
        timeout: Option<Duration>,
        mut check: impl FnMut(&mut Store) -> Option<T>,
    ) -> Option<T> {
        match &mut self.held {
            Some(store) => check(store),
            None => self.server.wait_for(timeout, check),
        }
    }

    /// Runs `f` with the store locked throughout, so that no other client
    /// can run a command in between (used by EXEC).
    /// If the store is already locked (inside EXEC), `f` just runs.
    pub fn atomically<R>(&mut self, f: impl FnOnce(&mut Ctx) -> R) -> R {
        if self.held.is_some() {
            return f(self);
        }
        let mut store = self.server.store();
        f(&mut Ctx { server: self.server, conn: self.conn, held: Some(&mut store) })
    }
}

/// The store, either locked just for this command or already held by EXEC.
pub enum StoreRef<'a> {
    Locked(MutexGuard<'a, Store>),
    Held(&'a mut Store),
}

impl Deref for StoreRef<'_> {
    type Target = Store;
    fn deref(&self) -> &Store {
        match self {
            StoreRef::Locked(guard) => guard,
            StoreRef::Held(store) => store,
        }
    }
}

impl DerefMut for StoreRef<'_> {
    fn deref_mut(&mut self) -> &mut Store {
        match self {
            StoreRef::Locked(guard) => guard,
            StoreRef::Held(store) => store,
        }
    }
}

pub trait Command: Sized {
    /// Write commands wake up blocked clients.
    const IS_WRITE: bool = false;
    /// Forward the command to replicas (as received) when it succeeds.
    const PROPAGATE: bool = Self::IS_WRITE;

    fn parse(args: Args) -> Result<Self, CmdError>;
    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError>;
}

fn run<C: Command>(args: Args, ctx: &mut Ctx) -> RespValue {
    let result = C::parse(args).and_then(|cmd| cmd.execute(ctx));
    if C::IS_WRITE && result.is_ok() {
        ctx.server.notify_changed();
    }
    result.unwrap_or_else(RespValue::from)
}

#[derive(Debug)]
pub enum CmdError {
    WrongArgs(String),
    Syntax,
    NotInteger,
    WrongType,
    /// Full error message, including the "ERR" prefix.
    Other(String),
}

impl From<WrongType> for CmdError {
    fn from(_: WrongType) -> Self {
        CmdError::WrongType
    }
}

impl From<CmdError> for RespValue {
    fn from(err: CmdError) -> Self {
        RespValue::Error(match err {
            CmdError::WrongArgs(name) => format!("ERR wrong number of arguments for '{name}' command"),
            CmdError::Syntax => "ERR syntax error".into(),
            CmdError::NotInteger => "ERR value is not an integer or out of range".into(),
            CmdError::WrongType => {
                "WRONGTYPE Operation against a key holding the wrong kind of value".into()
            }
            CmdError::Other(msg) => msg,
        })
    }
}

/// The arguments after the command name.
pub struct Args {
    name: String,
    items: std::vec::IntoIter<String>,
}

impl Args {
    fn new(name: String, items: Vec<String>) -> Self {
        Args { name, items: items.into_iter() }
    }

    pub fn wrong_args(&self) -> CmdError {
        CmdError::WrongArgs(self.name.clone())
    }

    /// Next argument; error if missing.
    pub fn next_string(&mut self) -> Result<String, CmdError> {
        self.items.next().ok_or_else(|| self.wrong_args())
    }

    /// Next argument parsed as `T`; error if missing or malformed.
    pub fn next_parse<T: FromStr>(&mut self) -> Result<T, CmdError> {
        self.next_string()?.parse().map_err(|_| CmdError::NotInteger)
    }

    /// Next argument, if any (for optional arguments / flags).
    pub fn next_opt(&mut self) -> Option<String> {
        self.items.next()
    }

    /// All remaining arguments; error if fewer than `min`.
    pub fn rest_min(self, min: usize) -> Result<Vec<String>, CmdError> {
        if self.items.len() < min {
            return Err(self.wrong_args());
        }
        Ok(self.items.collect())
    }

    /// Error if any arguments are left over.
    pub fn finish(self) -> Result<(), CmdError> {
        if self.items.len() > 0 {
            return Err(self.wrong_args());
        }
        Ok(())
    }
}

/// Helpers for unit-testing commands without a socket.
#[cfg(test)]
pub mod test_util {
    use super::{dispatch, Ctx};
    use crate::connection::ConnState;
    use crate::resp::RespValue;
    use crate::server::ServerState;

    /// A server plus one client connection.
    #[derive(Default)]
    pub struct Client {
        pub server: ServerState,
        pub conn: ConnState,
    }

    impl Client {
        pub fn cmd(&mut self, words: &[&str]) -> RespValue {
            run(&self.server, &mut self.conn, words)
        }

        /// Runs a command as a different client of the same server.
        pub fn other(&self, words: &[&str]) -> RespValue {
            run(&self.server, &mut ConnState::default(), words)
        }
    }

    /// Runs one command. Use it directly to share one server between
    /// several connections/threads (e.g. to test blocking commands).
    pub fn run(server: &ServerState, conn: &mut ConnState, words: &[&str]) -> RespValue {
        let words = words.iter().map(|w| w.to_string()).collect();
        dispatch(words, &mut Ctx::new(server, conn))
    }

    pub fn err(msg: &str) -> RespValue {
        RespValue::Error(msg.into())
    }
}

#[cfg(test)]
mod tests {
    use super::test_util::{err, Client};

    #[test]
    fn unknown_command() {
        assert_eq!(Client::default().cmd(&["NOPE", "x"]), err("ERR unknown command 'nope'"));
    }

    #[test]
    fn command_names_are_case_insensitive() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["ping"]), c.cmd(&["PiNg"]));
    }

    #[test]
    fn wrong_number_of_arguments() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["GET"]), err("ERR wrong number of arguments for 'get' command"));
        assert_eq!(c.cmd(&["GET", "a", "b"]), err("ERR wrong number of arguments for 'get' command"));
    }
}
