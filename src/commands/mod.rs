//! One file per command. To add a command:
//!   1. create `<family>/<name>.rs` with a struct implementing [`Command`],
//!   2. add `pub mod <name>;` to `<family>/mod.rs`,
//!   3. add one line to the table in [`lookup`].

mod connection;
mod keys;
mod list;
mod server;
mod stream;
mod string;
mod transaction;

pub use transaction::Transaction;

use std::ops::{Deref, DerefMut};
use std::str::FromStr;
use std::sync::MutexGuard;
use std::time::Duration;

use crate::connection::ConnState;
use crate::resp::RespValue;
use crate::server::ServerState;
use crate::store::{Store, WrongType};

pub fn dispatch(mut words: Vec<String>, ctx: &mut Ctx) -> RespValue {
    let name = words.remove(0).to_ascii_lowercase();
    let Some(handler) = lookup(&name) else {
        // Like Redis, an unknown command makes the whole transaction fail.
        if let Some(tx) = &mut ctx.conn.transaction {
            tx.aborted = true;
        }
        return RespValue::Error(format!("ERR unknown command '{name}'"));
    };
    let queue = !transaction::runs_immediately(&name);
    let args = Args::new(name, words);

    // Between MULTI and EXEC, commands are queued instead of run.
    if let Some(tx) = &mut ctx.conn.transaction
        && queue
    {
        tx.queued.push((handler, args));
        return RespValue::Simple("QUEUED".into());
    }
    handler(args, ctx)
}

/// Parses and runs one command.
pub type Handler = fn(Args, &mut Ctx) -> RespValue;

/// The command table.
fn lookup(name: &str) -> Option<Handler> {
    let handler: Handler = match name {
        // connection
        "ping" => run::<connection::ping::Ping>,
        "echo" => run::<connection::echo::Echo>,
        // keys
        "type" => run::<keys::r#type::Type>,
        // strings
        "get" => run::<string::get::Get>,
        "set" => run::<string::set::Set>,
        "incr" => run::<string::incr::Incr>,
        // lists
        "rpush" => run::<list::rpush::Rpush>,
        "lpush" => run::<list::lpush::Lpush>,
        "lrange" => run::<list::lrange::Lrange>,
        "llen" => run::<list::llen::Llen>,
        "lpop" => run::<list::lpop::Lpop>,
        "blpop" => run::<list::blpop::Blpop>,
        // streams
        "xadd" => run::<stream::xadd::Xadd>,
        "xrange" => run::<stream::xrange::Xrange>,
        "xread" => run::<stream::xread::Xread>,
        // server
        "info" => run::<server::info::Info>,
        // transactions
        "multi" => run::<transaction::multi::Multi>,
        "exec" => run::<transaction::exec::Exec>,
        "discard" => run::<transaction::discard::Discard>,
        "watch" => run::<transaction::watch::Watch>,
        "unwatch" => run::<transaction::unwatch::Unwatch>,

        _ => return None,
    };
    Some(handler)
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
    pub fn atomically<R>(&mut self, f: impl FnOnce(&mut Ctx) -> R) -> R {
        assert!(self.held.is_none(), "store is already locked");
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
    /// Write commands wake up blocked clients (and, later, get propagated to replicas).
    const IS_WRITE: bool = false;

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
