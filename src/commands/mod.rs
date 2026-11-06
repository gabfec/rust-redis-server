//! One file per command. To add a command:
//!   1. create `<family>/<name>.rs` with a struct implementing [`Command`],
//!   2. add `pub mod <name>;` to `<family>/mod.rs`,
//!   3. add one line to the table in [`dispatch`].

mod connection;
mod keys;
mod list;
mod stream;
mod string;

use std::str::FromStr;

use crate::connection::ConnState;
use crate::resp::RespValue;
use crate::server::ServerState;
use crate::store::WrongType;

pub fn dispatch(mut words: Vec<String>, ctx: &mut Ctx) -> RespValue {
    let name = words.remove(0).to_ascii_lowercase();
    let args = Args::new(name.clone(), words);

    match name.as_str() {
        // connection
        "ping" => run::<connection::ping::Ping>(args, ctx),
        "echo" => run::<connection::echo::Echo>(args, ctx),
        // keys
        "type" => run::<keys::r#type::Type>(args, ctx),
        // strings
        "get" => run::<string::get::Get>(args, ctx),
        "set" => run::<string::set::Set>(args, ctx),
        // lists
        "rpush" => run::<list::rpush::Rpush>(args, ctx),
        "lpush" => run::<list::lpush::Lpush>(args, ctx),
        "lrange" => run::<list::lrange::Lrange>(args, ctx),
        "llen" => run::<list::llen::Llen>(args, ctx),
        "lpop" => run::<list::lpop::Lpop>(args, ctx),
        "blpop" => run::<list::blpop::Blpop>(args, ctx),
        // streams
        "xadd" => run::<stream::xadd::Xadd>(args, ctx),
        "xrange" => run::<stream::xrange::Xrange>(args, ctx),
        "xread" => run::<stream::xread::Xread>(args, ctx),

        _ => RespValue::Error(format!("ERR unknown command '{name}'")),
    }
}

/// Everything a command may touch while executing.
pub struct Ctx<'a> {
    pub server: &'a ServerState,
    #[allow(dead_code)] // will be used by MULTI/EXEC, SUBSCRIBE, REPLCONF, ...
    pub conn: &'a mut ConnState,
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
