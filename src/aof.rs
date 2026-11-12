//! Append-only file (AOF) persistence: with `--appendonly yes`, the server
//! keeps its AOF files in `<dir>/<appenddirname>`, along with a manifest
//! (`<appendfilename>.manifest`) listing them, one per line:
//!
//! ```text
//! file appendonly.aof.1.incr.aof seq 1 type i
//! ```
//!
//! Every write command is appended to the incremental (`type i`) file
//! in RESP, exactly as the client sent it. At startup the file is
//! replayed to rebuild the data.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use crate::commands::{self, Ctx};
use crate::config::Config;
use crate::connection::{command_words, ConnState};
use crate::resp::{self, RespValue};
use crate::server::ServerState;

/// `<dir>/<appenddirname>`
fn aof_dir(config: &Config) -> PathBuf {
    Path::new(&config.dir).join(&config.appenddirname)
}

/// The first incremental AOF file: `<appendfilename>.1.incr.aof`.
fn first_incr_file(config: &Config) -> String {
    format!("{}.1.incr.aof", config.appendfilename)
}

fn manifest_path(config: &Config) -> PathBuf {
    aof_dir(config).join(format!("{}.manifest", config.appendfilename))
}

/// Sets up the AOF directory, file and manifest at startup, if AOF is
/// enabled. An existing manifest is kept: it says which files to use.
pub fn init(config: &Config) -> io::Result<()> {
    if config.appendonly != "yes" {
        return Ok(());
    }
    let dir = aof_dir(config);
    fs::create_dir_all(&dir)?;
    let manifest = manifest_path(config);
    if manifest.exists() {
        return Ok(());
    }
    let file = first_incr_file(config);
    // Created empty if missing; never truncated.
    OpenOptions::new().create(true).append(true).open(dir.join(&file))?;
    fs::write(manifest, format!("file {file} seq 1 type i\n"))
}

/// The incremental AOF file named in the manifest.
fn incr_file_from_manifest(config: &Config) -> io::Result<PathBuf> {
    let manifest = fs::read_to_string(manifest_path(config))?;
    for line in manifest.lines() {
        // "file <name> seq <n> type <t>": key/value pairs.
        let fields: Vec<&str> = line.split_whitespace().collect();
        let field = |key| fields.chunks(2).find(|kv| kv[0] == key).and_then(|kv| kv.get(1).copied());
        if let (Some(name), Some("i")) = (field("file"), field("type")) {
            return Ok(aof_dir(config).join(name));
        }
    }
    Err(io::Error::other("the AOF manifest names no incremental (type i) file"))
}

/// Runs every command in the AOF file, as if a client had sent them.
/// Must happen before the AOF is opened for writing, so the commands
/// aren't logged again. Returns how many commands ran.
pub fn replay(server: &ServerState, config: &Config) -> io::Result<usize> {
    if config.appendonly != "yes" || !manifest_path(config).exists() {
        return Ok(0);
    }
    let bytes = fs::read(incr_file_from_manifest(config)?)?;
    let mut conn = ConnState::default();
    let (mut pos, mut count) = (0, 0);
    while pos < bytes.len() {
        let parsed = resp::parse(&bytes[pos..]).map_err(|e| io::Error::other(e.to_string()))?;
        let Some((value, used)) = parsed else {
            eprintln!("warning: ignoring an incomplete command at the end of the AOF");
            break;
        };
        pos += used;
        let words = command_words(value)
            .filter(|words| !words.is_empty())
            .ok_or_else(|| io::Error::other("the AOF holds something that isn't a command"))?;
        commands::dispatch(words, &mut Ctx::new(server, &mut conn));
        count += 1;
    }
    Ok(count)
}

/// The file write commands are appended to.
pub struct Aof {
    file: Mutex<File>,
    /// appendfsync always: flush to disk before replying.
    fsync_always: bool,
}

impl Aof {
    /// Opens the file named in the manifest (see `init`), if AOF is enabled.
    pub fn open(config: &Config) -> io::Result<Option<Aof>> {
        if config.appendonly != "yes" {
            return Ok(None);
        }
        let file = OpenOptions::new().append(true).open(incr_file_from_manifest(config)?)?;
        if config.appendfsync == "everysec" {
            // Flush to disk once a second, in the background.
            let syncer = file.try_clone()?;
            thread::spawn(move || loop {
                thread::sleep(Duration::from_secs(1));
                let _ = syncer.sync_data();
            });
        }
        Ok(Some(Aof { file: Mutex::new(file), fsync_always: config.appendfsync == "always" }))
    }

    /// Appends a command, as the client sent it.
    pub fn append(&self, words: &[String]) -> io::Result<()> {
        let mut out = Vec::new();
        RespValue::bulk_array(words.iter().cloned()).encode(&mut out);
        let mut file = self.file.lock().unwrap();
        file.write_all(&out)?;
        if self.fsync_always {
            file.sync_data()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::test_util::Client;
    use crate::rdb::test_util::temp_dir;
    use crate::server::ServerState;

    /// A client of a server with AOF enabled in `dir`.
    fn aof_client(config: &Config) -> Client {
        init(config).unwrap();
        let c = Client { server: ServerState::new(config), ..Client::default() };
        let _ = c.server.aof.set(Aof::open(config).unwrap().unwrap());
        c
    }

    fn aof_config(dir: &Path) -> Config {
        Config {
            dir: dir.display().to_string(),
            appendonly: "yes".into(),
            appenddirname: "aofdir".into(),
            appendfilename: "log.aof".into(),
            ..Config::default()
        }
    }

    #[test]
    fn creates_the_directory_when_enabled() {
        let dir = temp_dir("aof-dir");
        init(&aof_config(&dir)).unwrap();
        assert!(dir.join("aofdir").is_dir());
        init(&aof_config(&dir)).unwrap(); // already there: fine
    }

    #[test]
    fn creates_an_empty_aof_file() {
        let dir = temp_dir("aof-file");
        init(&aof_config(&dir)).unwrap();
        let file = dir.join("aofdir/log.aof.1.incr.aof");
        assert_eq!(fs::read(&file).unwrap(), b"");

        fs::write(&file, b"existing").unwrap();
        fs::remove_file(dir.join("aofdir/log.aof.manifest")).unwrap();
        init(&aof_config(&dir)).unwrap();
        assert_eq!(fs::read(&file).unwrap(), b"existing", "not truncated on restart");
    }

    #[test]
    fn creates_the_manifest() {
        let dir = temp_dir("aof-manifest");
        init(&aof_config(&dir)).unwrap();
        assert_eq!(
            fs::read_to_string(dir.join("aofdir/log.aof.manifest")).unwrap(),
            "file log.aof.1.incr.aof seq 1 type i\n"
        );
    }

    #[test]
    fn keeps_an_existing_manifest() {
        let dir = temp_dir("aof-existing-manifest");
        fs::create_dir_all(dir.join("aofdir")).unwrap();
        let manifest = "file other.aof.1.incr.aof seq 1 type i\n";
        fs::write(dir.join("aofdir/log.aof.manifest"), manifest).unwrap();
        init(&aof_config(&dir)).unwrap();
        assert_eq!(fs::read_to_string(dir.join("aofdir/log.aof.manifest")).unwrap(), manifest);
        assert!(!dir.join("aofdir/log.aof.1.incr.aof").exists());
    }

    #[test]
    fn does_nothing_when_disabled() {
        let dir = temp_dir("aof-off");
        init(&Config { appendonly: "no".into(), ..aof_config(&dir) }).unwrap();
        assert!(!dir.join("aofdir").exists());
    }

    #[test]
    fn writes_a_command_to_the_file_named_in_the_manifest() {
        let dir = temp_dir("aof-write");
        fs::create_dir_all(dir.join("aofdir")).unwrap();
        fs::write(dir.join("aofdir/log.aof.manifest"), "file random.aof.1.incr.aof seq 1 type i\n").unwrap();
        fs::write(dir.join("aofdir/random.aof.1.incr.aof"), "").unwrap();
        let config = Config { appendfsync: "always".into(), ..aof_config(&dir) };

        let mut c = aof_client(&config);
        c.cmd(&["SET", "foo", "100"]);
        assert_eq!(
            fs::read_to_string(dir.join("aofdir/random.aof.1.incr.aof")).unwrap(),
            "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n100\r\n"
        );
        assert!(!dir.join("aofdir/log.aof.1.incr.aof").exists());
    }

    fn read_log(dir: &Path) -> String {
        fs::read_to_string(dir.join("aofdir/log.aof.1.incr.aof")).unwrap()
    }

    #[test]
    fn appends_multiple_commands_in_order() {
        let dir = temp_dir("aof-multiple");
        let mut c = aof_client(&Config { appendfsync: "always".into(), ..aof_config(&dir) });
        c.cmd(&["SET", "foo", "100"]);
        c.cmd(&["SET", "bar", "200"]);
        c.cmd(&["INCR", "foo"]);
        c.cmd(&["LPUSH", "l", "x"]);
        assert_eq!(
            read_log(&dir),
            "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n100\r\n\
             *3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n200\r\n\
             *2\r\n$4\r\nINCR\r\n$3\r\nfoo\r\n\
             *3\r\n$5\r\nLPUSH\r\n$1\r\nl\r\n$1\r\nx\r\n"
        );
    }

    #[test]
    fn only_write_commands_are_logged() {
        let dir = temp_dir("aof-filter");
        let mut c = aof_client(&Config { appendfsync: "always".into(), ..aof_config(&dir) });
        c.cmd(&["SET", "foo", "1"]);
        c.cmd(&["GET", "foo"]);
        c.cmd(&["PING"]);
        c.cmd(&["ECHO", "hello"]);
        c.cmd(&["CONFIG", "GET", "dir"]);
        c.cmd(&["KEYS", "*"]);
        c.cmd(&["INCR", "foo"]);
        c.cmd(&["SET", "s", "x"]);
        c.cmd(&["INCR", "s"]); // fails: not logged
        c.cmd(&["SET", "bar", "2"]);
        assert_eq!(
            read_log(&dir),
            "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$1\r\n1\r\n\
             *2\r\n$4\r\nINCR\r\n$3\r\nfoo\r\n\
             *3\r\n$3\r\nSET\r\n$1\r\ns\r\n$1\r\nx\r\n\
             *3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$1\r\n2\r\n"
        );
    }

    /// AOF files as the tester creates them: a manifest naming
    /// `random.aof.1.incr.aof`, holding `log`.
    fn existing_aof(name: &str, log: &str) -> (PathBuf, Config) {
        let dir = temp_dir(name);
        fs::create_dir_all(dir.join("aofdir")).unwrap();
        fs::write(dir.join("aofdir/log.aof.manifest"), "file random.aof.1.incr.aof seq 1 type i\n").unwrap();
        fs::write(dir.join("aofdir/random.aof.1.incr.aof"), log).unwrap();
        let config = aof_config(&dir);
        (dir, config)
    }

    /// Starts a server like main() does: init, replay, then open the AOF.
    fn restart(config: &Config) -> Client {
        init(config).unwrap();
        let c = Client { server: ServerState::new(config), ..Client::default() };
        replay(&c.server, config).unwrap();
        let _ = c.server.aof.set(Aof::open(config).unwrap().unwrap());
        c
    }

    #[test]
    fn replays_a_command() {
        let log = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let (dir, config) = existing_aof("aof-replay", log);
        let mut c = restart(&config);
        assert_eq!(c.cmd(&["GET", "foo"]), RespValue::bulk("bar"));
        assert_eq!(fs::read_to_string(dir.join("aofdir/random.aof.1.incr.aof")).unwrap(), log, "not logged twice");
    }

    #[test]
    fn replays_multiple_commands_in_order() {
        let log = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$2\r\n41\r\n\
                   *3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$5\r\nhello\r\n\
                   *2\r\n$4\r\nINCR\r\n$3\r\nfoo\r\n\
                   *3\r\n$5\r\nRPUSH\r\n$1\r\nl\r\n$1\r\na\r\n\
                   *3\r\n$3\r\nSET\r\n$3\r\nbaz";
        let (_dir, config) = existing_aof("aof-replay-many", log);
        let mut c = restart(&config);
        assert_eq!(c.cmd(&["GET", "foo"]), RespValue::bulk("42"));
        assert_eq!(c.cmd(&["GET", "bar"]), RespValue::bulk("hello"));
        assert_eq!(c.cmd(&["LRANGE", "l", "0", "-1"]), RespValue::bulk_array(["a"]));
        assert_eq!(c.cmd(&["GET", "baz"]), RespValue::NullBulk, "the incomplete command is ignored");
    }

    #[test]
    fn writes_survive_a_restart() {
        let dir = temp_dir("aof-restart");
        let config = aof_config(&dir);
        let mut c = restart(&config);
        c.cmd(&["SET", "counter", "1"]);
        c.cmd(&["INCR", "counter"]);
        c.cmd(&["RPUSH", "l", "x", "y"]);
        drop(c);

        let mut c = restart(&config);
        assert_eq!(c.cmd(&["GET", "counter"]), RespValue::bulk("2"));
        assert_eq!(c.cmd(&["LRANGE", "l", "0", "-1"]), RespValue::bulk_array(["x", "y"]));
        c.cmd(&["INCR", "counter"]);
        drop(c);
        assert_eq!(restart(&config).cmd(&["GET", "counter"]), RespValue::bulk("3"));
    }
}
