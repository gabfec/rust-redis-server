//! Command-line options: `--port <port>`, `--replicaof "<host> <port>"`,
//! `--dir <dir>`, `--dbfilename <name>`, and the AOF options `--appendonly`,
//! `--appenddirname`, `--appendfilename`, `--appendfsync`.

#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    pub port: u16,
    /// Master to replicate from, if this server is a replica.
    pub replicaof: Option<(String, u16)>,
    /// Where data files (the RDB file) are stored. Defaults to the
    /// directory the server was started in.
    pub dir: String,
    /// Name of the RDB file in `dir`.
    pub dbfilename: String,
    /// "yes" to log every write to the append-only file (AOF).
    pub appendonly: String,
    /// Directory under `dir` for the AOF files and their manifest.
    pub appenddirname: String,
    /// Base name of the AOF files.
    pub appendfilename: String,
    /// When AOF writes are flushed to disk: always, everysec or no.
    pub appendfsync: String,
}

impl Default for Config {
    fn default() -> Self {
        let cwd = std::env::current_dir().map(|d| d.display().to_string()).unwrap_or_default();
        Config {
            port: 6379,
            replicaof: None,
            dir: cwd,
            dbfilename: "dump.rdb".into(),
            appendonly: "no".into(),
            appenddirname: "appendonlydir".into(),
            appendfilename: "appendonly.aof".into(),
            appendfsync: "everysec".into(),
        }
    }
}

impl Config {
    /// Parses the arguments after the program name.
    pub fn from_args(args: impl IntoIterator<Item = String>) -> Result<Config, String> {
        let mut config = Config::default();
        let mut args = args.into_iter();
        while let Some(flag) = args.next() {
            match flag.as_str() {
                "--port" => {
                    let port = value(&mut args, &flag)?;
                    config.port = port.parse().map_err(|_| format!("invalid port '{port}'"))?;
                }
                "--replicaof" => {
                    let master = value(&mut args, &flag)?;
                    config.replicaof = Some(parse_host_port(&master)?);
                }
                "--dir" => config.dir = value(&mut args, &flag)?,
                "--dbfilename" => config.dbfilename = value(&mut args, &flag)?,
                "--appendonly" => config.appendonly = value(&mut args, &flag)?,
                "--appenddirname" => config.appenddirname = value(&mut args, &flag)?,
                "--appendfilename" => config.appendfilename = value(&mut args, &flag)?,
                "--appendfsync" => config.appendfsync = value(&mut args, &flag)?,
                _ => return Err(format!("unknown option '{flag}'")),
            }
        }
        Ok(config)
    }

    /// A parameter's value, for CONFIG GET (`name` in lowercase).
    pub fn get(&self, name: &str) -> Option<String> {
        Some(match name {
            "port" => self.port.to_string(),
            "dir" => self.dir.clone(),
            "dbfilename" => self.dbfilename.clone(),
            "appendonly" => self.appendonly.clone(),
            "appenddirname" => self.appenddirname.clone(),
            "appendfilename" => self.appendfilename.clone(),
            "appendfsync" => self.appendfsync.clone(),
            _ => return None,
        })
    }
}

/// "<host> <port>", as one argument.
fn parse_host_port(s: &str) -> Result<(String, u16), String> {
    let invalid = || format!("invalid master '{s}', expected \"<host> <port>\"");
    let (host, port) = s.trim().split_once(' ').ok_or_else(invalid)?;
    let port = port.trim().parse().map_err(|_| invalid())?;
    Ok((host.to_string(), port))
}

fn value(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next().ok_or_else(|| format!("missing value for {flag}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Config, String> {
        Config::from_args(args.iter().map(|a| a.to_string()))
    }

    #[test]
    fn defaults_to_6379() {
        let config = parse(&[]).unwrap();
        assert_eq!(config.port, 6379);
        assert_eq!(config.replicaof, None);
        assert_eq!(config.dir, std::env::current_dir().unwrap().display().to_string());
        assert_eq!(config.dbfilename, "dump.rdb");
    }

    #[test]
    fn custom_port() {
        assert_eq!(parse(&["--port", "6380"]).unwrap().port, 6380);
    }

    #[test]
    fn replicaof() {
        let config = parse(&["--port", "6380", "--replicaof", "localhost 6379"]).unwrap();
        assert_eq!(config.replicaof, Some(("localhost".into(), 6379)));
        assert_eq!(config.port, 6380);
        assert!(parse(&["--replicaof", "localhost"]).is_err());
        assert!(parse(&["--replicaof", "localhost x"]).is_err());
    }

    #[test]
    fn bad_arguments() {
        assert_eq!(parse(&["--port"]), Err("missing value for --port".into()));
        assert_eq!(parse(&["--port", "70000"]), Err("invalid port '70000'".into()));
        assert_eq!(parse(&["--nope"]), Err("unknown option '--nope'".into()));
    }

    #[test]
    fn rdb_location() {
        let config = parse(&["--dir", "/tmp/redis-files", "--dbfilename", "dump.rdb"]).unwrap();
        assert_eq!(config.get("dir").as_deref(), Some("/tmp/redis-files"));
        assert_eq!(config.get("dbfilename").as_deref(), Some("dump.rdb"));
        assert_eq!(config.get("nope"), None);
    }

    #[test]
    fn aof_defaults() {
        let config = parse(&[]).unwrap();
        let get = |name| config.get(name).unwrap();
        assert_eq!(get("appendonly"), "no");
        assert_eq!(get("appenddirname"), "appendonlydir");
        assert_eq!(get("appendfilename"), "appendonly.aof");
        assert_eq!(get("appendfsync"), "everysec");
        assert_eq!(get("dir"), std::env::current_dir().unwrap().display().to_string());
    }

    #[test]
    fn aof_options_from_flags() {
        let config = parse(&[
            "--dir", "/tmp/data", "--appendonly", "yes", "--appenddirname", "aofdir",
            "--appendfilename", "log.aof", "--appendfsync", "always",
        ])
        .unwrap();
        let get = |name| config.get(name).unwrap();
        assert_eq!(get("dir"), "/tmp/data");
        assert_eq!(get("appendonly"), "yes");
        assert_eq!(get("appenddirname"), "aofdir");
        assert_eq!(get("appendfilename"), "log.aof");
        assert_eq!(get("appendfsync"), "always");
        // Flags left out keep their defaults.
        assert_eq!(parse(&["--appendonly", "yes"]).unwrap().get("appendfsync").unwrap(), "everysec");
    }
}
