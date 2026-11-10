//! Command-line options: `--port <port>`, `--replicaof "<host> <port>"`.

#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    pub port: u16,
    /// Master to replicate from, if this server is a replica.
    pub replicaof: Option<(String, u16)>,
}

impl Default for Config {
    fn default() -> Self {
        Config { port: 6379, replicaof: None }
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
                _ => return Err(format!("unknown option '{flag}'")),
            }
        }
        Ok(config)
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
        assert_eq!(parse(&[]), Ok(Config { port: 6379, replicaof: None }));
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
}
