//! Command-line options: `--port <port>` (more to come, e.g. `--replicaof`).

#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    pub port: u16,
}

impl Default for Config {
    fn default() -> Self {
        Config { port: 6379 }
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
                _ => return Err(format!("unknown option '{flag}'")),
            }
        }
        Ok(config)
    }
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
        assert_eq!(parse(&[]), Ok(Config { port: 6379 }));
    }

    #[test]
    fn custom_port() {
        assert_eq!(parse(&["--port", "6380"]), Ok(Config { port: 6380 }));
    }

    #[test]
    fn bad_arguments() {
        assert_eq!(parse(&["--port"]), Err("missing value for --port".into()));
        assert_eq!(parse(&["--port", "70000"]), Err("invalid port '70000'".into()));
        assert_eq!(parse(&["--nope"]), Err("unknown option '--nope'".into()));
    }
}
