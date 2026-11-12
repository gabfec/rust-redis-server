//! Append-only file (AOF) persistence: with `--appendonly yes`, the server
//! keeps its AOF files in `<dir>/<appenddirname>`.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::config::Config;

/// `<dir>/<appenddirname>`
fn aof_dir(config: &Config) -> PathBuf {
    Path::new(&config.dir).join(&config.appenddirname)
}

/// Sets up the AOF directory at startup, if AOF is enabled.
pub fn init(config: &Config) -> io::Result<()> {
    if config.appendonly != "yes" {
        return Ok(());
    }
    fs::create_dir_all(aof_dir(config))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rdb::test_util::temp_dir;

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
    fn does_nothing_when_disabled() {
        let dir = temp_dir("aof-off");
        init(&Config { appendonly: "no".into(), ..aof_config(&dir) }).unwrap();
        assert!(!dir.join("aofdir").exists());
    }
}
