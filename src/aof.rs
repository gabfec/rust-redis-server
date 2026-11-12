//! Append-only file (AOF) persistence: with `--appendonly yes`, the server
//! keeps its AOF files in `<dir>/<appenddirname>`.

use std::fs::{self, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

use crate::config::Config;

/// `<dir>/<appenddirname>`
fn aof_dir(config: &Config) -> PathBuf {
    Path::new(&config.dir).join(&config.appenddirname)
}

/// The first incremental AOF file: `<appendfilename>.1.incr.aof`.
fn first_incr_file(config: &Config) -> String {
    format!("{}.1.incr.aof", config.appendfilename)
}

/// Sets up the AOF directory and file at startup, if AOF is enabled.
pub fn init(config: &Config) -> io::Result<()> {
    if config.appendonly != "yes" {
        return Ok(());
    }
    let dir = aof_dir(config);
    fs::create_dir_all(&dir)?;
    // Created empty if missing; never truncated.
    OpenOptions::new().create(true).append(true).open(dir.join(first_incr_file(config)))?;
    Ok(())
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
    fn creates_an_empty_aof_file() {
        let dir = temp_dir("aof-file");
        init(&aof_config(&dir)).unwrap();
        let file = dir.join("aofdir/log.aof.1.incr.aof");
        assert_eq!(fs::read(&file).unwrap(), b"");

        fs::write(&file, b"existing").unwrap();
        init(&aof_config(&dir)).unwrap();
        assert_eq!(fs::read(&file).unwrap(), b"existing", "not truncated on restart");
    }

    #[test]
    fn does_nothing_when_disabled() {
        let dir = temp_dir("aof-off");
        init(&Config { appendonly: "no".into(), ..aof_config(&dir) }).unwrap();
        assert!(!dir.join("aofdir").exists());
    }
}
