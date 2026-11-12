//! Append-only file (AOF) persistence: with `--appendonly yes`, the server
//! keeps its AOF files in `<dir>/<appenddirname>`, along with a manifest
//! (`<appendfilename>.manifest`) listing them, one per line:
//!
//! ```text
//! file appendonly.aof.1.incr.aof seq 1 type i
//! ```

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
}
