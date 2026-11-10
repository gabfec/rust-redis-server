use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// INFO [section]   (only the "replication" section exists so far)
pub struct Info {
    section: Option<String>,
}

impl Command for Info {
    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let section = args.next_opt().map(|s| s.to_ascii_lowercase());
        args.finish()?;
        Ok(Info { section })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let text = match self.section.as_deref() {
            None | Some("replication" | "all" | "everything" | "default") => {
                ctx.server.replication.info()
            }
            Some(_) => String::new(),
        };
        Ok(RespValue::bulk(text))
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::Client;
    use crate::config::Config;
    use crate::resp::RespValue;
    use crate::server::ServerState;

    fn info(c: &mut Client, args: &[&str]) -> String {
        let words: Vec<&str> = ["INFO"].iter().chain(args).copied().collect();
        match c.cmd(&words) {
            RespValue::Bulk(s) => s,
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn replication_section_reports_master() {
        let mut c = Client::default();
        assert!(info(&mut c, &["replication"]).contains("role:master"));
        assert!(info(&mut c, &["REPLICATION"]).contains("role:master"));
        assert!(info(&mut c, &[]).contains("role:master"));
        assert_eq!(info(&mut c, &["keyspace"]), "");
    }

    #[test]
    fn replication_id_and_offset() {
        let mut c = Client::default();
        let text = info(&mut c, &["replication"]);
        let replid = text
            .lines()
            .find_map(|l| l.strip_prefix("master_replid:"))
            .expect("master_replid line");
        assert_eq!(replid.len(), 40);
        assert!(text.lines().any(|l| l == "master_repl_offset:0"), "{text}");
    }

    #[test]
    fn replica_reports_slave() {
        let config = Config { replicaof: Some(("localhost".into(), 6379)), ..Config::default() };
        let mut c = Client { server: ServerState::new(&config), ..Client::default() };
        let text = info(&mut c, &["replication"]);
        assert!(text.contains("role:slave"), "{text}");
        assert!(!text.contains("role:master"));
    }
}
