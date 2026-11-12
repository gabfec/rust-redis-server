use super::parse_unit;
use crate::commands::{Args, CmdError, Command, Ctx};
use crate::geo::Point;
use crate::resp::RespValue;

/// GEODIST key member1 member2 [M | KM | FT | MI]
/// The distance between two members (in meters by default), or nil if
/// either doesn't exist.
pub struct Geodist {
    key: String,
    members: [String; 2],
    meters_per_unit: f64,
}

impl Command for Geodist {
    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let key = args.next_string()?;
        let members = [args.next_string()?, args.next_string()?];
        let meters_per_unit = match args.next_opt() {
            Some(unit) => parse_unit(&unit)?,
            None => 1.0,
        };
        if args.next_opt().is_some() {
            return Err(CmdError::Syntax);
        }
        Ok(Geodist { key, members, meters_per_unit })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let mut store = ctx.store();
        let Some(set) = store.get_zset(&self.key)? else {
            return Ok(RespValue::NullBulk);
        };
        let [Some(a), Some(b)] = self.members.each_ref().map(|member| set.score(member)) else {
            return Ok(RespValue::NullBulk);
        };
        let meters = Point::from_score(a).distance(Point::from_score(b));
        Ok(RespValue::bulk(format!("{:.4}", meters / self.meters_per_unit)))
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::{err, Client};
    use crate::resp::RespValue;

    fn places() -> Client {
        let mut c = Client::default();
        c.cmd(&["GEOADD", "places", "11.5030378", "48.164271", "Munich"]);
        c.cmd(&["GEOADD", "places", "2.2944692", "48.8584625", "Paris"]);
        c
    }

    #[test]
    fn distances() {
        let mut c = places();
        assert_eq!(c.cmd(&["GEODIST", "places", "Munich", "Paris"]), RespValue::bulk("682477.7582"));
        assert_eq!(c.cmd(&["GEODIST", "places", "Paris", "Munich"]), RespValue::bulk("682477.7582"));
        assert_eq!(c.cmd(&["GEODIST", "places", "Paris", "Paris"]), RespValue::bulk("0.0000"));
        assert_eq!(c.cmd(&["GEODIST", "places", "Munich", "Paris", "KM"]), RespValue::bulk("682.4778"));
    }

    #[test]
    fn missing_members_give_nil() {
        let mut c = places();
        assert_eq!(c.cmd(&["GEODIST", "places", "Munich", "Nowhere"]), RespValue::NullBulk);
        assert_eq!(c.cmd(&["GEODIST", "missing", "Munich", "Paris"]), RespValue::NullBulk);
    }

    #[test]
    fn bad_arguments() {
        let mut c = places();
        assert_eq!(
            c.cmd(&["GEODIST", "places", "Munich", "Paris", "yards"]),
            err("ERR unsupported unit provided. please use M, KM, FT, MI")
        );
        assert_eq!(c.cmd(&["GEODIST", "places", "Munich"]), err("ERR wrong number of arguments for 'geodist' command"));
    }
}
