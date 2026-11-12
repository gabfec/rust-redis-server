use super::{parse_point, Point};
use crate::commands::{Args, CmdError, Command, Ctx};
use crate::resp::RespValue;

/// GEOADD key longitude latitude member [longitude latitude member ...]
/// Adds locations to the sorted set at `key`. Returns how many members
/// were added (not counting updated ones).
pub struct Geoadd {
    key: String,
    locations: Vec<Location>,
}

struct Location {
    #[allow(dead_code)] // used for the score in a later stage
    point: Point,
    member: String,
}

impl Command for Geoadd {
    const IS_WRITE: bool = true;

    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let key = args.next_string()?;
        let wrong_args = args.wrong_args();
        let rest = args.rest_min(3)?;
        if rest.len() % 3 != 0 {
            return Err(wrong_args);
        }
        let locations = rest
            .chunks(3)
            .map(|triple| Ok(Location { point: parse_point(&triple[0], &triple[1])?, member: triple[2].clone() }))
            .collect::<Result<_, CmdError>>()?;
        Ok(Geoadd { key, locations })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let mut store = ctx.store();
        let set = store.zset_or_create(self.key)?;
        let added = self.locations.into_iter().filter(|location| set.insert(location.member.clone(), 0.0)).count();
        Ok(RespValue::Integer(added as i64))
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::{err, Client};
    use crate::resp::RespValue;

    #[test]
    fn returns_the_number_of_added_locations() {
        let mut c = Client::default();
        assert_eq!(c.cmd(&["GEOADD", "places", "11.5030378", "48.164271", "Munich"]), RespValue::Integer(1));
        assert_eq!(c.cmd(&["GEOADD", "places", "11.5030378", "48.164271", "Munich"]), RespValue::Integer(0));
        assert_eq!(
            c.cmd(&["GEOADD", "places", "2.2944692", "48.8584625", "Paris", "-0.0884948", "51.506479", "London"]),
            RespValue::Integer(2)
        );
    }

    #[test]
    fn bad_arguments() {
        let mut c = Client::default();
        assert_eq!(
            c.cmd(&["GEOADD", "places", "1", "2"]),
            err("ERR wrong number of arguments for 'geoadd' command")
        );
        assert_eq!(c.cmd(&["GEOADD", "places", "x", "2", "m"]), err("ERR value is not a valid float"));
        assert_eq!(
            c.cmd(&["GEOADD", "places", "180", "90", "test1"]),
            err("ERR invalid longitude,latitude pair 180.000000,90.000000")
        );
        assert_eq!(
            c.cmd(&["GEOADD", "places", "181", "0.3", "test2"]),
            err("ERR invalid longitude,latitude pair 181.000000,0.300000")
        );
        // One invalid location means none is added.
        assert_eq!(
            c.cmd(&["GEOADD", "places", "1", "2", "ok", "200", "100", "foo"]),
            err("ERR invalid longitude,latitude pair 200.000000,100.000000")
        );
        assert_eq!(c.cmd(&["TYPE", "places"]), RespValue::Simple("none".into()));
        c.cmd(&["SET", "s", "x"]);
        assert_eq!(
            c.cmd(&["GEOADD", "s", "1", "2", "m"]),
            err("WRONGTYPE Operation against a key holding the wrong kind of value")
        );
    }
}
