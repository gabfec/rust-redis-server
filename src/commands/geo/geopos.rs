use crate::commands::{Args, CmdError, Command, Ctx};
use crate::geo::Point;
use crate::resp::RespValue;

/// GEOPOS key member [member ...]
/// The [longitude, latitude] of each member, or a null array for a member
/// (or key) that doesn't exist.
pub struct Geopos {
    key: String,
    members: Vec<String>,
}

impl Command for Geopos {
    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let key = args.next_string()?;
        let members = args.rest_min(1)?;
        Ok(Geopos { key, members })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let mut store = ctx.store();
        let set = store.get_zset(&self.key)?;
        let positions = self
            .members
            .iter()
            .map(|member| match set.and_then(|set| set.score(member)) {
                Some(score) => {
                    let point = Point::from_score(score);
                    RespValue::bulk_array([point.longitude.to_string(), point.latitude.to_string()])
                }
                None => RespValue::NullArray,
            })
            .collect();
        Ok(RespValue::Array(positions))
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::test_util::{err, Client};
    use crate::resp::RespValue;

    /// The [longitude, latitude] of a GEOPOS entry.
    fn coordinates(entry: &RespValue) -> [f64; 2] {
        let RespValue::Array(pair) = entry else { panic!("not a position: {entry:?}") };
        let number = |value: &RespValue| match value {
            RespValue::Bulk(s) => s.parse::<f64>().unwrap(),
            other => panic!("not a bulk string: {other:?}"),
        };
        [number(&pair[0]), number(&pair[1])]
    }

    #[test]
    fn positions() {
        let mut c = Client::default();
        c.cmd(&["GEOADD", "location_key", "-0.0884948", "51.506479", "London"]);
        c.cmd(&["GEOADD", "location_key", "11.5030378", "48.164271", "Munich"]);
        let RespValue::Array(entries) = c.cmd(&["GEOPOS", "location_key", "London", "missing", "Munich"]) else {
            panic!("not an array");
        };
        assert_eq!(entries.len(), 3);
        let [lon, lat] = coordinates(&entries[0]);
        assert!((lon - -0.0884948).abs() < 1e-6 && (lat - 51.506479).abs() < 1e-6, "{lon},{lat}");
        assert_eq!(entries[1], RespValue::NullArray);
        let [lon, lat] = coordinates(&entries[2]);
        assert!((lon - 11.5030378).abs() < 1e-6 && (lat - 48.164271).abs() < 1e-6, "{lon},{lat}");
    }

    #[test]
    fn missing_key() {
        let mut c = Client::default();
        assert_eq!(
            c.cmd(&["GEOPOS", "missing_key", "London", "Munich"]),
            RespValue::Array(vec![RespValue::NullArray, RespValue::NullArray])
        );
        c.cmd(&["SET", "s", "x"]);
        assert_eq!(
            c.cmd(&["GEOPOS", "s", "m"]),
            err("WRONGTYPE Operation against a key holding the wrong kind of value")
        );
    }
}
