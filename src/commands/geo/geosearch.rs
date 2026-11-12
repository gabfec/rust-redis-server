use super::{parse_point, parse_unit};
use crate::commands::{Args, CmdError, Command, Ctx};
use crate::geo::Point;
use crate::resp::RespValue;

/// GEOSEARCH key FROMLONLAT longitude latitude BYRADIUS radius M|KM|FT|MI
/// The members within `radius` of the point, nearest first.
pub struct Geosearch {
    key: String,
    centre: Point,
    radius_meters: f64,
}

impl Command for Geosearch {
    fn parse(mut args: Args) -> Result<Self, CmdError> {
        let key = args.next_string()?;
        let (mut centre, mut radius_meters) = (None, None);
        while let Some(option) = args.next_opt() {
            match option.to_ascii_uppercase().as_str() {
                "FROMLONLAT" => {
                    let (longitude, latitude) = (args.next_string()?, args.next_string()?);
                    centre = Some(parse_point(&longitude, &latitude)?);
                }
                "BYRADIUS" => {
                    let radius: f64 = args
                        .next_string()?
                        .parse()
                        .ok()
                        .filter(|radius: &f64| *radius >= 0.0)
                        .ok_or_else(|| CmdError::Other("ERR radius cannot be negative".into()))?;
                    radius_meters = Some(radius * parse_unit(&args.next_string()?)?);
                }
                _ => return Err(CmdError::Syntax),
            }
        }
        let centre = centre.ok_or_else(|| {
            CmdError::Other("ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for geosearch".into())
        })?;
        let radius_meters = radius_meters.ok_or_else(|| {
            CmdError::Other("ERR exactly one of BYRADIUS and BYBOX can be specified for geosearch".into())
        })?;
        Ok(Geosearch { key, centre, radius_meters })
    }

    fn execute(self, ctx: &mut Ctx) -> Result<RespValue, CmdError> {
        let mut store = ctx.store();
        let Some(set) = store.get_zset(&self.key)? else {
            return Ok(RespValue::Array(vec![]));
        };
        let mut found: Vec<(f64, &str)> = set
            .iter()
            .map(|(member, score)| (self.centre.distance(Point::from_score(score)), member))
            .filter(|(distance, _)| *distance <= self.radius_meters)
            .collect();
        found.sort_by(|a, b| a.0.total_cmp(&b.0));
        Ok(RespValue::bulk_array(found.into_iter().map(|(_, member)| member)))
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
        c.cmd(&["GEOADD", "places", "-0.0884948", "51.506479", "London"]);
        c
    }

    #[test]
    fn finds_members_within_the_radius() {
        let mut c = places();
        let search = |c: &mut Client, lon: &str, lat: &str, radius: &str, unit: &str| {
            c.cmd(&["GEOSEARCH", "places", "FROMLONLAT", lon, lat, "BYRADIUS", radius, unit])
        };
        assert_eq!(search(&mut c, "2", "48", "100000", "m"), RespValue::bulk_array(["Paris"]));
        assert_eq!(search(&mut c, "2", "48", "500000", "m"), RespValue::bulk_array(["Paris", "London"]));
        assert_eq!(search(&mut c, "11", "50", "300000", "m"), RespValue::bulk_array(["Munich"]));
        assert_eq!(search(&mut c, "2", "48", "500", "km"), RespValue::bulk_array(["Paris", "London"]));
        assert_eq!(search(&mut c, "100", "0", "1", "m"), RespValue::bulk_array(Vec::<String>::new()));
        assert_eq!(
            c.cmd(&["GEOSEARCH", "missing", "FROMLONLAT", "2", "48", "BYRADIUS", "1", "m"]),
            RespValue::Array(vec![])
        );
    }

    #[test]
    fn bad_arguments() {
        let mut c = places();
        assert_eq!(
            c.cmd(&["GEOSEARCH", "places", "BYRADIUS", "1", "m"]),
            err("ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for geosearch")
        );
        assert_eq!(
            c.cmd(&["GEOSEARCH", "places", "FROMLONLAT", "2", "48", "BYRADIUS", "1", "parsec"]),
            err("ERR unsupported unit provided. please use M, KM, FT, MI")
        );
        assert_eq!(
            c.cmd(&["GEOSEARCH", "places", "FROMLONLAT", "2", "48", "BYRADIUS", "1", "m", "WITHDIST"]),
            err("ERR syntax error")
        );
    }
}
