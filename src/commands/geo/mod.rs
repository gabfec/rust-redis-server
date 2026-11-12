pub mod geoadd;
pub mod geodist;
pub mod geopos;
pub mod geosearch;

use crate::commands::CmdError;
use crate::geo::{Point, MAX_LATITUDE, MAX_LONGITUDE, MIN_LATITUDE, MIN_LONGITUDE};

/// Meters per distance unit (m, km, mi, ft), case-insensitive.
fn parse_unit(unit: &str) -> Result<f64, CmdError> {
    match unit.to_ascii_lowercase().as_str() {
        "m" => Ok(1.0),
        "km" => Ok(1000.0),
        "mi" => Ok(1609.34),
        "ft" => Ok(0.3048),
        _ => Err(CmdError::Other("ERR unsupported unit provided. please use M, KM, FT, MI".into())),
    }
}

/// Parses and validates a longitude, latitude pair.
fn parse_point(longitude: &str, latitude: &str) -> Result<Point, CmdError> {
    let parse = |s: &str| {
        s.parse::<f64>().map_err(|_| CmdError::Other("ERR value is not a valid float".into()))
    };
    let (longitude, latitude) = (parse(longitude)?, parse(latitude)?);
    if !(MIN_LONGITUDE..=MAX_LONGITUDE).contains(&longitude)
        || !(MIN_LATITUDE..=MAX_LATITUDE).contains(&latitude)
    {
        return Err(CmdError::Other(format!(
            "ERR invalid longitude,latitude pair {longitude:.6},{latitude:.6}"
        )));
    }
    Ok(Point { longitude, latitude })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits_are_inclusive() {
        for (lon, lat) in [("-180", "-85.05112878"), ("180", "85.05112878"), ("0", "0")] {
            assert!(parse_point(lon, lat).is_ok(), "{lon},{lat}");
        }
        for (lon, lat) in [("180.0001", "0"), ("-181", "0"), ("0", "85.06"), ("0", "-90"), ("nan", "0")] {
            assert!(parse_point(lon, lat).is_err(), "{lon},{lat}");
        }
    }
}
