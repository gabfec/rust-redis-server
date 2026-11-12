//! Locations on Earth and the scores Redis stores them as: a 52-bit
//! geohash, the longitude and latitude bits interleaved.

pub const MIN_LONGITUDE: f64 = -180.0;
pub const MAX_LONGITUDE: f64 = 180.0;
/// Latitudes stop short of the poles because of the Web Mercator
/// projection Redis uses.
pub const MIN_LATITUDE: f64 = -85.05112878;
pub const MAX_LATITUDE: f64 = 85.05112878;

const LONGITUDE_RANGE: f64 = MAX_LONGITUDE - MIN_LONGITUDE;
const LATITUDE_RANGE: f64 = MAX_LATITUDE - MIN_LATITUDE;
/// The value Redis uses, in meters.
const EARTH_RADIUS: f64 = 6372797.560856;
/// Each coordinate is a 26-bit cell number.
const CELLS: f64 = (1u64 << 26) as f64;

/// A location, in degrees.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Point {
    pub longitude: f64,
    pub latitude: f64,
}

impl Point {
    /// The sorted set score of this location.
    pub fn score(self) -> f64 {
        let latitude = (CELLS * (self.latitude - MIN_LATITUDE) / LATITUDE_RANGE) as u64;
        let longitude = (CELLS * (self.longitude - MIN_LONGITUDE) / LONGITUDE_RANGE) as u64;
        // Fits in 52 bits, so the f64 is exact.
        (spread(latitude) | (spread(longitude) << 1)) as f64
    }

    /// Great-circle distance in meters (the haversine formula, as in
    /// Redis).
    pub fn distance(self, other: Point) -> f64 {
        let (lat1, lat2) = (self.latitude.to_radians(), other.latitude.to_radians());
        let u = ((lat2 - lat1) / 2.0).sin();
        let v = ((other.longitude - self.longitude).to_radians() / 2.0).sin();
        2.0 * EARTH_RADIUS * (u * u + lat1.cos() * lat2.cos() * v * v).sqrt().asin()
    }

    /// The location a score stands for: the centre of its geohash cell.
    pub fn from_score(score: f64) -> Point {
        let hash = score as u64;
        let (latitude, longitude) = (compact(hash) as f64, compact(hash >> 1) as f64);
        let centre = |cell: f64, min: f64, range: f64| min + range * (cell + 0.5) / CELLS;
        Point {
            longitude: centre(longitude, MIN_LONGITUDE, LONGITUDE_RANGE),
            latitude: centre(latitude, MIN_LATITUDE, LATITUDE_RANGE),
        }
    }
}

/// Moves bit i of `v` (32 bits) to bit 2i.
fn spread(v: u64) -> u64 {
    let mut v = v & 0xFFFF_FFFF;
    v = (v | (v << 16)) & 0x0000_FFFF_0000_FFFF;
    v = (v | (v << 8)) & 0x00FF_00FF_00FF_00FF;
    v = (v | (v << 4)) & 0x0F0F_0F0F_0F0F_0F0F;
    v = (v | (v << 2)) & 0x3333_3333_3333_3333;
    (v | (v << 1)) & 0x5555_5555_5555_5555
}

/// The reverse of [`spread`]: moves bit 2i of `v` to bit i.
fn compact(v: u64) -> u64 {
    let mut v = v & 0x5555_5555_5555_5555;
    v = (v | (v >> 1)) & 0x3333_3333_3333_3333;
    v = (v | (v >> 2)) & 0x0F0F_0F0F_0F0F_0F0F;
    v = (v | (v >> 4)) & 0x00FF_00FF_00FF_00FF;
    v = (v | (v >> 8)) & 0x0000_FFFF_0000_FFFF;
    (v | (v >> 16)) & 0x0000_0000_FFFF_FFFF
}

#[cfg(test)]
mod tests {
    use super::*;

    /// (name, longitude, latitude, score), from CodeCrafters'
    /// redis-geocoding-algorithm repository.
    const CITIES: [(&str, f64, f64, u64); 12] = [
        ("Bangkok", 100.5252, 13.7220, 3962257306574459),
        ("Beijing", 116.3972, 39.9075, 4069885364908765),
        ("Berlin", 13.4105, 52.5244, 3673983964876493),
        ("Copenhagen", 12.5655, 55.6759, 3685973395504349),
        ("New Delhi", 77.2167, 28.6667, 3631527070936756),
        ("Kathmandu", 85.3206, 27.7017, 3639507404773204),
        ("London", -0.1278, 51.5074, 2163557714755072),
        ("New York", -74.0060, 40.7128, 1791873974549446),
        ("Paris", 2.3488, 48.8534, 3663832752681684),
        ("Sydney", 151.2093, -33.8688, 3252046221964352),
        ("Tokyo", 139.6917, 35.6895, 4171231230197045),
        ("Vienna", 16.3707, 48.2064, 3673109836391743),
    ];

    #[test]
    fn scores() {
        for (name, longitude, latitude, score) in CITIES {
            assert_eq!(Point { longitude, latitude }.score(), score as f64, "{name}");
        }
    }

    #[test]
    fn decodes_scores_to_within_a_cell() {
        for (name, longitude, latitude, score) in CITIES {
            let point = Point::from_score(score as f64);
            assert!((point.longitude - longitude).abs() < 1e-5, "{name}: {point:?}");
            assert!((point.latitude - latitude).abs() < 1e-5, "{name}: {point:?}");
        }
        // The decoded location is the centre of the cell, from the
        // reference implementation.
        let paris = Point::from_score(3663832752681684.0);
        assert!((paris.longitude - 2.348802387714386).abs() < 1e-12);
        assert!((paris.latitude - 48.85340071224621).abs() < 1e-12);
    }
}
