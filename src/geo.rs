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
}
