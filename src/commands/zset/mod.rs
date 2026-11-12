pub mod zadd;
pub mod zcard;
pub mod zrange;
pub mod zrank;

use crate::commands::CmdError;

/// A score argument: a float (also "inf", "-inf"), never NaN.
fn parse_score(s: &str) -> Result<f64, CmdError> {
    match s.parse::<f64>() {
        Ok(score) if !score.is_nan() => Ok(score),
        _ => Err(CmdError::Other("ERR value is not a valid float".into())),
    }
}
