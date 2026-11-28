//! Minimal civil-time helpers.
//!
//! The console deliberately avoids a date-time crate: everything it needs is
//! UTC epoch seconds, a Shanghai (UTC+8) date partition and ISO-8601 strings in
//! the exact shape SQLite writes (`YYYY-MM-DDTHH:MM:SS.sssZ`).

use std::time::{SystemTime, UNIX_EPOCH};

/// Seconds since the Unix epoch; `0` if the system clock predates 1970.
pub fn now_epoch() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_secs() as i64)
        .unwrap_or(0)
}

/// Howard Hinnant's days-to-civil algorithm (proleptic Gregorian).
pub fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };
    (year, m as u32, d as u32)
}

/// Inverse of [`civil_from_days`].
pub fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let m = i64::from(month);
    let d = i64::from(day);
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// `YYYY-MM-DD` in Shanghai time for the given epoch second.
pub fn shanghai_date(epoch: i64) -> String {
    let days = (epoch + 8 * 3600).div_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    format!("{year:04}-{month:02}-{day:02}")
}

/// Seconds since Shanghai midnight for the given epoch second.
pub fn shanghai_seconds_of_day(epoch: i64) -> i64 {
    (epoch + 8 * 3600).rem_euclid(86_400)
}

/// Start of the current Shanghai day, as an epoch second.
pub fn shanghai_day_start(epoch: i64) -> i64 {
    epoch - shanghai_seconds_of_day(epoch)
}

/// `YYYY-MM-DDTHH:MM:SS.mmmZ` in UTC, matching SQLite's `strftime` output.
pub fn format_iso_utc(epoch: i64) -> String {
    let days = epoch.div_euclid(86_400);
    let rem = epoch.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.000Z",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}

/// Parses the ISO-8601 timestamps stored by the store. Fractional seconds and
/// a trailing `Z` are optional; anything else returns `None`.
pub fn parse_iso_utc(value: &str) -> Option<i64> {
    let value = value.trim();
    let (date, time) = value.split_once('T')?;
    let mut date_parts = date.split('-');
    let year: i64 = date_parts.next()?.parse().ok()?;
    let month: u32 = date_parts.next()?.parse().ok()?;
    let day: u32 = date_parts.next()?.parse().ok()?;
    if date_parts.next().is_some() || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let time = time.trim_end_matches('Z');
    let time = time.split('.').next().unwrap_or(time);
    let mut time_parts = time.split(':');
    let hour: i64 = time_parts.next()?.parse().ok()?;
    let minute: i64 = time_parts.next()?.parse().ok()?;
    let second: i64 = time_parts.next()?.parse().ok()?;
    if time_parts.next().is_some() || hour > 23 || minute > 59 || second > 60 {
        return None;
    }
    let days = days_from_civil(year, month, day);
    Some(days * 86_400 + hour * 3600 + minute * 60 + second)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn civil_conversions_round_trip() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(20_000), (2024, 10, 4));
        for days in [-1, 0, 1, 11_000, 20_000, 25_000] {
            let (y, m, d) = civil_from_days(days);
            assert_eq!(days_from_civil(y, m, d), days);
        }
    }

    #[test]
    fn iso_timestamps_parse_and_format() {
        let epoch = 1_795_000_000;
        let text = format_iso_utc(epoch);
        assert!(text.ends_with('Z'));
        assert_eq!(parse_iso_utc(&text), Some(epoch));
        assert_eq!(
            parse_iso_utc("2026-11-28T12:47:59.743Z"),
            parse_iso_utc("2026-11-28T12:47:59Z")
        );
        assert_eq!(parse_iso_utc("not-a-date"), None);
        assert_eq!(parse_iso_utc("2026-13-01T00:00:00Z"), None);
    }

    #[test]
    fn shanghai_helpers_shift_by_eight_hours() {
        // 2026-11-28T00:30:00Z == 2026-11-28 08:30 Shanghai.
        let epoch = match parse_iso_utc("2026-11-28T00:30:00Z") {
            Some(value) => value,
            None => panic!("parse"),
        };
        assert_eq!(shanghai_date(epoch), "2026-11-28");
        assert_eq!(shanghai_seconds_of_day(epoch), 8 * 3600 + 1800);
        assert_eq!(shanghai_day_start(epoch), epoch - 8 * 3600 - 1800);

        // 2026-11-27T16:30:00Z == 2026-11-28 00:30 Shanghai (next day).
        let late = epoch - 8 * 3600;
        assert_eq!(shanghai_date(late), "2026-11-28");
        assert_eq!(shanghai_seconds_of_day(late), 1800);
    }
}
