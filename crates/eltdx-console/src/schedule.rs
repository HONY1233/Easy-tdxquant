//! Schedule parsing and due-time evaluation.
//!
//! Supported forms are intentionally small and explicit:
//! `manual` (no automation), `interval` (`30s`/`5m`/`2h`/`1d` or seconds) and
//! `daily` (`HH:MM` in Asia/Shanghai). Anything else is rejected instead of
//! silently degrading to a different cadence.

use crate::clock::{parse_iso_utc, shanghai_day_start};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Schedule {
    Manual,
    Interval { seconds: i64 },
    Daily { hour: u32, minute: u32 },
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ScheduleError {
    #[error("unknown schedule kind: {0}")]
    UnknownKind(String),
    #[error("interval needs 30s, 5m, 2h, 1d or plain seconds")]
    BadInterval,
    #[error("daily needs a HH:MM time in Asia/Shanghai")]
    BadDaily,
}

impl Schedule {
    pub fn parse(kind: &str, expr: Option<&str>) -> Result<Self, ScheduleError> {
        let kind = kind.trim();
        match kind {
            "" | "manual" => Ok(Self::Manual),
            "interval" => {
                let expr = expr.unwrap_or_default();
                parse_interval(expr)
                    .map(|seconds| Self::Interval { seconds })
                    .ok_or(ScheduleError::BadInterval)
            }
            "daily" => {
                let (hour, minute) =
                    parse_daily(expr.unwrap_or_default()).ok_or(ScheduleError::BadDaily)?;
                Ok(Self::Daily { hour, minute })
            }
            other => Err(ScheduleError::UnknownKind(other.to_owned())),
        }
    }
}

fn parse_interval(expr: &str) -> Option<i64> {
    let expr = expr.trim().to_ascii_lowercase();
    if expr.is_empty() {
        return None;
    }
    let (digits, multiplier) = match expr.chars().last()? {
        's' => (&expr[..expr.len() - 1], 1),
        'm' => (&expr[..expr.len() - 1], 60),
        'h' => (&expr[..expr.len() - 1], 3_600),
        'd' => (&expr[..expr.len() - 1], 86_400),
        _ => (expr.as_str(), 1),
    };
    let value: i64 = digits.trim().parse().ok()?;
    if value <= 0 || value > 1_000_000 {
        return None;
    }
    Some(value * multiplier)
}

fn parse_daily(expr: &str) -> Option<(u32, u32)> {
    let (hour, minute) = expr.trim().split_once(':')?;
    let hour: u32 = hour.trim().parse().ok()?;
    let minute: u32 = minute.trim().parse().ok()?;
    if hour > 23 || minute > 59 {
        return None;
    }
    Some((hour, minute))
}

/// Whether a scheduled task should start now.
///
/// An unparseable stored timestamp suppresses the run rather than risking a
/// tight restart loop; a manual trigger still works in that case.
pub fn is_due(schedule: &Schedule, last_started_iso: Option<&str>, now: i64) -> bool {
    match schedule {
        Schedule::Manual => false,
        Schedule::Interval { seconds } => match last_started_iso {
            None => true,
            Some(value) => match parse_iso_utc(value) {
                Some(last) => now.saturating_sub(last) >= *seconds,
                None => false,
            },
        },
        Schedule::Daily { hour, minute } => {
            let due_at =
                shanghai_day_start(now) + i64::from(*hour) * 3600 + i64::from(*minute) * 60;
            if now < due_at {
                return false;
            }
            match last_started_iso {
                None => true,
                Some(value) => match parse_iso_utc(value) {
                    Some(last) => last < due_at,
                    None => false,
                },
            }
        }
    }
}

/// Next planned run as an epoch second, for display.
pub fn next_due(schedule: &Schedule, last_started_iso: Option<&str>, now: i64) -> Option<i64> {
    match schedule {
        Schedule::Manual => None,
        Schedule::Interval { seconds } => {
            let last = last_started_iso.and_then(parse_iso_utc);
            Some(match last {
                Some(last) => (last + seconds).max(now),
                None => now,
            })
        }
        Schedule::Daily { hour, minute } => {
            let due_at =
                shanghai_day_start(now) + i64::from(*hour) * 3600 + i64::from(*minute) * 60;
            if now < due_at {
                Some(due_at)
            } else {
                Some(due_at + 86_400)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn epoch(value: &str) -> i64 {
        match parse_iso_utc(value) {
            Some(epoch) => epoch,
            None => panic!("parse {value}"),
        }
    }

    #[test]
    fn manual_and_unknown_kinds_are_distinct() {
        assert_eq!(Schedule::parse("manual", None), Ok(Schedule::Manual));
        assert_eq!(Schedule::parse("", None), Ok(Schedule::Manual));
        assert_eq!(
            Schedule::parse("weekly", None),
            Err(ScheduleError::UnknownKind("weekly".to_owned()))
        );
    }

    #[test]
    fn intervals_accept_units_and_seconds() {
        assert_eq!(
            Schedule::parse("interval", Some("30s")),
            Ok(Schedule::Interval { seconds: 30 })
        );
        assert_eq!(
            Schedule::parse("interval", Some("5m")),
            Ok(Schedule::Interval { seconds: 300 })
        );
        assert_eq!(
            Schedule::parse("interval", Some("2H")),
            Ok(Schedule::Interval { seconds: 7_200 })
        );
        assert_eq!(
            Schedule::parse("interval", Some("600")),
            Ok(Schedule::Interval { seconds: 600 })
        );
        assert_eq!(
            Schedule::parse("interval", Some("0s")),
            Err(ScheduleError::BadInterval)
        );
        assert_eq!(
            Schedule::parse("interval", Some("soon")),
            Err(ScheduleError::BadInterval)
        );
    }

    #[test]
    fn daily_accepts_hh_mm_only() {
        assert_eq!(
            Schedule::parse("daily", Some("09:05")),
            Ok(Schedule::Daily { hour: 9, minute: 5 })
        );
        assert_eq!(
            Schedule::parse("daily", Some("24:00")),
            Err(ScheduleError::BadDaily)
        );
        assert_eq!(
            Schedule::parse("daily", Some("9")),
            Err(ScheduleError::BadDaily)
        );
    }

    #[test]
    fn interval_due_follows_the_last_run() {
        let schedule = Schedule::Interval { seconds: 300 };
        let now = epoch("2026-11-28T12:00:00Z");
        assert!(is_due(&schedule, None, now));
        assert!(is_due(&schedule, Some("2026-11-28T11:54:00Z"), now));
        assert!(!is_due(&schedule, Some("2026-11-28T11:57:00Z"), now));
        assert!(!is_due(&schedule, Some("garbage"), now));
    }

    #[test]
    fn daily_due_uses_shanghai_midnight() {
        let schedule = Schedule::Daily { hour: 9, minute: 0 };
        // 2026-11-28T00:30:00Z == 08:30 Shanghai, before 09:00.
        let early = epoch("2026-11-28T00:30:00Z");
        assert!(!is_due(&schedule, None, early));
        // 2026-11-28T01:30:00Z == 09:30 Shanghai, after 09:00.
        let late = epoch("2026-11-28T01:30:00Z");
        assert!(is_due(&schedule, None, late));
        assert!(!is_due(&schedule, Some("2026-11-28T01:10:00Z"), late));
        assert!(is_due(&schedule, Some("2026-11-27T01:10:00Z"), late));
        assert_eq!(
            next_due(&schedule, None, late),
            Some(epoch("2026-11-29T01:00:00Z"))
        );
    }

    #[test]
    fn manual_never_becomes_due() {
        assert!(!is_due(&Schedule::Manual, None, 1_800_000_000));
        assert_eq!(next_due(&Schedule::Manual, None, 1_800_000_000), None);
    }
}
