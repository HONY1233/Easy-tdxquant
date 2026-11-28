//! Session tokens, cookies and login throttling for the local console.
//!
//! The plaintext token only ever exists in the client cookie; the store keeps
//! the hex SHA-256 digest so a database leak cannot be replayed as a session.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use rand::RngCore;
use sha2::{Digest, Sha256};

/// Name of the session cookie. The `Secure` attribute is optional because the
/// console intentionally binds to loopback over plain HTTP.
pub const SESSION_COOKIE: &str = "eltdx_console_session";
pub const MIN_PASSWORD_LEN: usize = 12;
pub const MAX_PASSWORD_LEN: usize = 256;
pub const MAX_USERNAME_LEN: usize = 64;
/// Default session lifetime and idle window: 12 hours.
pub const DEFAULT_SESSION_TTL_MINUTES: i64 = 12 * 60;
const TOKEN_BYTES: usize = 32;

/// Generates a 256-bit random session token rendered as lowercase hex.
pub fn new_token() -> String {
    let mut bytes = [0_u8; TOKEN_BYTES];
    rand::rng().fill_bytes(&mut bytes);
    hex(&bytes)
}

/// Hex SHA-256 of a session token, used as the primary key in `admin_sessions`.
pub fn hash_token(token: &str) -> String {
    hex(&Sha256::digest(token.as_bytes()))
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

pub fn validate_username(username: &str) -> Result<(), &'static str> {
    let trimmed = username.trim();
    if trimmed.is_empty() {
        return Err("username must not be empty");
    }
    if trimmed.len() > MAX_USERNAME_LEN {
        return Err("username is too long");
    }
    if trimmed.chars().any(char::is_control) {
        return Err("username must not contain control characters");
    }
    Ok(())
}

/// Rejects weak or oversized passwords before they reach argon2.
pub fn validate_password(password: &str) -> Result<(), &'static str> {
    if password.chars().count() < MIN_PASSWORD_LEN {
        return Err("password must contain at least 12 characters");
    }
    if password.len() > MAX_PASSWORD_LEN {
        return Err("password is too long");
    }
    Ok(())
}

/// Reads one cookie value from a `Cookie` request header.
pub fn read_cookie<'a>(header: &'a str, name: &str) -> Option<&'a str> {
    for part in header.split(';') {
        let part = part.trim();
        let Some((key, value)) = part.split_once('=') else {
            continue;
        };
        if key.trim() == name {
            let value = value.trim();
            if !value.is_empty() {
                return Some(value);
            }
        }
    }
    None
}

/// `Set-Cookie` value for a fresh session.
pub fn session_cookie(token: &str, ttl_minutes: i64, secure: bool) -> String {
    let max_age = ttl_minutes.max(1) * 60;
    let secure = if secure { "; Secure" } else { "" };
    format!(
        "{SESSION_COOKIE}={token}; Path=/; HttpOnly; SameSite=Strict; Max-Age={max_age}{secure}"
    )
}

/// `Set-Cookie` value that expires the session cookie immediately.
pub fn clear_cookie() -> String {
    format!("{SESSION_COOKIE}=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0")
}

#[derive(Debug, Clone)]
struct Attempts {
    failures: u32,
    first_failure: Instant,
    locked_until: Option<Instant>,
}

/// Fixed-window failure counter keyed by `username@ip`.
///
/// The console is a loopback admin surface, so the goal is to blunt password
/// guessing rather than to build a distributed rate limiter.
pub struct LoginThrottle {
    state: Mutex<HashMap<String, Attempts>>,
    max_failures: u32,
    window: Duration,
    lockout: Duration,
}

impl Default for LoginThrottle {
    fn default() -> Self {
        Self::new(5, Duration::from_secs(10 * 60), Duration::from_secs(5 * 60))
    }
}

impl LoginThrottle {
    pub fn new(max_failures: u32, window: Duration, lockout: Duration) -> Self {
        Self {
            state: Mutex::new(HashMap::new()),
            max_failures: max_failures.max(1),
            window,
            lockout,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Attempts>> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }

    /// Returns how long the caller must wait, if the key is currently locked.
    pub fn retry_after(&self, key: &str) -> Option<Duration> {
        let mut state = self.lock();
        let now = Instant::now();
        let attempts = state.get(key)?;
        match attempts.locked_until {
            Some(until) if until > now => Some(until - now),
            Some(_) => {
                state.remove(key);
                None
            }
            None => None,
        }
    }

    /// Records a failed attempt and returns the lockout applied, if any.
    pub fn record_failure(&mut self, key: &str) -> Option<Duration> {
        let now = Instant::now();
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let attempts = state.entry(key.to_owned()).or_insert(Attempts {
            failures: 0,
            first_failure: now,
            locked_until: None,
        });
        if now.duration_since(attempts.first_failure) > self.window {
            attempts.failures = 0;
            attempts.first_failure = now;
        }
        attempts.failures += 1;
        if attempts.failures >= self.max_failures {
            let until = now + self.lockout;
            attempts.locked_until = Some(until);
            return Some(self.lockout);
        }
        None
    }

    pub fn record_success(&mut self, key: &str) {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_are_unique_hex_and_hash_to_64_chars() {
        let first = new_token();
        let second = new_token();
        assert_eq!(first.len(), TOKEN_BYTES * 2);
        assert_ne!(first, second);
        assert!(first.chars().all(|value| value.is_ascii_hexdigit()));
        let digest = hash_token(&first);
        assert_eq!(digest.len(), 64);
        assert_eq!(digest, hash_token(&first));
        assert_ne!(digest, hash_token(&second));
    }

    #[test]
    fn passwords_and_usernames_are_validated() {
        assert!(validate_password("short").is_err());
        assert!(validate_password("twelve chars!").is_ok());
        assert!(validate_password(&"a".repeat(MAX_PASSWORD_LEN + 1)).is_err());
        assert!(validate_username("   ").is_err());
        assert!(validate_username("ad\nmin").is_err());
        assert!(validate_username("admin\n").is_ok());
        assert!(validate_username("admin").is_ok());
    }

    #[test]
    fn cookies_round_trip_through_the_header() {
        let token = new_token();
        let cookie = session_cookie(&token, 60, false);
        assert!(cookie.contains("HttpOnly"));
        assert!(cookie.contains("SameSite=Strict"));
        assert!(cookie.contains("Max-Age=3600"));
        assert!(!cookie.contains("Secure"));

        let header = format!("other=1; {cookie}");
        assert_eq!(read_cookie(&header, SESSION_COOKIE), Some(token.as_str()));
        assert_eq!(read_cookie(&header, "missing"), None);
        assert_eq!(
            read_cookie(&session_cookie("", 60, true), SESSION_COOKIE),
            None
        );

        let cleared = clear_cookie();
        assert!(cleared.contains("Max-Age=0"));
    }

    #[test]
    fn throttle_locks_after_repeated_failures() {
        let mut throttle = LoginThrottle::new(2, Duration::from_secs(60), Duration::from_secs(30));
        let key = "admin@127.0.0.1";
        assert!(throttle.retry_after(key).is_none());
        assert!(throttle.record_failure(key).is_none());
        assert!(throttle.record_failure(key).is_some());
        assert!(throttle.retry_after(key).is_some());
        throttle.record_success(key);
        assert!(throttle.retry_after(key).is_none());
    }
}
