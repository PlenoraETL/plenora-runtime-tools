//! Exact lexical predicates, evaluated before conversion or discovery.
use chrono::{DateTime, Utc};

pub(crate) fn version(value: &str) -> bool {
    value
        .as_bytes()
        .first()
        .is_some_and(|v| matches!(v, b'1'..=b'9'))
        && value.bytes().all(|v| v.is_ascii_digit())
}

pub(crate) fn uuid(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(i, v)| {
            if matches!(i, 8 | 13 | 18 | 23) {
                v == b'-'
            } else {
                v.is_ascii_digit() || matches!(v, b'a'..=b'f')
            }
        })
}

pub(crate) fn operation(value: &str) -> bool {
    value.len() <= 128
        && value.contains('.')
        && value.split('.').all(|part| {
            part.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
                && part.bytes().all(|v| {
                    v.is_ascii_lowercase() || v.is_ascii_digit() || matches!(v, b'_' | b'-')
                })
        })
}

pub(crate) fn capability(value: &str) -> bool {
    value.len() <= 128
        && value
            .strip_prefix("plenora.")
            .and_then(|v| v.strip_suffix("-tools"))
            .is_some_and(|v| {
                v.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
                    && v.bytes()
                        .all(|v| v.is_ascii_lowercase() || v.is_ascii_digit() || v == b'-')
            })
}

/// UTC profile: T/t, Z/z or +00:00; 1..9 fractional digits.
/// Decision: docs/adr/0008-chain-product-scope.md.
/// Leap seconds are rejected because `SystemTime` cannot preserve their identity.
pub(crate) fn deadline(value: &str) -> Option<DateTime<Utc>> {
    let bytes = value.as_bytes();
    if bytes.len() < 20 {
        return None;
    }
    for (i, byte) in bytes.iter().take(19).enumerate() {
        let valid = match i {
            4 | 7 => *byte == b'-',
            10 => matches!(byte, b'T' | b't'),
            13 | 16 => *byte == b':',
            _ => byte.is_ascii_digit(),
        };
        if !valid {
            return None;
        }
    }
    if bytes.get(17..19)? >= b"60".as_slice() {
        return None;
    }
    let tail = value.get(19..)?;
    let fraction = tail
        .strip_suffix('Z')
        .or_else(|| tail.strip_suffix('z'))
        .or_else(|| tail.strip_suffix("+00:00"))?;
    if !fraction.is_empty() {
        let digits = fraction.strip_prefix('.')?;
        if !(1..=9).contains(&digits.len()) || !digits.bytes().all(|v| v.is_ascii_digit()) {
            return None;
        }
    }
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|v| v.with_timezone(&Utc))
}
