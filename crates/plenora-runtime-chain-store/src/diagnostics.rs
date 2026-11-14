//! Opt-in operational diagnostics. Never format driver messages, sources, SQL or values.
use std::any::Any;

fn enabled() -> bool {
    cfg!(test) || std::env::var_os("PLENORA_STORAGE_DIAGNOSTICS").is_some_and(|v| v == "1")
}

#[derive(Debug)]
struct Facts<'a> {
    kind: &'static str,
    sqlstate: &'a str,
    closed: bool,
}
fn facts(error: &dyn Any) -> Facts<'_> {
    if let Some(error) = error.downcast_ref::<tokio_postgres::Error>() {
        let state = error
            .code()
            .map_or("none", tokio_postgres::error::SqlState::code);
        let sqlstate = if state == "none"
            || (state.len() == 5
                && state
                    .bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit()))
        {
            state
        } else {
            "invalid"
        };
        return Facts {
            kind: if error.code().is_some() {
                "postgres_server"
            } else {
                "postgres_client"
            },
            sqlstate,
            closed: error.is_closed(),
        };
    }
    let kind = if error.is::<std::num::TryFromIntError>() {
        "integer_conversion"
    } else if error.is::<serde_json::Error>() {
        "journal_codec"
    } else if error.is::<crate::StoreError>() {
        "primary_authority"
    } else if error.is::<plenora_runtime_chains::Error>() {
        "chain_state"
    } else {
        "settlement_state"
    };
    Facts {
        kind,
        sqlstate: "none",
        closed: false,
    }
}

#[track_caller]
pub(crate) fn report<E: Any>(error: &E) {
    if enabled() {
        let site = std::panic::Location::caller();
        let facts = facts(error);
        eprintln!(
            "Storage failure at {}:{}; kind={}; SQLSTATE={}; closed={}",
            site.file(),
            site.line(),
            facts.kind,
            facts.sqlstate,
            facts.closed
        );
    }
}

#[track_caller]
pub(crate) fn integrity(code: &'static str) {
    if enabled() {
        let site = std::panic::Location::caller();
        eprintln!(
            "Storage integrity failure at {}:{}; code={code}",
            site.file(),
            site.line()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn diagnostics_do_not_format_untrusted_errors() -> Result<(), Box<dyn std::error::Error>> {
        let error = serde_json::from_str::<serde_json::Value>("SECRET_SENTINEL")
            .err()
            .ok_or("invalid fixture")?;
        let value = facts(&error);
        assert_eq!(value.kind, "journal_codec");
        assert!(!format!("{value:?}").contains("SECRET_SENTINEL"));
        assert_eq!(
            facts(&crate::StoreError::Unavailable).kind,
            "primary_authority"
        );
        assert_eq!(
            facts(&plenora_runtime_chains::Error::Invalid).kind,
            "chain_state"
        );
        assert_eq!(
            facts(&u8::try_from(256_u16).err().ok_or("overflow")?).kind,
            "integer_conversion"
        );
        Ok(())
    }
}
