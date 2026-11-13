//! Closed schema revisions and ordered script catalogue, shared by production and qualification.
use std::collections::BTreeSet;

#[derive(Clone, Copy)]
#[repr(i32)]
pub(crate) enum Version {
    V1 = 1,
    V2 = 2,
}

pub(crate) const MIGRATIONS: &[(Version, &str, &str)] = &[
    (Version::V1, "migration.sql", include_str!("migration.sql")),
    (
        Version::V2,
        "migration-v2.sql",
        include_str!("migration-v2.sql"),
    ),
    (
        Version::V2,
        "evidence-v1.sql",
        include_str!("evidence-v1.sql"),
    ),
    (
        Version::V2,
        "delivery-v1.sql",
        include_str!("delivery-v1.sql"),
    ),
    (Version::V2, "runs-v1.sql", include_str!("runs-v1.sql")),
    (
        Version::V2,
        "idempotency-v1.sql",
        include_str!("idempotency-v1.sql"),
    ),
    (Version::V2, "work-v1.sql", include_str!("work-v1.sql")),
    (
        Version::V2,
        "resources-v1.sql",
        include_str!("resources-v1.sql"),
    ),
];

pub(crate) fn supported_versions() -> BTreeSet<i32> {
    MIGRATIONS
        .iter()
        .map(|(version, _, _)| *version as i32)
        .collect()
}

pub(crate) const fn current_version() -> i32 {
    MIGRATIONS[MIGRATIONS.len() - 1].0 as i32
}
