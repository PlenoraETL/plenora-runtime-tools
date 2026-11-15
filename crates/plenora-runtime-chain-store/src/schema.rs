//! One admitted schema; the SQL shape equals the final pre-cleanup schema.
pub(crate) const VERSION: i32 = 3;
pub(crate) const BOOTSTRAP: &str = include_str!("bootstrap.sql");
pub(crate) const fn current_version() -> i32 {
    VERSION
}

// Layout guard, not a database-integrity/security attestation. Complete definitions
// are compared against the immutable baseline by the real PostgreSQL test.
// PR23: preserve the existing SQL shape, including its historical version CHECK;
// admission above accepts only VERSION (see docs/contract-alignment.md).
pub(crate) async fn validate_layout(
    tx: &tokio_postgres::Transaction<'_>,
) -> Result<(), crate::StoreError> {
    let expected: serde_json::Value = serde_json::from_str(include_str!("schema-layout.json"))
        .map_err(|_| crate::StoreError::Invalid)?;
    let actual: String = tx.query_one(r#"
        WITH tables AS (
            SELECT c.* FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname='public' AND left(c.relname,14)='plenora_chain_'
              AND c.relkind IN ('r','p','v','m','f')
        ) SELECT jsonb_build_object(
            'tables', (SELECT jsonb_agg(t.relname ORDER BY t.relname COLLATE "C") FROM tables t),
            'columns', (SELECT jsonb_agg(jsonb_build_array(t.relname,a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull,pg_get_expr(d.adbin,d.adrelid)) ORDER BY t.relname COLLATE "C",a.attnum)
                FROM tables t JOIN pg_attribute a ON a.attrelid=t.oid
                LEFT JOIN pg_attrdef d ON d.adrelid=t.oid AND d.adnum=a.attnum
                WHERE a.attnum>0 AND NOT a.attisdropped),
            'constraints', (SELECT jsonb_agg(c.conname ORDER BY c.conname COLLATE "C") FROM tables t JOIN pg_constraint c ON c.conrelid=t.oid),
            'indexes', (SELECT jsonb_agg(c.relname ORDER BY c.relname COLLATE "C") FROM tables t JOIN pg_index i ON i.indrelid=t.oid JOIN pg_class c ON c.oid=i.indexrelid),
            'invalid', EXISTS(SELECT FROM tables t WHERE t.relkind<>'r' OR t.relpersistence<>'p' OR t.relrowsecurity OR t.relforcerowsecurity)
                OR EXISTS(SELECT FROM tables t JOIN pg_index i ON i.indrelid=t.oid WHERE NOT i.indisvalid OR NOT i.indisready)
                OR EXISTS(SELECT FROM tables t JOIN pg_constraint c ON c.conrelid=t.oid WHERE NOT c.convalidated)
                OR EXISTS(SELECT FROM tables t JOIN pg_trigger g ON g.tgrelid=t.oid WHERE NOT g.tgisinternal)
                OR EXISTS(SELECT FROM tables t JOIN pg_policy p ON p.polrelid=t.oid)
                OR EXISTS(SELECT FROM tables t JOIN pg_attribute a ON a.attrelid=t.oid WHERE a.attnum>0 AND (a.attisdropped OR a.attidentity<>'' OR a.attgenerated<>''))
        )::text
    "#, &[]).await?.try_get(0)?;
    let mut actual: serde_json::Value =
        serde_json::from_str(&actual).map_err(|_| crate::StoreError::Invalid)?;
    if actual.get("invalid") != Some(&serde_json::Value::Bool(false)) {
        return Err(crate::StoreError::Invalid);
    }
    actual
        .as_object_mut()
        .ok_or(crate::StoreError::Invalid)?
        .remove("invalid");
    if actual != expected {
        return Err(crate::StoreError::Invalid);
    }
    Ok(())
}
