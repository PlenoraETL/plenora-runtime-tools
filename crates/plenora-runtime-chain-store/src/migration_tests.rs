//! Migration source checks and real PostgreSQL syntax, upgrade and idempotence qualification.
#[cfg(test)]
mod cases {
    use crate::{
        AcceptanceRecord, MIGRATIONS, NegotiationContext, PostgresStore, Retention, StoreError,
    };
    use plenora_runtime_capabilities::RuntimeBinding;
    use plenora_runtime_messaging::MessageId;
    use std::{collections::BTreeSet, error::Error, fs, path::Path};
    use tokio_postgres::Client;
    use uuid::Uuid;

    fn canonical_source(sql: &str) -> bool {
        !sql.is_empty()
            && !sql.contains('\u{feff}')
            && !sql
                .chars()
                .any(|c| c.is_control() && !matches!(c, '\t' | '\r' | '\n'))
    }

    #[test]
    fn all_migration_sources_are_enumerated_and_have_canonical_encoding()
    -> Result<(), Box<dyn Error>> {
        let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut files = BTreeSet::new();
        for entry in fs::read_dir(&source)? {
            let path = entry?.path();
            if path.extension().is_some_and(|e| e == "sql") {
                let name = path
                    .file_name()
                    .ok_or("SQL filename")?
                    .to_str()
                    .ok_or("UTF-8 filename")?;
                files.insert(name.to_owned());
                assert!(
                    canonical_source(&fs::read_to_string(&path)?),
                    "noncanonical SQL source: {name}"
                );
            }
        }
        let listed: BTreeSet<_> = MIGRATIONS
            .iter()
            .map(|(name, _)| (*name).to_owned())
            .collect();
        assert_eq!(listed.len(), MIGRATIONS.len(), "duplicate migration");
        assert_eq!(files, listed, "every SQL script must run through migrate");
        for (name, sql) in MIGRATIONS {
            assert_eq!(*sql, fs::read_to_string(source.join(name))?);
            assert!(canonical_source(sql));
            // Reproduce the exact failure class without normalizing the input at runtime.
            assert!(!canonical_source(&format!("\u{feff}{sql}")));
            assert!(!canonical_source(&format!("{sql}\0")));
        }
        Ok(())
    }

    async fn verify_database(store: &PostgresStore, legacy: bool) -> Result<(), StoreError> {
        let client = store.connect().await?;
        let tables: i64 = client
            .query_one(
                "SELECT count(*) FROM pg_tables WHERE schemaname='public'",
                &[],
            )
            .await?
            .try_get(0)?;
        if tables != 0 {
            return Err(StoreError::Invalid);
        }
        if legacy {
            client.batch_execute(include_str!("migration.sql")).await?;
            client
                .execute(
                    "INSERT INTO plenora_chain_schema VALUES (true,1,604800,7776000)",
                    &[],
                )
                .await?;
        }
        drop(client);
        store.migrate(Retention::default()).await?;
        let id = MessageId::random();
        let context = NegotiationContext {
            requires: RuntimeBinding::V2,
            accepts_downgrade: false,
        };
        store.accept_async(id, Some(context)).await?;
        for policy in [
            Retention::default(),
            Retention::reduced(60, 120)?,
            Retention::reduced(60, 120)?,
        ] {
            // Runs the identical complete script sequence used by production, including ALTER.
            store.migrate(policy).await?;
            if !matches!(store.accept_async(id, None).await?, AcceptanceRecord::Existing(c) if c == context)
            {
                return Err(StoreError::Conflict);
            }
            let client = store.connect().await?;
            let row = client.query_one("SELECT version,delivery_seconds,report_seconds FROM plenora_chain_schema WHERE singleton", &[]).await?;
            if row.try_get::<_, i32>(0)? != 2
                || row.try_get::<_, i64>(1)? != policy.delivery
                || row.try_get::<_, i64>(2)? != policy.reports
            {
                return Err(StoreError::Invalid);
            }
            let tables: i64 = client
                .query_one(
                    "SELECT count(*) FROM pg_tables WHERE schemaname='public'",
                    &[],
                )
                .await?
                .try_get(0)?;
            if tables != 12 {
                return Err(StoreError::Invalid);
            }
        }
        Ok(())
    }

    async fn isolated_database(
        admin: &Client,
        base: &PostgresStore,
        legacy: bool,
    ) -> Result<(), StoreError> {
        // The identifier is generated locally from hex UUID digits, never connection/user data.
        let name = format!("migration_{}", Uuid::new_v4().simple());
        admin
            .batch_execute(&format!("CREATE DATABASE {name} TEMPLATE template0"))
            .await?;
        let mut store = base.clone();
        store.config.dbname(&name);
        let result = verify_database(&store, legacy).await;
        // Cleanup runs on both success and failure, including open driver tasks after an error.
        let cleanup = admin
            .batch_execute(&format!("DROP DATABASE {name} WITH (FORCE)"))
            .await
            .map_err(StoreError::from);
        if cleanup.is_err() {
            eprintln!("Migration qualification database cleanup failed");
        }
        result.and(cleanup)
    }

    #[tokio::test]
    #[ignore = "requires disposable PostgreSQL with CREATEDB; CI runs this before store/coverage scenarios"]
    async fn postgres_migrations_empty_upgrade_and_idempotence() -> Result<(), StoreError> {
        let connection = std::env::var("PLENORA_POSTGRES_URL").map_err(|_| StoreError::Invalid)?;
        let base = PostgresStore::new(&connection)?;
        let admin = base.connect().await?;
        for legacy in [false, true] {
            isolated_database(&admin, &base, legacy).await?;
        }
        Ok(())
    }
}
