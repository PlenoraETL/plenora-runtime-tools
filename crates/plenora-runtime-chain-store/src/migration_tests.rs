//! Current bootstrap source and real PostgreSQL idempotence qualification.
#[cfg(test)]
mod cases {
    use crate::{
        AcceptanceRecord, BOOTSTRAP, NegotiationContext, PostgresStore, Retention, StoreError,
    };
    use plenora_runtime_capabilities::RuntimeBinding;
    use plenora_runtime_messaging::MessageId;
    use std::{collections::BTreeSet, error::Error, fs, path::Path};
    use tokio_postgres::Client;
    use uuid::Uuid;

    fn canonical_source(sql: &str) -> bool {
        !sql.is_empty()
            && !sql.contains('\r')
            && !sql.contains('\u{feff}')
            && !sql
                .chars()
                .any(|c| c.is_control() && !matches!(c, '\t' | '\n'))
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
        assert_eq!(files, BTreeSet::from(["bootstrap.sql".to_owned()]));
        assert_eq!(BOOTSTRAP, fs::read_to_string(source.join("bootstrap.sql"))?);
        assert!(canonical_source(BOOTSTRAP));
        assert!(!canonical_source(&format!("\u{feff}{BOOTSTRAP}")));
        assert!(!canonical_source(&format!("{BOOTSTRAP}\0")));
        Ok(())
    }

    fn expected_tables() -> BTreeSet<String> {
        BOOTSTRAP
            .lines()
            .filter_map(|line| line.strip_prefix("CREATE TABLE "))
            .filter_map(|line| line.split_whitespace().next())
            .map(str::to_owned)
            .collect()
    }

    #[test]
    fn exactly_one_current_schema_and_no_incremental_scripts() {
        assert_eq!(crate::current_version(), 3);
        assert_eq!(
            BOOTSTRAP
                .matches("CREATE TABLE plenora_chain_schema (")
                .count(),
            1
        );
        assert!(BOOTSTRAP.contains("version integer NOT NULL CHECK (version IN (1,2,3))"));
        assert!(!BOOTSTRAP.contains("ALTER TABLE"));
        assert!(!BOOTSTRAP.contains("UPDATE "));
        assert!(expected_tables().contains("plenora_chain_resources"));
        assert!(expected_tables().contains("plenora_chain_runs"));
        assert_eq!(expected_tables().len(), 13);
    }

    #[test]
    fn baseline_oracle_is_frozen_and_layout_is_complete() -> Result<(), Box<dyn Error>> {
        use sha2::{Digest, Sha256};
        let oracle = include_str!("../tests/fixtures/baseline-schema.sql");
        assert!(canonical_source(oracle));
        assert_eq!(
            format!("{:x}", Sha256::digest(oracle.as_bytes())),
            "deafe93105cbdcb32e6426b1440ef32b57ce7e690f693cac45784961c38bb32f"
        );
        let layout: serde_json::Value = serde_json::from_str(include_str!("schema-layout.json"))?;
        assert_eq!(layout["columns"].as_array().ok_or("columns")?.len(), 72);
        assert_eq!(
            layout["constraints"].as_array().ok_or("constraints")?.len(),
            40
        );
        assert_eq!(layout["indexes"].as_array().ok_or("indexes")?.len(), 21);
        Ok(())
    }

    async fn verify_bounded_retention(store: &PostgresStore) -> Result<(), StoreError> {
        use plenora_runtime_boundary::{DeliveryJournal, DeliveryPlan};
        let mut client = store.connect().await?;
        let tx = PostgresStore::transaction(&mut client).await?;
        let mut keys = Vec::new();
        for _ in 0..33 {
            let key = format!("batch:{}", MessageId::random());
            let journal = DeliveryJournal::new(
                &key,
                [5; 32],
                DeliveryPlan::new("batch-route", 1, 0, 4096, 1024)
                    .map_err(|_| StoreError::Invalid)?,
                1,
                1,
                2,
            )
            .map_err(|_| StoreError::Invalid)?;
            let bytes = journal.to_json().map_err(|_| StoreError::Invalid)?;
            tx.execute("INSERT INTO plenora_chain_acceptance(id,requires,downgrade,expires_at) VALUES ($1,'plenora-runtime-binding-v2',false,clock_timestamp())", &[&journal.result_id()]).await?;
            tx.execute("INSERT INTO plenora_chain_delivery(key,result_id,route,stage,journal,created_ms,expired_report) VALUES ($1,$2,'batch-route','ready',$3,1,false)", &[&key,&journal.result_id(),&bytes]).await?;
            keys.push(key);
        }
        for expected in [32_i64, 33] {
            PostgresStore::expire_delivery_reports(&tx).await?;
            let actual: i64 = tx.query_one("SELECT count(*) FROM plenora_chain_delivery WHERE key=ANY($1) AND expired_report", &[&keys]).await?.try_get(0)?;
            assert_eq!(
                actual, expected,
                "bounded maintenance must make monotone progress"
            );
        }
        tx.rollback().await?;
        Ok(())
    }

    async fn verify_database(store: &PostgresStore) -> Result<(), StoreError> {
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
            // Reopening uses the same bootstrap and cannot extend retention.
            store.migrate(policy).await?;
            if !matches!(store.accept_async(id, None).await?, AcceptanceRecord::Existing(c) if c == context)
            {
                return Err(StoreError::Conflict);
            }
            let client = store.connect().await?;
            let row = client.query_one("SELECT version,delivery_seconds,report_seconds FROM plenora_chain_schema WHERE singleton", &[]).await?;
            if row.try_get::<_, i32>(0)? != crate::current_version()
                || row.try_get::<_, i64>(1)? != policy.delivery
                || row.try_get::<_, i64>(2)? != policy.reports
            {
                eprintln!("Migration schema version or retention mismatch");
                return Err(StoreError::Invalid);
            }
            let tables: BTreeSet<String> = client
                .query(
                    "SELECT tablename FROM pg_tables WHERE schemaname='public'",
                    &[],
                )
                .await?
                .iter()
                .map(|r| r.try_get(0))
                .collect::<Result<_, _>>()?;
            if tables != expected_tables() {
                eprintln!("Migration table catalogue mismatch");
                return Err(StoreError::Invalid);
            }
        }
        verify_bounded_retention(store).await?;
        verify_unsupported_versions(store).await
    }

    async fn verify_unsupported_versions(store: &PostgresStore) -> Result<(), StoreError> {
        let client = store.connect().await?;
        for unsupported in [0, 1, 2, crate::current_version() + 1] {
            client.batch_execute("ALTER TABLE plenora_chain_schema DROP CONSTRAINT plenora_chain_schema_version_check").await?;
            client
                .execute(
                    "UPDATE plenora_chain_schema SET version=$1",
                    &[&unsupported],
                )
                .await?;
            assert!(matches!(
                store.migrate(Retention::reduced(60, 120)?).await,
                Err(StoreError::Invalid)
            ));
            let actual: i32 = client
                .query_one(
                    "SELECT version FROM plenora_chain_schema WHERE singleton",
                    &[],
                )
                .await?
                .try_get(0)?;
            assert_eq!(actual, unsupported);
            client
                .execute(
                    "UPDATE plenora_chain_schema SET version=$1",
                    &[&crate::current_version()],
                )
                .await?;
            // Restoring the row alone must not hide a missing constraint.
            assert!(matches!(
                store.migrate(Retention::reduced(60, 120)?).await,
                Err(StoreError::Invalid)
            ));
            client.batch_execute("ALTER TABLE plenora_chain_schema ADD CONSTRAINT plenora_chain_schema_version_check CHECK (version IN (1,2,3))").await?;
            store.migrate(Retention::reduced(60, 120)?).await?;
        }
        // Rename instead of deleting: rejection must preserve the existing rows unchanged.
        for alteration in [
            (
                "CREATE TABLE plenora_chain_extra ()",
                "DROP TABLE plenora_chain_extra",
            ),
            (
                "ALTER TABLE plenora_chain_runs ENABLE ROW LEVEL SECURITY",
                "ALTER TABLE plenora_chain_runs DISABLE ROW LEVEL SECURITY",
            ),
            (
                "ALTER TABLE plenora_chain_resources RENAME TO absent_resources",
                "ALTER TABLE absent_resources RENAME TO plenora_chain_resources",
            ),
            (
                "ALTER INDEX plenora_chain_resources_due RENAME TO absent_index",
                "ALTER INDEX absent_index RENAME TO plenora_chain_resources_due",
            ),
            (
                "ALTER TABLE plenora_chain_runs ALTER COLUMN work_fence DROP NOT NULL",
                "ALTER TABLE plenora_chain_runs ALTER COLUMN work_fence SET NOT NULL",
            ),
        ] {
            client.batch_execute(alteration.0).await?;
            assert!(matches!(
                store.migrate(Retention::reduced(60, 120)?).await,
                Err(StoreError::Invalid)
            ));
            client.batch_execute(alteration.1).await?;
            store.migrate(Retention::reduced(60, 120)?).await?;
        }
        client.batch_execute("CREATE TEMP TABLE marker_saved AS SELECT * FROM plenora_chain_schema; DELETE FROM plenora_chain_schema").await?;
        assert!(matches!(
            store.migrate(Retention::default()).await,
            Err(StoreError::Invalid)
        ));
        client.batch_execute("INSERT INTO plenora_chain_schema SELECT * FROM marker_saved; DROP TABLE marker_saved").await?;
        client
            .batch_execute("ALTER TABLE plenora_chain_schema RENAME TO absent_schema")
            .await?;
        assert!(matches!(
            store.migrate(Retention::default()).await,
            Err(StoreError::Invalid)
        ));
        assert!(
            client
                .query_one("SELECT to_regclass('plenora_chain_schema') IS NULL", &[])
                .await?
                .try_get::<_, bool>(0)?
        );
        client
            .batch_execute("ALTER TABLE absent_schema RENAME TO plenora_chain_schema")
            .await?;
        Ok(())
    }

    async fn catalog(client: &impl tokio_postgres::GenericClient) -> Result<String, StoreError> {
        client
            .query_one(include_str!("../tests/fixtures/catalog.sql"), &[])
            .await?
            .try_get(0)
            .map_err(StoreError::from)
    }

    async fn verify_catalog_equivalence(store: &PostgresStore) -> Result<(), StoreError> {
        let mut client = store.connect().await?;
        client.batch_execute("SET search_path = public").await?;
        // Immutable oracle copied from HEAD a973b6e, including the final Rust DDL/row.
        // Never called by production: there remains exactly one bootstrap path.
        client
            .batch_execute(include_str!("../tests/fixtures/baseline-schema.sql"))
            .await?;
        let baseline = catalog(&client).await?;
        let baseline_row = client
            .query_one(
                "SELECT row_to_json(s)::text FROM plenora_chain_schema s",
                &[],
            )
            .await?
            .try_get::<_, String>(0)?;
        for table in expected_tables() {
            client
                .batch_execute(&format!("DROP TABLE {table} CASCADE"))
                .await?;
        }
        store.migrate(Retention::default()).await?;
        let actual = catalog(&client).await?;
        let actual_row: String = client
            .query_one(
                "SELECT row_to_json(s)::text FROM plenora_chain_schema s",
                &[],
            )
            .await?
            .try_get(0)?;
        if actual != baseline || actual_row != baseline_row {
            eprintln!("Bootstrap catalog or marker differs from the final baseline");
            return Err(StoreError::Invalid);
        }
        // Prove the oracle observes definitions, not just counts/names.
        for mutation in [
            "ALTER TABLE plenora_chain_delivery ALTER COLUMN delivered SET DEFAULT true",
            "ALTER TABLE plenora_chain_schema DROP CONSTRAINT plenora_chain_schema_version_check; ALTER TABLE plenora_chain_schema ADD CONSTRAINT plenora_chain_schema_version_check CHECK (version=3)",
            "DROP INDEX plenora_chain_resources_due",
            "GRANT SELECT ON plenora_chain_delivery TO PUBLIC",
        ] {
            let tx = client.transaction().await?;
            tx.batch_execute(mutation).await?;
            if catalog(&tx).await? == baseline {
                tx.rollback().await?;
                eprintln!("Catalog oracle did not detect a changed definition");
                return Err(StoreError::Invalid);
            }
            tx.rollback().await?;
        }
        Ok(())
    }

    async fn isolated_database(
        admin: &Client,
        base: &PostgresStore,
        equivalence: bool,
    ) -> Result<(), StoreError> {
        // The identifier is generated locally from hex UUID digits, never connection/user data.
        let name = format!("migration_{}", Uuid::new_v4().simple());
        admin
            .batch_execute(&format!("CREATE DATABASE {name} TEMPLATE template0"))
            .await?;
        let mut store = base.clone();
        store.config.dbname(&name);
        let result = tokio::spawn(async move {
            if equivalence {
                verify_catalog_equivalence(&store).await
            } else {
                verify_database(&store).await
            }
        })
        .await
        .map_err(|_| StoreError::Invalid)
        .and_then(|result| result);
        // Cleanup runs on both success and failure, including assertions/panics and open driver tasks after an error.
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
    async fn postgres_bootstrap_empty_reopen_and_rejection() -> Result<(), StoreError> {
        let connection = std::env::var("PLENORA_POSTGRES_URL").map_err(|_| StoreError::Invalid)?;
        let base = PostgresStore::new(&connection)?;
        let admin = base.connect().await?;
        isolated_database(&admin, &base, false).await?;
        Ok(())
    }
    #[tokio::test]
    #[ignore = "requires disposable PostgreSQL with CREATEDB; compares full final baseline catalog"]
    async fn postgres_bootstrap_matches_baseline_catalog() -> Result<(), StoreError> {
        let connection = std::env::var("PLENORA_POSTGRES_URL").map_err(|_| StoreError::Invalid)?;
        let base = PostgresStore::new(&connection)?;
        let admin = base.connect().await?;
        isolated_database(&admin, &base, true).await
    }
}
