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
            .map(|(_, name, _)| (*name).to_owned())
            .collect();
        assert_eq!(listed.len(), MIGRATIONS.len(), "duplicate migration");
        assert_eq!(files, listed, "every SQL script must run through migrate");
        for (_, name, sql) in MIGRATIONS {
            assert_eq!(*sql, fs::read_to_string(source.join(name))?);
            assert!(canonical_source(sql));
            // Reproduce the exact failure class without normalizing the input at runtime.
            assert!(!canonical_source(&format!("\u{feff}{sql}")));
            assert!(!canonical_source(&format!("{sql}\0")));
        }
        Ok(())
    }

    fn expected_tables() -> BTreeSet<String> {
        MIGRATIONS
            .iter()
            .flat_map(|(_, _, sql)| sql.lines())
            .filter_map(|line| line.strip_prefix("CREATE TABLE IF NOT EXISTS "))
            .filter_map(|line| line.split_whitespace().next())
            .map(str::to_owned)
            .collect()
    }

    #[test]
    fn schema_versions_and_tables_follow_the_complete_ordered_catalogue() {
        let versions: Vec<_> = MIGRATIONS.iter().map(|(v, _, _)| *v as i32).collect();
        assert!(versions.windows(2).all(|pair| pair[0] <= pair[1]));
        assert_eq!(versions.last(), Some(&crate::current_version()));
        assert_eq!(
            versions.iter().copied().collect::<BTreeSet<_>>(),
            crate::supported_versions()
        );
        assert!(expected_tables().contains("plenora_chain_resources"));
        assert!(expected_tables().contains("plenora_chain_runs"));
        assert!(!crate::supported_versions().contains(&0));
        assert!(!crate::supported_versions().contains(&(crate::current_version() + 1)));
    }

    async fn seed_delivery(client: &Client, prefix: usize) -> Result<(String, bool), StoreError> {
        let legacy_key = format!("migration:{}", MessageId::random());
        let legacy = if MIGRATIONS[..prefix]
            .iter()
            .any(|(_, name, _)| *name == "delivery-v1.sql")
        {
            use plenora_runtime_boundary::{DeliveryJournal, DeliveryPlan};
            let journal = DeliveryJournal::new(
                &legacy_key,
                [4; 32],
                DeliveryPlan::new("migration-route", 1, 0, 4096, 1024)
                    .map_err(|_| StoreError::Invalid)?,
                1,
                1,
                2,
            )
            .map_err(|_| StoreError::Invalid)?;
            let bytes = journal.to_json().map_err(|_| StoreError::Invalid)?;
            client.execute("INSERT INTO plenora_chain_acceptance(id,requires,downgrade,expires_at) VALUES ($1,'plenora-runtime-binding-v2',false,clock_timestamp())", &[&journal.result_id()]).await?;
            if MIGRATIONS[..prefix]
                .iter()
                .any(|(_, name, _)| *name == "delivery-retention-v3.sql")
            {
                client.execute("INSERT INTO plenora_chain_delivery(key,result_id,route,stage,journal,created_ms,expired_report) VALUES ($1,$2,'migration-route','ready',$3,1,false)", &[&legacy_key,&journal.result_id(),&bytes]).await?;
            } else {
                client.execute("INSERT INTO plenora_chain_delivery(key,result_id,route,stage,journal) VALUES ($1,$2,'migration-route','ready',$3)", &[&legacy_key,&journal.result_id(),&bytes]).await?;
            }
            true
        } else {
            false
        };
        Ok((legacy_key, legacy))
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

    async fn verify_database(store: &PostgresStore, prefix: usize) -> Result<(), StoreError> {
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
        if prefix > 0 {
            for (_, _, sql) in &MIGRATIONS[..prefix] {
                client.batch_execute(sql).await?;
            }
            client
                .execute(
                    "INSERT INTO plenora_chain_schema VALUES (true,$1,$2,$3)",
                    &[
                        &(MIGRATIONS[prefix - 1].0 as i32),
                        &Retention::default().delivery,
                        &Retention::default().reports,
                    ],
                )
                .await?;
        }
        let (legacy_key, legacy) = seed_delivery(&client, prefix).await?;
        drop(client);
        store.migrate(Retention::default()).await?;
        if legacy {
            let client = store.connect().await?;
            let row = client.query_one("SELECT created_ms,expired_report,journal FROM plenora_chain_delivery WHERE key=$1", &[&legacy_key]).await?;
            assert_eq!(row.try_get::<_, i64>(0)?, 1);
            assert!(row.try_get::<_, bool>(1)?);
            let j = plenora_runtime_boundary::DeliveryJournal::from_json(
                &row.try_get::<_, Vec<u8>>(2)?,
            )
            .map_err(|_| StoreError::Invalid)?;
            assert_eq!(j.created_ms(), 1);
            assert!(j.report_expired());
        }
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
        if prefix == 0 {
            verify_bounded_retention(store).await?;
        }
        verify_unsupported_versions(store).await
    }

    async fn verify_unsupported_versions(store: &PostgresStore) -> Result<(), StoreError> {
        let client = store.connect().await?;
        for unsupported in [0, crate::current_version() + 1] {
            // A rejected upgrade must roll back table creation, not only the version row.
            client
                .batch_execute("DROP TABLE plenora_chain_resources")
                .await?;
            client.batch_execute("ALTER TABLE plenora_chain_schema DROP CONSTRAINT IF EXISTS plenora_chain_schema_version_check").await?;
            client
                .execute(
                    "UPDATE plenora_chain_schema SET version=$1",
                    &[&unsupported],
                )
                .await?;
            if !matches!(
                store.migrate(Retention::reduced(60, 120)?).await,
                Err(StoreError::Invalid)
            ) {
                return Err(StoreError::Conflict);
            }
            let actual: i32 = client
                .query_one(
                    "SELECT version FROM plenora_chain_schema WHERE singleton",
                    &[],
                )
                .await?
                .try_get(0)?;
            let absent: bool = client
                .query_one(
                    "SELECT to_regclass('public.plenora_chain_resources') IS NULL",
                    &[],
                )
                .await?
                .try_get(0)?;
            if actual != unsupported || !absent {
                return Err(StoreError::Conflict);
            }
            client
                .execute(
                    "UPDATE plenora_chain_schema SET version=$1",
                    &[&crate::current_version()],
                )
                .await?;
            store.migrate(Retention::reduced(60, 120)?).await?;
        }
        Ok(())
    }

    async fn isolated_database(
        admin: &Client,
        base: &PostgresStore,
        prefix: usize,
    ) -> Result<(), StoreError> {
        // The identifier is generated locally from hex UUID digits, never connection/user data.
        let name = format!("migration_{}", Uuid::new_v4().simple());
        admin
            .batch_execute(&format!("CREATE DATABASE {name} TEMPLATE template0"))
            .await?;
        let mut store = base.clone();
        store.config.dbname(&name);
        let result = verify_database(&store, prefix).await;
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
        let mut failed = false;
        for prefix in 0..=MIGRATIONS.len() {
            if let Err(error) = isolated_database(&admin, &base, prefix).await {
                eprintln!("Migration prefix {prefix} failed: {error:?}");
                failed = true;
            }
        }
        if failed {
            return Err(StoreError::Invalid);
        }
        Ok(())
    }
}
