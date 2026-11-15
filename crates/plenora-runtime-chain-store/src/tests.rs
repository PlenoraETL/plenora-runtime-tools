#[cfg(test)]
mod cases {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        reason = "Test assertions"
    )]
    use crate::*;
    use plenora_runtime_capabilities::RuntimeBinding;
    use plenora_runtime_core::Clock;
    use plenora_runtime_testkit::ManualClock;
    use std::{
        collections::BTreeMap,
        error::Error,
        sync::Arc,
        time::{Duration, SystemTime},
    };

    const CONTEXT: NegotiationContext = NegotiationContext {
        requires: RuntimeBinding::V2,
        accepts_downgrade: false,
    };
    // This model exists only inside cfg(test); there is no memory production profile.
    struct MemoryDouble {
        clock: Arc<dyn Clock>,
        records: BTreeMap<MessageId, SystemTime>,
        available: bool,
    }
    impl MemoryDouble {
        fn accept(&mut self, id: MessageId) -> Result<bool, StoreError> {
            if !self.available {
                return Err(StoreError::Unavailable);
            }
            if let Some(expiry) = self.records.get(&id) {
                if self.clock.now() >= *expiry {
                    return Err(StoreError::Expired);
                }
                return Ok(false);
            }
            self.records
                .insert(id, self.clock.now() + Duration::from_hours(168));
            Ok(true)
        }
    }
    #[test]
    fn memory_double_retains_ids_and_requires_confirmation() {
        let clock = Arc::new(ManualClock::default());
        let mut store = MemoryDouble {
            clock: clock.clone(),
            records: BTreeMap::new(),
            available: true,
        };
        let id = MessageId::random();
        assert_eq!(store.accept(id), Ok(true));
        assert_eq!(store.accept(id), Ok(false));
        clock.advance(Duration::from_hours(168)).unwrap();
        assert_eq!(store.accept(id), Err(StoreError::Expired));
        assert!(store.records.contains_key(&id));
        store.available = false;
        assert_eq!(
            store.accept(MessageId::random()),
            Err(StoreError::Unavailable)
        );
    }
    #[test]
    fn policy_and_input_are_exact_and_errors_are_data_free() {
        for (delivery, reports) in [(0, 90), (604_801, 7_776_000), (1, 7_776_001), (2, 1)] {
            assert_eq!(
                Retention::reduced(delivery, reports),
                Err(StoreError::Invalid)
            );
        }
        let driver_error = Config::from_str("sentinel=sentinel").unwrap_err();
        let converted = StoreError::from(driver_error);
        assert_eq!(converted, StoreError::Unavailable);
        assert!(converted.source().is_none());
        assert_eq!(
            format!("{converted:?} {converted}"),
            "Unavailable chain store Unavailable"
        );
        let store = PostgresStore::new("host=localhost user=sentinel password=sentinel").unwrap();
        assert!(!format!("{store:?}").contains("sentinel"));
        for error in [
            StoreError::Invalid,
            StoreError::Conflict,
            StoreError::Unavailable,
            StoreError::Unconfirmed,
        ] {
            assert!(error.source().is_none());
            assert!(!format!("{error:?} {error}").contains("sentinel"));
        }
    }
    fn database() -> String {
        std::env::var("PLENORA_POSTGRES_URL")
            .expect("PLENORA_POSTGRES_URL required for ignored tests")
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires dedicated real PostgreSQL; run only postgres_real with --ignored"]
    #[allow(
        clippy::too_many_lines,
        reason = "Ordered crash and retention scenario on one disposable database"
    )]
    async fn postgres_real() -> Result<(), Box<dyn Error>> {
        let store = PostgresStore::new(&database())?;
        store.migrate(Retention::default()).await?;
        let admin = store.connect().await?;
        // case: acceptance_race
        let id = MessageId::random();
        let mut jobs = Vec::new();
        for _ in 0..8 {
            let store = store.clone();
            jobs.push(tokio::spawn(async move {
                store.accept_async(id, Some(CONTEXT)).await
            }));
        }
        let mut created = 0;
        for job in jobs {
            if job.await?? == AcceptanceRecord::Created {
                created += 1;
            }
        }
        assert_eq!(created, 1);
        let sync_store = store.clone();
        let acceptance =
            tokio::task::spawn_blocking(move || AcceptanceStore::accept(&sync_store, &id, None))
                .await??;
        assert!(matches!(acceptance, AcceptanceRecord::Existing(CONTEXT)));
        let mut repeatable = store.clone();
        repeatable
            .config
            .options("-c default_transaction_isolation=repeatable\\ read");
        let mut connection = repeatable.connect().await?;
        let transaction = PostgresStore::transaction(&mut connection).await?;
        let isolation: String = transaction
            .query_one("SHOW transaction_isolation", &[])
            .await?
            .try_get(0)?;
        assert_eq!(isolation, "read committed");
        PostgresStore::commit(transaction).await?;
        // case: authoritative_read
        let mut readonly = store.clone();
        readonly
            .config
            .options("-c default_transaction_read_only=on");
        assert!(readonly.accept_async(id, None).await.is_err());
        assert!(
            readonly
                .accept_async(MessageId::random(), Some(CONTEXT))
                .await
                .is_err()
        );
        // case: store_unavailable
        let offline = PostgresStore::new("host=127.0.0.1 port=1 user=none connect_timeout=1")?;
        assert!(
            offline
                .accept_async(MessageId::random(), Some(CONTEXT))
                .await
                .is_err()
        );
        // case: commit_unconfirmed
        let mut client = store.connect().await?;
        let tx = PostgresStore::transaction(&mut client).await?;
        let killed = MessageId::random().to_string();
        tx.execute("INSERT INTO plenora_chain_acceptance(id,requires,downgrade,expires_at) VALUES ($1,'plenora-runtime-binding-v2',false,clock_timestamp()+interval '1 day')", &[&killed]).await?;
        let pid: i32 = tx
            .query_one("SELECT pg_backend_pid()", &[])
            .await?
            .try_get(0)?;
        admin
            .query_one("SELECT pg_terminate_backend($1)", &[&pid])
            .await?;
        assert_eq!(
            PostgresStore::commit(tx).await,
            Err(StoreError::Unconfirmed)
        );
        assert!(matches!(
            store
                .accept_async(MessageId::from_str(&killed)?, None)
                .await,
            Err(StoreError::Missing)
        ));
        // case: tombstone
        admin.execute("UPDATE plenora_chain_acceptance SET expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", &[&id.to_string()]).await?;
        assert_eq!(
            store.accept_async(id, Some(CONTEXT)).await,
            Err(StoreError::Expired)
        );
        // case: retention_reduction
        store.migrate(Retention::reduced(60, 120)?).await?;
        assert_eq!(
            store.migrate(Retention::default()).await,
            Err(StoreError::Invalid)
        );
        Ok(())
    }
}
