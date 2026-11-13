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
    const RT_CASES: &[(&str, &[&str])] = &[
        (
            "RT-022",
            &[
                "identity_race",
                "key_conflict",
                "step_fence",
                "crash_restart",
                "atomic_finish",
                "result_replay",
            ],
        ),
        (
            "RT-027",
            &[
                "acceptance_race",
                "authoritative_read",
                "store_unavailable",
                "commit_unconfirmed",
                "tombstone",
                "retention_reduction",
            ],
        ),
    ];

    #[test]
    fn enumerate_p2_2_register_d2() {
        assert_eq!(
            RT_CASES.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            ["RT-022", "RT-027"]
        );
        assert_eq!(
            RT_CASES.iter().map(|(_, cases)| cases.len()).sum::<usize>(),
            12
        );
        for (_, cases) in RT_CASES {
            for case in *cases {
                assert!(include_str!("tests.rs").contains(&format!("// case: {case}")));
            }
        }
    }

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
        let id = MessageId::random();
        for step in ["", "A", "step x", "?", "0start"] {
            assert_eq!(
                validate(&invocation(id, "run", step, None, b"input")),
                Err(StoreError::Invalid)
            );
        }
        assert_eq!(
            validate(&invocation(id, "run", "step", Some(""), b"input")),
            Err(StoreError::Invalid)
        );
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
    fn invocation<'a>(
        id: MessageId,
        scope: &'a str,
        step: &'a str,
        key: Option<&'a str>,
        bytes: &'a [u8],
    ) -> Invocation<'a> {
        Invocation {
            message_id: id,
            scope,
            step,
            key: key.map(|value| ScopedKey { scope, value }),
            canonical_input: bytes,
        }
    }
    fn database() -> String {
        std::env::var("PLENORA_POSTGRES_URL")
            .expect("PLENORA_POSTGRES_URL required for ignored tests")
    }
    async fn accepted(store: &PostgresStore) -> MessageId {
        let id = MessageId::random();
        assert_eq!(
            store.accept_async(id, Some(CONTEXT)).await.unwrap(),
            AcceptanceRecord::Created
        );
        id
    }
    async fn started(store: &PostgresStore, id: MessageId, scope: &str) -> StartPermit {
        match store
            .start(invocation(
                id,
                scope,
                "step",
                Some("key"),
                b"canonical input",
            ))
            .await
            .unwrap()
        {
            StartDecision::Started(permit) => permit,
            other => panic!("unexpected start decision: {other:?}"),
        }
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
        // case: identity_race
        let scope = Uuid::new_v4().to_string();
        let mut jobs = Vec::new();
        for _ in 0..8 {
            let store = store.clone();
            let scope = scope.clone();
            jobs.push(tokio::spawn(async move {
                store
                    .start(invocation(
                        id,
                        &scope,
                        "step",
                        Some("key"),
                        b"canonical input",
                    ))
                    .await
            }));
        }
        let mut permits = Vec::new();
        for job in jobs {
            if let StartDecision::Started(p) = job.await?? {
                permits.push(p);
            }
        }
        assert_eq!(permits.len(), 1);
        let permit = permits.pop().unwrap();
        // case: key_conflict
        let duplicate = accepted(&store).await;
        assert!(matches!(
            store
                .start(invocation(
                    duplicate,
                    &scope,
                    "step",
                    Some("key"),
                    b"different"
                ))
                .await,
            Err(StoreError::Conflict)
        ));
        assert!(
            matches!(store.start(invocation(duplicate,&scope,"step",Some("key"),b"canonical input")).await?, StartDecision::RequiresRecovery(original) if original==id)
        );
        assert!(matches!(
            store
                .start(invocation(
                    duplicate,
                    "another-run",
                    "other",
                    None,
                    b"canonical input"
                ))
                .await,
            Err(StoreError::Conflict)
        ));
        let mut altered_binding =
            invocation(id, "changed-run", "changed-step", None, b"canonical input");
        altered_binding.key = Some(ScopedKey {
            scope: &scope,
            value: "key",
        });
        assert!(matches!(
            store.start(altered_binding).await,
            Err(StoreError::Conflict)
        ));
        let binary_key_id = accepted(&store).await;
        let binary_key_scope = binary_key_id.to_string();
        assert!(matches!(
            store
                .start(invocation(
                    binary_key_id,
                    &binary_key_scope,
                    "step",
                    Some("key\0suffix"),
                    b"binary key request"
                ))
                .await?,
            StartDecision::Started(_)
        ));
        assert!(matches!(
            store
                .start(invocation(
                    binary_key_id,
                    &binary_key_scope,
                    "step",
                    Some("key\0suffix"),
                    b"binary key request"
                ))
                .await?,
            StartDecision::RequiresRecovery(_)
        ));
        assert!(matches!(
            store
                .start(invocation(
                    binary_key_id,
                    &binary_key_scope,
                    "step",
                    Some("key"),
                    b"binary key request"
                ))
                .await,
            Err(StoreError::Conflict)
        ));
        // case: step_fence
        let forged = StartPermit {
            id: permit.id.clone(),
            fence: Uuid::new_v4().to_string(),
        };
        assert_eq!(
            store
                .finish(&forged, MessageId::random(), b"report", b"envelope")
                .await,
            Err(StoreError::StaleFence)
        );
        // Distinct identities and scopes sharing a tenant/operation key still start once.
        let shared_scope = Uuid::new_v4().to_string();
        let mut jobs = Vec::new();
        for _ in 0..8 {
            let candidate = accepted(&store).await;
            let store = store.clone();
            let key_scope = shared_scope.clone();
            jobs.push(tokio::spawn(async move {
                let run_scope = candidate.to_string();
                let mut request = invocation(candidate, &run_scope, "step", None, b"same request");
                request.key = Some(ScopedKey {
                    scope: &key_scope,
                    value: "same key",
                });
                store.start(request).await
            }));
        }
        let mut starts = 0;
        for job in jobs {
            if matches!(job.await??, StartDecision::Started(_)) {
                starts += 1;
            }
        }
        assert_eq!(starts, 1);
        let run_a = Uuid::new_v4().to_string();
        let run_b = Uuid::new_v4().to_string();
        let key_scope = Uuid::new_v4().to_string();
        for scope in [&run_a, &run_b] {
            let candidate = accepted(&store).await;
            let mut request = invocation(candidate, scope, "step", None, b"slot request");
            request.key = Some(ScopedKey {
                scope: &key_scope,
                value: "slot key",
            });
            let decision = store.start(request).await?;
            if scope == &run_a {
                assert!(matches!(decision, StartDecision::Started(_)));
            } else {
                assert!(matches!(decision, StartDecision::RequiresRecovery(_)));
            }
        }
        for key in [None, Some("different key")] {
            let candidate = accepted(&store).await;
            assert!(matches!(
                store
                    .start(invocation(candidate, &run_b, "step", key, b"slot request"))
                    .await,
                Err(StoreError::Conflict)
            ));
        }
        // At-most-one-start also holds without caller idempotency support.
        let unkeyed = accepted(&store).await;
        let mut jobs = Vec::new();
        for _ in 0..8 {
            let store = store.clone();
            jobs.push(tokio::spawn(async move {
                store
                    .start(invocation(
                        unkeyed,
                        &unkeyed.to_string(),
                        "step",
                        None,
                        b"same request",
                    ))
                    .await
            }));
        }
        let mut starts = 0;
        for job in jobs {
            if matches!(job.await??, StartDecision::Started(_)) {
                starts += 1;
            }
        }
        assert_eq!(starts, 1);
        // case: atomic_finish
        assert!(!store.contains(id).await?);
        assert!(store.record_processed(id).await.is_err());
        let result_id = MessageId::random();
        admin.batch_execute("CREATE FUNCTION plenora_test_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected crash point'; END $$; CREATE TRIGGER plenora_test_fail BEFORE INSERT ON plenora_chain_inbox FOR EACH ROW EXECUTE FUNCTION plenora_test_fail()").await?;
        assert!(
            store
                .finish(&permit, result_id, b"report", b"envelope")
                .await
                .is_err()
        );
        assert!(!store.read(id).await?.finished);
        assert!(store.claim().await?.is_none());
        admin.batch_execute("DROP TRIGGER plenora_test_fail ON plenora_chain_inbox; DROP FUNCTION plenora_test_fail()").await?;
        store
            .finish(&permit, result_id, b"report", b"envelope")
            .await?;
        assert!(store.contains(id).await?);
        assert!(store.contains(duplicate).await?);
        // case: result_replay
        store
            .finish(&permit, result_id, b"report", b"envelope")
            .await?;
        assert_eq!(
            store
                .finish(&permit, result_id, b"changed", b"envelope")
                .await,
            Err(StoreError::Conflict)
        );
        assert!(
            matches!(store.start(invocation(id,&scope,"step",Some("key"),b"canonical input")).await?,StartDecision::Replay(original) if original==id)
        );
        assert_eq!(
            store
                .finish(&permit, result_id, b"report", b"changed envelope")
                .await,
            Err(StoreError::Conflict)
        );
        assert_eq!(
            store
                .finish(&permit, MessageId::random(), b"report", b"envelope")
                .await,
            Err(StoreError::Conflict)
        );
        let collision = accepted(&store).await;
        let collision_permit = started(&store, collision, &collision.to_string()).await;
        assert_eq!(
            store
                .finish(&collision_permit, result_id, b"report", b"envelope")
                .await,
            Err(StoreError::Conflict)
        );
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
        let restarted = PostgresStore::new(&database())?;
        assert_eq!(
            restarted.read(id).await?.report.as_deref(),
            Some(b"report".as_slice())
        );
        let stale = store.claim().await?.unwrap();
        assert_eq!(stale.result_id, result_id.to_string());
        assert!(restarted.claim().await?.is_none());
        admin.execute("UPDATE plenora_chain_outbox SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", &[&id.to_string()]).await?;
        let current = restarted.claim().await?.unwrap();
        assert_eq!(current.payload, b"envelope");
        assert_eq!(store.confirm(&stale).await, Err(StoreError::StaleFence));
        restarted.confirm(&current).await?;
        assert!(store.read(id).await?.delivered);
        // case: authoritative_read
        let mut readonly = store.clone();
        readonly
            .config
            .options("-c default_transaction_read_only=on");
        assert!(readonly.read(id).await.is_err());
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
        // case: crash_restart
        for phase in ["uncommitted", "started", "finished"] {
            let child_id = MessageId::random();
            let executable = std::env::current_exe()?;
            let exit = tokio::task::spawn_blocking(move || {
                std::process::Command::new(executable)
                    .args([
                        "--exact",
                        "tests::cases::postgres_crash_child",
                        "--ignored",
                        "--nocapture",
                    ])
                    .env("PLENORA_CRASH_PHASE", phase)
                    .env("PLENORA_CRASH_ID", child_id.to_string())
                    .status()
            })
            .await??;
            assert_eq!(exit.code(), Some(73));
            let fresh = PostgresStore::new(&database())?;
            if phase == "uncommitted" {
                assert_eq!(
                    fresh.accept_async(child_id, None).await,
                    Err(StoreError::Missing)
                );
            } else if phase == "started" {
                assert!(matches!(
                    fresh
                        .start(invocation(
                            child_id,
                            &child_id.to_string(),
                            "step",
                            Some("key"),
                            b"canonical input"
                        ))
                        .await?,
                    StartDecision::RequiresRecovery(_)
                ));
            } else {
                assert!(fresh.read(child_id).await?.finished);
                assert!(fresh.contains(child_id).await?);
            }
        }
        // case: tombstone
        admin.execute("UPDATE plenora_chain_acceptance SET expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", &[&id.to_string()]).await?;
        assert_eq!(
            store.accept_async(id, Some(CONTEXT)).await,
            Err(StoreError::Expired)
        );
        assert!(store.read(id).await?.delivery_expired);
        // case: retention_reduction
        store.migrate(Retention::reduced(60, 120)?).await?;
        assert_eq!(
            store.migrate(Retention::default()).await,
            Err(StoreError::Invalid)
        );
        admin.execute("UPDATE plenora_chain_steps SET finished_at=clock_timestamp()-interval '121 seconds' WHERE finished", &[]).await?;
        store.maintain().await?;
        let expired = store.read(id).await?;
        assert!(expired.report_expired);
        assert!(expired.report.is_none());
        let pending: i64=admin.query_one("SELECT count(*) FROM plenora_chain_steps s JOIN plenora_chain_outbox o ON o.id=s.id WHERE NOT o.delivered AND s.report IS NOT NULL", &[]).await?.try_get(0)?;
        assert_eq!(pending, 0);
        let expired_pending: i64 = admin.query_one("SELECT count(*) FROM plenora_chain_steps s JOIN plenora_chain_outbox o ON o.id=s.id WHERE NOT o.delivered AND s.report_expired", &[]).await?.try_get(0)?;
        assert!(expired_pending > 0);
        assert_eq!(
            store.accept_async(id, Some(CONTEXT)).await,
            Err(StoreError::Expired)
        );
        Ok(())
    }

    #[tokio::test]
    #[ignore = "child process crash helper; invoked by postgres_real"]
    async fn postgres_crash_child() -> Result<(), Box<dyn Error>> {
        let Ok(phase) = std::env::var("PLENORA_CRASH_PHASE") else {
            return Ok(());
        };
        let id = MessageId::from_str(&std::env::var("PLENORA_CRASH_ID")?)?;
        let store = PostgresStore::new(&database())?;
        if phase == "uncommitted" {
            let mut client = store.connect().await?;
            let tx = PostgresStore::transaction(&mut client).await?;
            tx.execute("INSERT INTO plenora_chain_acceptance(id,requires,downgrade,expires_at) VALUES ($1,'plenora-runtime-binding-v2',false,clock_timestamp()+interval '1 day')", &[&id.to_string()]).await?;
            std::process::exit(73);
        }
        store.accept_async(id, Some(CONTEXT)).await?;
        let permit = started(&store, id, &id.to_string()).await;
        if phase == "finished" {
            store
                .finish(
                    &permit,
                    MessageId::random(),
                    b"complete report",
                    b"complete envelope",
                )
                .await?;
        }
        std::process::exit(73);
    }
}
