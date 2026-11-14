//! Real PostgreSQL transition/race/migration/retention probes; never used as production storage.
#[cfg(test)]
mod cases {
    use crate::*;
    use plenora_runtime_boundary::{
        DeliveryChange, DeliveryJournal, DeliveryOpen, DeliveryPlan, DeliveryStage,
        DurableExecutionStore, PreparedResult, StoredMessage,
    };
    use plenora_runtime_capabilities::{
        PLENORA_ERROR_CONTENT_TYPE, PlenoraError, PlenoraErrorCategory, PlenoraErrorPhase,
        PlenoraErrorRemoteEffect, PlenoraErrorRetry, RuntimeBinding,
    };
    use plenora_runtime_messaging::SerializedMessage;
    use std::{error::Error, sync::Arc};
    fn prepared(j: &DeliveryJournal) -> Result<PreparedResult, Box<dyn Error>> {
        let error = PlenoraError::new(
            PlenoraErrorCategory::Internal,
            PlenoraErrorPhase::Cleanup,
            PlenoraErrorRemoteEffect::Committed,
            PlenoraErrorRetry::Never,
            "Fixture outcome.",
        )?;
        let mut message = SerializedMessage::new(PLENORA_ERROR_CONTENT_TYPE, error.to_json()?);
        for (k, v) in [
            ("plenora.message.id", j.result_id()),
            ("plenora.negotiation.requires", "plenora-runtime-binding-v2"),
            ("plenora.negotiation.accepts_downgrade", "false"),
        ] {
            message.headers.insert_text(k, v)?;
        }
        Ok(serde_json::from_value(
            serde_json::json!({"report":error,"terminal":StoredMessage::capture(&message)?,"retry_ms":null}),
        )?)
    }
    #[allow(
        clippy::too_many_lines,
        reason = "One isolated database lifecycle verifies migration, both authorities and retention"
    )]
    #[tokio::test]
    #[ignore = "requires disposable PostgreSQL; included by the postgres_real CI filter"]
    async fn postgres_real_delivery_state_and_legacy_exclusion() -> Result<(), Box<dyn Error>> {
        let store = Arc::new(PostgresStore::new(&std::env::var("PLENORA_POSTGRES_URL")?)?);
        store.migrate(Retention::default()).await?;
        let admin = store.connect().await?;
        assert_eq!(
            admin
                .query_one(
                    "SELECT version FROM plenora_chain_schema WHERE singleton",
                    &[]
                )
                .await?
                .get::<_, i32>(0),
            current_version()
        );
        let context = NegotiationContext {
            requires: RuntimeBinding::V2,
            accepts_downgrade: false,
        };
        let id = MessageId::random();
        let key = format!("13:delivery-test:m:{id}");
        let plan = DeliveryPlan::new("result-route", 1, 0, 4096, 1024)?;
        let DeliveryOpen::Opened(j, _) = store.open(&key, [1; 32], &plan).await? else {
            return Err("receipt".into());
        };
        store.accept_async(id, Some(context)).await?;
        let old = Invocation {
            message_id: id,
            scope: "legacy",
            step: "step",
            key: None,
            canonical_input: b"input",
        };
        assert!(matches!(store.start(old).await, Err(StoreError::Conflict)));
        let (a, b) = tokio::join!(
            store.change(&key, j.revision(), DeliveryChange::Start),
            store.change(&key, j.revision(), DeliveryChange::Start)
        );
        assert_ne!(a.is_ok(), b.is_ok());
        let (running, _) = store.load(&key).await?;
        assert_eq!(running.stage(), DeliveryStage::Running);
        store
            .change(
                &key,
                running.revision(),
                DeliveryChange::Evidence(prepared(&running)?),
            )
            .await?;
        let reopened = PostgresStore::new(&std::env::var("PLENORA_POSTGRES_URL")?)?;
        let (sealed, _) = reopened.load(&key).await?;
        assert_eq!(sealed.result_id(), j.result_id());
        let (finished, _) = reopened
            .change(&key, sealed.revision(), DeliveryChange::Settle)
            .await?;
        let (claimed, _) = reopened
            .change(&key, finished.revision(), DeliveryChange::Claim)
            .await?;
        let (released, _) = reopened
            .change(&key, claimed.revision(), DeliveryChange::Release)
            .await?;
        let (reclaimed, _) = reopened
            .change(&key, released.revision(), DeliveryChange::Claim)
            .await?;
        assert!(
            reopened
                .change(&key, claimed.revision(), DeliveryChange::Confirm)
                .await
                .is_err()
        );
        reopened
            .change(&key, reclaimed.revision(), DeliveryChange::Confirm)
            .await?;
        // Opposite authority ordering is also excluded.
        let legacy = MessageId::random();
        store.accept_async(legacy, Some(context)).await?;
        let StartDecision::Started(_) = store
            .start(Invocation {
                message_id: legacy,
                scope: "legacy-other",
                step: "step",
                key: None,
                canonical_input: b"input",
            })
            .await?
        else {
            return Err("legacy start".into());
        };
        assert!(matches!(
            store
                .open(&format!("13:delivery-test:m:{legacy}"), [2; 32], &plan)
                .await?,
            DeliveryOpen::Conflict
        ));
        // Report maintenance must remove inactive bytes without requiring a read.
        let past: i64 = admin
            .query_one(
                "SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint-7776000001",
                &[],
            )
            .await?
            .try_get(0)?;
        admin.execute("UPDATE plenora_chain_delivery SET created_ms=$2::bigint,journal=convert_to(jsonb_set(convert_from(journal,'UTF8')::jsonb,'{created}',to_jsonb($2::bigint))::text,'UTF8') WHERE key=$1",&[&key,&past]).await?;
        admin.execute("UPDATE plenora_chain_acceptance SET expires_at=clock_timestamp()-interval '1 second' WHERE id=$1",&[&j.result_id()]).await?;
        store.maintain().await?;
        let bytes: Vec<u8> = admin
            .query_one(
                "SELECT journal FROM plenora_chain_delivery WHERE key=$1",
                &[&key],
            )
            .await?
            .get(0);
        assert!(DeliveryJournal::from_json(&bytes)?.evidence().is_none());
        assert!(
            store
                .pending("13:delivery-test:", "result-route", 8)
                .await?
                .is_empty()
        );
        assert!(
            store
                .change(&key, reclaimed.revision(), DeliveryChange::Start)
                .await
                .is_err()
        );
        Ok(())
    }
    #[tokio::test]
    #[ignore = "requires disposable PostgreSQL; included by the postgres_real CI filter"]
    async fn postgres_real_retention_selects_metadata_before_decoding_and_preserves_corruption()
    -> Result<(), Box<dyn Error>> {
        let store = PostgresStore::new(&std::env::var("PLENORA_POSTGRES_URL")?)?;
        store.migrate(Retention::default()).await?;
        let admin = store.connect().await?;
        let key = format!("retention:{}", MessageId::random());
        let plan = DeliveryPlan::new("retention-route", 1, 0, 4096, 1024)?;
        let DeliveryOpen::Opened(journal, _) = store.open(&key, [9; 32], &plan).await? else {
            return Err("receipt".into());
        };
        let bytes = journal.to_json()?;
        // An unrelated live payload must not be decoded by global maintenance.
        let invalid = vec![0xff_u8];
        admin
            .execute(
                "UPDATE plenora_chain_delivery SET journal=$2 WHERE key=$1",
                &[&key, &invalid],
            )
            .await?;
        let maintenance = store.maintain().await;
        let rejected = store.load(&key).await.is_err();
        admin
            .execute(
                "UPDATE plenora_chain_delivery SET journal=$2 WHERE key=$1",
                &[&key, &bytes],
            )
            .await?;
        maintenance?;
        assert!(
            rejected,
            "the authoritative read must still reject corrupt bytes"
        );
        // A forged due scalar must fail before evidence can be erased.
        admin
            .execute(
                "UPDATE plenora_chain_delivery SET created_ms=0 WHERE key=$1",
                &[&key],
            )
            .await?;
        let rejected = store.maintain().await.is_err();
        let retained: Vec<u8> = admin
            .query_one(
                "SELECT journal FROM plenora_chain_delivery WHERE key=$1",
                &[&key],
            )
            .await?
            .try_get(0)?;
        admin
            .execute(
                "UPDATE plenora_chain_delivery SET created_ms=$2 WHERE key=$1",
                &[&key, &i64::try_from(journal.created_ms())?],
            )
            .await?;
        assert!(rejected);
        assert_eq!(retained, bytes, "failed maintenance must preserve evidence");
        store.maintain().await?;
        Ok(())
    }
}
