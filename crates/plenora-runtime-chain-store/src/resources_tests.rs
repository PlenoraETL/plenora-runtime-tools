//! Real-primary ownership tests; the domain provider is explicitly a byte-buffer double.
#[cfg(test)]
mod cases {
    use super::super::*;
    use async_trait::async_trait;
    use plenora_runtime_chains::{
        Adapter, ChainEngine, Invocation, OwnedResource, ResourceRequest, RunOptions, Store,
        contracts::{StepResult, parse_plan, parse_registry, parse_result},
    };
    use sha2::Digest;
    use std::{error::Error as StdError, sync::Arc};

    struct Buffers(Arc<PostgresStore>);
    #[async_trait]
    impl Adapter for Buffers {
        async fn authorize(
            &self,
            _: &str,
            _: &plenora_runtime_chains::contracts::Plan,
        ) -> Result<(), Error> {
            Ok(())
        }
        async fn prepare(&self, _: &Invocation) -> Result<(), Error> {
            Ok(())
        }
        async fn invoke(&self, i: &Invocation) -> Result<StepResult, Error> {
            let plan = parse_plan(include_bytes!(
                "../../../contracts/chains-v2/fixtures/plan-valid.json"
            ))?;
            let request = ResourceRequest::new(
                i,
                plan.budgets,
                vec![
                    OwnedResource {
                        reference: "ref:read".into(),
                        bytes: vec![0, 1, 2, 3],
                        consumers: vec!["transform".into()],
                        temporary: false,
                    },
                    OwnedResource {
                        reference: "ref:temporary".into(),
                        bytes: vec![4, 5],
                        consumers: vec![],
                        temporary: true,
                    },
                ],
            );
            let lease = self.0.acquire_resources(&request).await?;
            let renewed = self.0.renew_resources(&i.tenant, &i.run_id, &lease).await?;
            assert!(renewed.until >= lease.until);
            let mut snapshot = serde_json::to_value(
                self.0
                    .resource_snapshot(&i.tenant, &i.run_id)
                    .await?
                    .ok_or(Error::Storage)?,
            )
            .map_err(|_| Error::Invalid)?;
            snapshot["attempts"][&i.message_id]["until"] = serde_json::json!(0);
            let client = self.0.connect().await.map_err(unavailable)?;
            client
                .execute(
                    "UPDATE plenora_chain_resources SET ledger=$3 WHERE tenant=$1 AND run_id=$2",
                    &[
                        &i.tenant,
                        &i.run_id,
                        &serde_json::from_value::<ResourceLedger>(snapshot)
                            .map_err(|_| Error::Invalid)?
                            .to_json()?,
                    ],
                )
                .await
                .map_err(unavailable)?;
            assert!(matches!(
                self.0.renew_resources(&i.tenant, &i.run_id, &lease).await,
                Err(Error::Conflict)
            ));
            self.0.cleanup_resources(&i.tenant).await?;
            assert_eq!(
                self.0
                    .resource_snapshot(&i.tenant, &i.run_id)
                    .await?
                    .ok_or(Error::Storage)?
                    .live_bytes(),
                6
            );
            let mut result = parse_result(include_bytes!(
                "../../../contracts/chains-v2/fixtures/result-valid.json"
            ))?
            .steps
            .into_iter()
            .find(|s| s.step_id == i.step.id)
            .ok_or(Error::Invalid)?;
            let output = result.outputs.get_mut("dataset").ok_or(Error::Invalid)?;
            output.bytes = 4;
            output.sha256 = format!("{:x}", sha2::Sha256::digest([0, 1, 2, 3]));
            result.counters.artifact_bytes = 4;
            result.counters.memory_bytes = 4;
            result.counters.spool_bytes = 4;
            Ok(result) // Authenticated late stop proof is still valid after lease expiry.
        }
        async fn request_cancel(&self, _: &Invocation) -> Result<(), Error> {
            Ok(())
        }
        async fn recover(&self, _: &Invocation) -> Result<Option<StepResult>, Error> {
            Ok(None)
        }
    }

    #[tokio::test]
    #[ignore = "Requires real PostgreSQL; executed by the existing postgres_real CI job"]
    async fn postgres_real_owned_buffers_renew_expire_cleanup_and_preserve_tombstones()
    -> Result<(), Box<dyn StdError>> {
        let store = Arc::new(PostgresStore::new(&std::env::var("PLENORA_POSTGRES_URL")?)?);
        store.migrate(crate::Retention::default()).await?;
        let tenant = uuid::Uuid::new_v4().to_string();
        let engine = ChainEngine::new(
            store.clone(),
            Arc::new(Buffers(store.clone())),
            parse_registry(include_bytes!(
                "../../../contracts/chains-v2/fixtures/context.json"
            ))?,
            Arc::new(plenora_runtime_core::SystemClock),
            &tenant,
        )?;
        let plan = parse_plan(include_bytes!(
            "../../../contracts/chains-v2/fixtures/plan-valid.json"
        ))?;
        let run = engine
            .submit(
                &plan,
                RunOptions {
                    run_id: uuid::Uuid::new_v4().to_string(),
                    idempotency_key: Some("owned-once".into()),
                    deadline_ms: None,
                },
            )
            .await?;
        check_worker_lease(&store, &tenant, &run.run_id).await?;
        let client = store.connect().await?;
        engine.advance(&run.run_id).await?;
        let before = store.load(&tenant, &run.run_id).await?.to_json()?;
        let ledger = store
            .resource_snapshot(&tenant, &run.run_id)
            .await?
            .ok_or("resources")?;
        assert_eq!(ledger.active(), 0);
        assert_eq!(ledger.live_bytes(), 4);
        let mut expired = serde_json::to_value(&ledger)?;
        for object in expired["objects"]
            .as_object_mut()
            .ok_or("objects")?
            .values_mut()
        {
            object["expires"] = serde_json::json!(0);
        }
        client.execute("UPDATE plenora_chain_resources SET ledger=$3,next_check=0 WHERE tenant=$1 AND run_id=$2", &[&tenant,&run.run_id,&serde_json::from_value::<ResourceLedger>(expired)?.to_json()?]).await?;
        let restarted = PostgresStore::new(&std::env::var("PLENORA_POSTGRES_URL")?)?;
        assert_eq!(
            restarted
                .read_resource(&tenant, &run.run_id, "ref:read", 1024)
                .await,
            Err(Error::Expired)
        );
        assert_eq!(
            restarted
                .resource_snapshot(&tenant, &run.run_id)
                .await?
                .ok_or("physical evidence")?
                .live_bytes(),
            4
        );
        restarted.cleanup_resources(&tenant).await?;
        restarted.cleanup_resources(&tenant).await?;
        assert_eq!(
            restarted
                .resource_snapshot(&tenant, &run.run_id)
                .await?
                .ok_or("ownership tombstone")?
                .live_bytes(),
            0
        );
        assert_eq!(
            restarted.load(&tenant, &run.run_id).await?.to_json()?,
            before
        );
        assert_eq!(engine.report(&run.run_id).await?.len(), 1);
        client
            .execute(
                "UPDATE plenora_chain_runs SET journal=NULL WHERE tenant=$1 AND run_id=$2",
                &[&tenant, &run.run_id],
            )
            .await?;
        restarted.maintain().await?;
        assert!(
            restarted
                .resource_snapshot(&tenant, &run.run_id)
                .await?
                .is_some()
        );
        assert!(matches!(
            restarted
                .lookup(&tenant, &run.run_id, Some("owned-once"))
                .await,
            Err(Error::Expired)
        ));
        Ok(())
    }

    /// Primary-clock expiry and takeover keep the old fence invalid.
    async fn check_worker_lease(
        store: &PostgresStore,
        tenant: &str,
        run: &str,
    ) -> Result<(), Box<dyn StdError>> {
        let lease = store
            .claim_work(tenant, &uuid::Uuid::new_v4().to_string(), 30_000)
            .await?
            .ok_or("lease")?;
        let renewed = store.renew_work(&lease, 30_000).await?;
        assert_eq!(renewed.token(), lease.token());
        let client = store.connect().await?;
        client
            .execute(
                "UPDATE plenora_chain_runs SET work_until=0 WHERE tenant=$1 AND run_id=$2",
                &[&tenant, &run],
            )
            .await?;
        assert_eq!(store.renew_work(&lease, 30_000).await, Err(Error::Conflict));
        let next = store
            .claim_work(tenant, &uuid::Uuid::new_v4().to_string(), 30_000)
            .await?
            .ok_or("takeover")?;
        assert!(next.token() > lease.token());
        assert_eq!(store.release_work(&lease).await, Err(Error::Conflict));
        store.release_work(&next).await?;
        Ok(())
    }
}
