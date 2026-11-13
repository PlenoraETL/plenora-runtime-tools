//! Primary transactions for chain snapshots, scoped identities and permanent tombstones.
use crate::PostgresStore;
use async_trait::async_trait;
use plenora_runtime_chains::{Error, Journal, Store};
use tokio_postgres::{Row, Transaction};

fn unavailable(_: impl std::fmt::Debug) -> Error {
    Error::Storage
}
async fn policy(tx: &Transaction<'_>) -> Result<(u64, u64, u64), Error> {
    let row = tx.query_one("SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint, delivery_seconds*1000, report_seconds*1000 FROM plenora_chain_schema WHERE singleton", &[]).await.map_err(unavailable)?;
    let get =
        |i| u64::try_from(row.try_get::<_, i64>(i).map_err(unavailable)?).map_err(unavailable);
    Ok((get(0)?, get(1)?, get(2)?))
}
fn restore(row: &Row, now: u64, reports: u64) -> Result<Journal, Error> {
    let created = u64::try_from(row.try_get::<_, i64>("created_ms").map_err(unavailable)?)
        .map_err(unavailable)?;
    if now < created {
        return Err(Error::Storage);
    }
    if now - created >= reports {
        return Err(Error::Expired);
    }
    let bytes = row
        .try_get::<_, Option<Vec<u8>>>("journal")
        .map_err(unavailable)?
        .ok_or(Error::Expired)?;
    let j = Journal::from_json(&bytes)?;
    if j.created_ms() != created
        || i64::try_from(j.revision()).map_err(unavailable)?
            != row.try_get::<_, i64>("revision").map_err(unavailable)?
    {
        return Err(Error::Storage);
    }
    Ok(j)
}
async fn expire(tx: &Transaction<'_>, now: u64, reports: u64) -> Result<(), Error> {
    tx.execute("UPDATE plenora_chain_runs SET journal=NULL WHERE journal IS NOT NULL AND created_ms <= $1::bigint - $2::bigint", &[&i64::try_from(now).map_err(unavailable)?, &i64::try_from(reports).map_err(unavailable)?]).await.map_err(unavailable)?;
    Ok(())
}
#[async_trait]
impl Store for PostgresStore {
    async fn claim_work(
        &self,
        tenant: &str,
        owner: &str,
        lease_ms: u64,
    ) -> Result<Option<plenora_runtime_chains::RunLease>, Error> {
        if !(1..=60_000).contains(&lease_ms) {
            return Err(Error::Invalid);
        }
        let mut client = self.connect().await.map_err(unavailable)?;
        let tx = Self::transaction(&mut client).await.map_err(unavailable)?;
        let (now, delivery, reports) = policy(&tx).await?;
        expire(&tx, now, reports).await?;
        let rows = tx.query("SELECT * FROM plenora_chain_runs WHERE tenant=$1 AND journal IS NOT NULL AND work_until <= $2 AND created_ms <= $2 AND created_ms > $2 - $3 AND NOT (convert_from(journal,'UTF8')::jsonb->>'terminal')::boolean AND NOT COALESCE((convert_from(journal,'UTF8')::jsonb->>'waiting_recovery')::boolean,false) AND (COALESCE((convert_from(journal,'UTF8')::jsonb->>'retry_due')::bigint,0) <= $2 OR (convert_from(journal,'UTF8')::jsonb->>'cancelled')::boolean OR (convert_from(journal,'UTF8')::jsonb->>'deadline')::bigint <= $2) ORDER BY work_until,created_ms,run_id LIMIT 1", &[&tenant, &i64::try_from(now).map_err(unavailable)?, &i64::try_from(delivery).map_err(unavailable)?]).await.map_err(unavailable)?;
        let result = if let Some(row) = rows.first() {
            let journal = restore(row, now, reports)?;
            if !journal.work_due(now) {
                return Err(Error::Storage);
            }
            let token = u64::try_from(row.try_get::<_, i64>("work_fence").map_err(unavailable)?)
                .map_err(unavailable)?
                .checked_add(1)
                .ok_or(Error::ResourceLimit)?;
            let lease = plenora_runtime_chains::RunLease::new(
                tenant,
                journal.run_id(),
                owner,
                token,
                now.checked_add(lease_ms).ok_or(Error::ResourceLimit)?,
            )?;
            tx.execute("UPDATE plenora_chain_runs SET work_owner=$3,work_fence=$4,work_until=$5 WHERE tenant=$1 AND run_id=$2", &[&tenant, &journal.run_id(), &owner, &i64::try_from(token).map_err(unavailable)?, &i64::try_from(lease.until()).map_err(unavailable)?]).await.map_err(unavailable)?;
            Some(lease)
        } else {
            None
        };
        Self::commit(tx).await.map_err(unavailable)?;
        Ok(result)
    }
    async fn release_work(&self, lease: &plenora_runtime_chains::RunLease) -> Result<(), Error> {
        let mut client = self.connect().await.map_err(unavailable)?;
        let tx = Self::transaction(&mut client).await.map_err(unavailable)?;
        let count = tx.execute("UPDATE plenora_chain_runs SET work_owner=NULL,work_until=0 WHERE tenant=$1 AND run_id=$2 AND work_owner=$3 AND work_fence=$4", &[&lease.tenant(), &lease.run_id(), &lease.owner(), &i64::try_from(lease.token()).map_err(unavailable)?]).await.map_err(unavailable)?;
        if count != 1 {
            return Err(Error::Conflict);
        }
        Self::commit(tx).await.map_err(unavailable)
    }

    async fn lookup(
        &self,
        tenant: &str,
        run_id: &str,
        key: Option<&str>,
    ) -> Result<Option<Journal>, Error> {
        let mut client = self.connect().await.map_err(unavailable)?;
        let tx = Self::transaction(&mut client).await.map_err(unavailable)?;
        let (now, delivery, reports) = policy(&tx).await?;
        expire(&tx, now, reports).await?;
        let rows = tx.query("SELECT * FROM plenora_chain_runs WHERE tenant=$1 AND (run_id=$2 OR (operation='chain.submit@2' AND idempotency_key=$3))", &[&tenant, &run_id, &key]).await.map_err(unavailable)?;
        let result = match rows.as_slice() {
            [] => Ok(None),
            [row] => restore(row, now, reports).and_then(|j| {
                if now - j.created_ms() >= delivery {
                    Err(Error::Expired)
                } else {
                    Ok(Some(j))
                }
            }),
            _ => Err(Error::Conflict),
        };
        Self::commit(tx).await.map_err(unavailable)?;
        result
    }
    async fn create(
        &self,
        candidate: plenora_runtime_chains::Submission,
    ) -> Result<Journal, Error> {
        let candidate = candidate.into_journal();
        let bytes = candidate.to_json()?;
        let mut client = self.connect().await.map_err(unavailable)?;
        let tx = Self::transaction(&mut client).await.map_err(unavailable)?;
        let (now, delivery, reports) = policy(&tx).await?;
        expire(&tx, now, reports).await?;
        let rows = tx.query("SELECT * FROM plenora_chain_runs WHERE tenant=$1 AND (run_id=$2 OR (operation='chain.submit@2' AND idempotency_key=$3))", &[&candidate.tenant(), &candidate.run_id(), &candidate.idempotency_key()]).await.map_err(unavailable)?;
        if let Some(row) = rows.first() {
            let old = restore(row, now, reports)?;
            if now - old.created_ms() >= delivery {
                return Err(Error::Expired);
            }
            if rows.len() != 1 || !old.same_submission(&candidate) {
                return Err(Error::Conflict);
            }
            Self::commit(tx).await.map_err(unavailable)?;
            return Ok(old);
        }
        if candidate.created_ms() > now || now - candidate.created_ms() >= delivery {
            return Err(Error::Expired);
        }
        tx.execute("INSERT INTO plenora_chain_runs (tenant,run_id,operation,idempotency_key,revision,created_ms,journal) VALUES ($1,$2,'chain.submit@2',$3,$4,$5,$6)", &[&candidate.tenant(), &candidate.run_id(), &candidate.idempotency_key(), &i64::try_from(candidate.revision()).map_err(unavailable)?, &i64::try_from(candidate.created_ms()).map_err(unavailable)?, &bytes]).await.map_err(unavailable)?;
        Self::commit(tx).await.map_err(unavailable)?;
        Ok(candidate)
    }
    async fn load(&self, tenant: &str, run_id: &str) -> Result<Journal, Error> {
        let mut client = self.connect().await.map_err(unavailable)?;
        let tx = Self::transaction(&mut client).await.map_err(unavailable)?;
        let (now, _, reports) = policy(&tx).await?;
        expire(&tx, now, reports).await?;
        let row = tx
            .query_opt(
                "SELECT * FROM plenora_chain_runs WHERE tenant=$1 AND run_id=$2",
                &[&tenant, &run_id],
            )
            .await
            .map_err(unavailable)?
            .ok_or(Error::NotFound)?;
        let result = restore(&row, now, reports);
        Self::commit(tx).await.map_err(unavailable)?;
        result
    }
    async fn compare_exchange(
        &self,
        expected: u64,
        next: plenora_runtime_chains::Change,
    ) -> Result<(), Error> {
        let lease = next.lease().cloned();
        let next = next.into_journal();
        let bytes = next.to_json()?;
        let mut client = self.connect().await.map_err(unavailable)?;
        let tx = Self::transaction(&mut client).await.map_err(unavailable)?;
        let (now, delivery, reports) = policy(&tx).await?;
        expire(&tx, now, reports).await?;
        let row = tx
            .query_opt(
                "SELECT * FROM plenora_chain_runs WHERE tenant=$1 AND run_id=$2",
                &[&next.tenant(), &next.run_id()],
            )
            .await
            .map_err(unavailable)?
            .ok_or(Error::NotFound)?;
        let old = restore(&row, now, reports)?;
        let until = u64::try_from(row.try_get::<_, i64>("work_until").map_err(unavailable)?)
            .map_err(unavailable)?;
        let fence = u64::try_from(row.try_get::<_, i64>("work_fence").map_err(unavailable)?)
            .map_err(unavailable)?;
        let owner: Option<String> = row.try_get("work_owner").map_err(unavailable)?;
        match lease {
            Some(lease)
                if lease.tenant() == next.tenant()
                    && lease.run_id() == next.run_id()
                    && lease.token() == fence
                    && owner.as_deref() == Some(lease.owner())
                    && now < until => {}
            None if until <= now || old.is_cancel_successor(&next) => {}
            _ => return Err(Error::Conflict),
        }
        old.validate_successor(&next)?;
        if now - old.created_ms() >= delivery && next.starts_invocation_after(&old) {
            return Err(Error::Expired);
        }
        if old.revision() != expected
            || next.revision() != expected.checked_add(1).ok_or(Error::Conflict)?
            || !old.same_submission(&next)
            || old.created_ms() != next.created_ms()
        {
            return Err(Error::Conflict);
        }
        let changed = tx.execute("UPDATE plenora_chain_runs SET revision=$3,journal=$4 WHERE tenant=$1 AND run_id=$2 AND revision=$5", &[&next.tenant(), &next.run_id(), &i64::try_from(next.revision()).map_err(unavailable)?, &bytes, &i64::try_from(expected).map_err(unavailable)?]).await.map_err(unavailable)?;
        if changed != 1 {
            return Err(Error::Conflict);
        }
        Self::commit(tx).await.map_err(unavailable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use plenora_runtime_chains::{
        Adapter, ChainEngine, Invocation, RunOptions,
        contracts::{StepResult, parse_plan, parse_registry},
    };
    use plenora_runtime_testkit::ManualClock;
    use std::{
        error::Error as StdError,
        sync::Arc,
        time::{Duration, UNIX_EPOCH},
    };

    struct NoInvocation;
    #[async_trait]
    impl Adapter for NoInvocation {
        // Declared store-only fake: authorization is explicit and no domain can run.
        async fn authorize(
            &self,
            _: &str,
            _: &plenora_runtime_chains::contracts::Plan,
        ) -> Result<(), Error> {
            Ok(())
        }
        async fn prepare(&self, _: &Invocation) -> Result<(), Error> {
            Err(Error::Unsupported)
        }
        async fn invoke(&self, _: &Invocation) -> Result<StepResult, Error> {
            Err(Error::Recovery)
        }
        async fn request_cancel(&self, _: &Invocation) -> Result<(), Error> {
            Ok(())
        }
        async fn recover(&self, _: &Invocation) -> Result<Option<StepResult>, Error> {
            Ok(None)
        }
    }

    #[tokio::test]
    #[ignore = "Requires the ephemeral PostgreSQL job; no in-memory replacement"]
    async fn postgres_real_chain_scope_race_restart_and_revision() -> Result<(), Box<dyn StdError>>
    {
        let store = Arc::new(PostgresStore::new(&std::env::var("PLENORA_POSTGRES_URL")?)?);
        store.migrate(crate::Retention::default()).await?;
        let mut connection = store.connect().await?;
        let tx = PostgresStore::transaction(&mut connection).await?;
        let (now, _, _) = policy(&tx).await?;
        PostgresStore::commit(tx).await?;
        let clock = Arc::new(ManualClock::new(UNIX_EPOCH + Duration::from_millis(now)));
        let registry = parse_registry(include_bytes!(
            "../../../contracts/chains-v2/fixtures/context.json"
        ))?;
        let plan = parse_plan(include_bytes!(
            "../../../contracts/chains-v2/fixtures/plan-valid.json"
        ))?;
        let tenant = uuid::Uuid::new_v4().to_string();
        let a = ChainEngine::new(
            store.clone(),
            Arc::new(NoInvocation),
            registry.clone(),
            clock.clone(),
            &tenant,
        )?;
        let b = ChainEngine::new(
            store.clone(),
            Arc::new(NoInvocation),
            registry,
            clock.clone(),
            &tenant,
        )?;
        let options = || RunOptions {
            run_id: uuid::Uuid::new_v4().to_string(),
            idempotency_key: Some("same-key".into()),
            deadline_ms: Some(now + 10),
        };
        let (left, right) = tokio::join!(a.submit(&plan, options()), b.submit(&plan, options()));
        let left = left?;
        assert_eq!(left, right?);
        let cancelled = b.cancel(&left.run_id, left.revision).await?;
        assert_eq!(
            a.cancel(&left.run_id, left.revision).await,
            Err(Error::Conflict)
        );
        assert_eq!(
            a.resume(&left.run_id, cancelled.revision).await?.state,
            "cancelled"
        );
        assert_eq!(b.result(&left.run_id).await?.state, "cancelled");
        clock.advance(Duration::from_millis(11))?;
        assert_eq!(b.submit(&plan, options()).await?.run_id, left.run_id);
        let mut fresh = options();
        fresh.idempotency_key = Some("new-after-deadline".into());
        assert_eq!(a.submit(&plan, fresh).await, Err(Error::Timeout));
        let mut different = plan.clone();
        different.revision += 1;
        assert_eq!(a.submit(&different, options()).await, Err(Error::Conflict));
        Ok(())
    }
}
