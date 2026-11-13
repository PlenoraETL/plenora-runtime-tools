//! Primary-backed owned byte provider for the declared library doubles.
use crate::PostgresStore;
use plenora_runtime_chains::{Error, Journal, ResourceLease, ResourceLedger, ResourceRequest};

fn unavailable(_: impl std::fmt::Debug) -> Error {
    Error::Storage
}
fn encode(ledger: &ResourceLedger) -> Result<Vec<u8>, Error> {
    ledger.to_json()
}
async fn now(tx: &tokio_postgres::Transaction<'_>) -> Result<u64, Error> {
    let row = tx
        .query_one(
            "SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint",
            &[],
        )
        .await
        .map_err(unavailable)?;
    u64::try_from(row.try_get::<_, i64>(0).map_err(unavailable)?).map_err(unavailable)
}
impl PostgresStore {
    pub(crate) async fn acquire_owned(
        &self,
        request: &ResourceRequest,
        recovery: bool,
    ) -> Result<ResourceLease, Error> {
        let mut client = self.connect().await.map_err(unavailable)?;
        let tx = Self::transaction(&mut client).await.map_err(unavailable)?;
        let now = now(&tx).await?;
        let row = tx.query_opt("SELECT r.journal,r.created_ms,p.report_seconds*1000 AS reports,p.delivery_seconds*1000 AS delivery FROM plenora_chain_runs r CROSS JOIN plenora_chain_schema p WHERE r.tenant=$1 AND r.run_id=$2", &[&request.tenant, &request.run_id]).await.map_err(unavailable)?.ok_or(Error::NotFound)?;
        let raw: Option<Vec<u8>> = row.try_get("journal").map_err(unavailable)?;
        let journal = Journal::from_json(&raw.ok_or(Error::Expired)?)?;
        journal.authorize_resources(request)?;
        let created = u64::try_from(row.try_get::<_, i64>("created_ms").map_err(unavailable)?)
            .map_err(unavailable)?;
        let reports = u64::try_from(row.try_get::<_, i64>("reports").map_err(unavailable)?)
            .map_err(unavailable)?;
        let retained_until = created.checked_add(reports).ok_or(Error::ResourceLimit)?;
        let delivery = u64::try_from(row.try_get::<_, i64>("delivery").map_err(unavailable)?)
            .map_err(unavailable)?;
        if !recovery && now >= created.checked_add(delivery).ok_or(Error::ResourceLimit)? {
            return Err(Error::Expired);
        }
        if now >= retained_until {
            return Err(Error::Expired);
        }
        let old = tx
            .query_opt(
                "SELECT ledger FROM plenora_chain_resources WHERE tenant=$1 AND run_id=$2",
                &[&request.tenant, &request.run_id],
            )
            .await
            .map_err(unavailable)?;
        let mut ledger = match old {
            Some(row) => {
                ResourceLedger::from_json(&row.try_get::<_, Vec<u8>>(0).map_err(unavailable)?)
                    .map_err(unavailable)?
            }
            None => ResourceLedger::new(&request.limits),
        };
        let lease = if recovery {
            ledger.reconcile(request, now, retained_until)?
        } else {
            ledger.acquire(request, now, retained_until)?
        };
        tx.execute("INSERT INTO plenora_chain_resources VALUES ($1,$2,$3,$4) ON CONFLICT(tenant,run_id) DO UPDATE SET ledger=excluded.ledger,next_check=excluded.next_check", &[&request.tenant, &request.run_id, &encode(&ledger)?, &i64::try_from(now).map_err(unavailable)?]).await.map_err(unavailable)?;
        Self::commit(tx).await.map_err(unavailable)?;
        Ok(lease)
    }
    pub(crate) async fn renew_owned(
        &self,
        tenant: &str,
        run: &str,
        lease: &ResourceLease,
    ) -> Result<ResourceLease, Error> {
        let mut client = self.connect().await.map_err(unavailable)?;
        let tx = Self::transaction(&mut client).await.map_err(unavailable)?;
        let now = now(&tx).await?;
        let row = tx
            .query_opt(
                "SELECT ledger FROM plenora_chain_resources WHERE tenant=$1 AND run_id=$2",
                &[&tenant, &run],
            )
            .await
            .map_err(unavailable)?
            .ok_or(Error::NotFound)?;
        let mut ledger: ResourceLedger =
            ResourceLedger::from_json(&row.try_get::<_, Vec<u8>>(0).map_err(unavailable)?)
                .map_err(unavailable)?;
        let lease = ledger.renew(lease, now)?;
        tx.execute(
            "UPDATE plenora_chain_resources SET ledger=$3 WHERE tenant=$1 AND run_id=$2",
            &[&tenant, &run, &encode(&ledger)?],
        )
        .await
        .map_err(unavailable)?;
        Self::commit(tx).await.map_err(unavailable)?;
        Ok(lease)
    }
    pub(crate) async fn read_owned(
        &self,
        tenant: &str,
        run: &str,
        reference: &str,
        limit: usize,
    ) -> Result<Vec<u8>, Error> {
        let mut client = self.connect().await.map_err(unavailable)?;
        let tx = Self::transaction(&mut client).await.map_err(unavailable)?;
        let instant = now(&tx).await?;
        let row = tx.query_opt("SELECT o.ledger,r.created_ms+p.report_seconds*1000 AS expires FROM plenora_chain_resources o JOIN plenora_chain_runs r ON r.tenant=o.tenant AND r.run_id=o.run_id CROSS JOIN plenora_chain_schema p WHERE o.tenant=$1 AND o.run_id=$2", &[&tenant,&run]).await.map_err(unavailable)?.ok_or(Error::NotFound)?;
        let cutoff =
            u64::try_from(row.try_get::<_, i64>(1).map_err(unavailable)?).map_err(unavailable)?;
        if instant >= cutoff {
            return Err(Error::Expired);
        }
        let ledger =
            ResourceLedger::from_json(&row.try_get::<_, Vec<u8>>(0).map_err(unavailable)?)?;
        let bytes = ledger.artifact(reference, instant)?;
        if bytes.len() > limit {
            return Err(Error::ResourceLimit);
        }
        let output = bytes.to_vec();
        Self::commit(tx).await.map_err(unavailable)?;
        Ok(output)
    }
    pub(crate) async fn snapshot_owned(
        &self,
        tenant: &str,
        run: &str,
    ) -> Result<Option<ResourceLedger>, Error> {
        let mut client = self.connect().await.map_err(unavailable)?;
        let tx = Self::transaction(&mut client).await.map_err(unavailable)?;
        let row = tx
            .query_opt(
                "SELECT ledger FROM plenora_chain_resources WHERE tenant=$1 AND run_id=$2",
                &[&tenant, &run],
            )
            .await
            .map_err(unavailable)?;
        let ledger = row
            .map(|row| {
                ResourceLedger::from_json(&row.try_get::<_, Vec<u8>>(0).map_err(unavailable)?)
                    .map_err(unavailable)
            })
            .transpose()?;
        Self::commit(tx).await.map_err(unavailable)?;
        Ok(ledger)
    }
    pub(crate) async fn sync_owned(
        tx: &tokio_postgres::Transaction<'_>,
        journal: &Journal,
        now: u64,
    ) -> Result<(), Error> {
        let tenant = journal.tenant();
        let run = journal.run_id();
        if let Some(row) = tx
            .query_opt(
                "SELECT ledger FROM plenora_chain_resources WHERE tenant=$1 AND run_id=$2",
                &[&tenant, &run],
            )
            .await
            .map_err(unavailable)?
        {
            let mut ledger: ResourceLedger =
                ResourceLedger::from_json(&row.try_get::<_, Vec<u8>>(0).map_err(unavailable)?)
                    .map_err(unavailable)?;
            ledger.observe(journal)?;
            ledger.cleanup(now);
            tx.execute("UPDATE plenora_chain_resources SET ledger=$3,next_check=$4 WHERE tenant=$1 AND run_id=$2", &[&tenant,&run,&encode(&ledger)?,&i64::try_from(now).map_err(unavailable)?]).await.map_err(unavailable)?;
        }
        Ok(())
    }
    pub(crate) async fn cleanup_owned(&self, tenant: &str) -> Result<(), Error> {
        let mut client = self.connect().await.map_err(unavailable)?;
        let tx = Self::transaction(&mut client).await.map_err(unavailable)?;
        Self::sweep_owned(&tx, Some(tenant)).await?;
        Self::commit(tx).await.map_err(unavailable)
    }
    pub(crate) async fn sweep_owned(
        tx: &tokio_postgres::Transaction<'_>,
        scope: Option<&str>,
    ) -> Result<(), Error> {
        let now = now(tx).await?;
        // Discover keys only: never materialize a batch of large provider payloads together.
        let keys = tx.query("SELECT tenant,run_id FROM plenora_chain_resources WHERE ($1::text IS NULL OR tenant=$1) AND next_check <= $2 ORDER BY next_check,tenant,run_id LIMIT 32", &[&scope,&i64::try_from(now).map_err(unavailable)?]).await.map_err(unavailable)?;
        for key in keys {
            let tenant: String = key.try_get(0).map_err(unavailable)?;
            let run: String = key.try_get(1).map_err(unavailable)?;
            let row = tx.query_one("SELECT o.ledger,r.created_ms + p.report_seconds*1000 AS expires FROM plenora_chain_resources o JOIN plenora_chain_runs r ON r.tenant=o.tenant AND r.run_id=o.run_id CROSS JOIN plenora_chain_schema p WHERE o.tenant=$1 AND o.run_id=$2", &[&tenant,&run]).await.map_err(unavailable)?;
            let mut ledger: ResourceLedger =
                ResourceLedger::from_json(&row.try_get::<_, Vec<u8>>(0).map_err(unavailable)?)
                    .map_err(unavailable)?;
            let expires = u64::try_from(row.try_get::<_, i64>(1).map_err(unavailable)?)
                .map_err(unavailable)?;
            ledger.reduce_retention(expires);
            ledger.cleanup(now);
            let next = if ledger.live_bytes() == 0 && ledger.active() == 0 {
                i64::MAX
            } else {
                i64::try_from(now.checked_add(1000).ok_or(Error::ResourceLimit)?)
                    .map_err(unavailable)?
            };
            tx.execute("UPDATE plenora_chain_resources SET ledger=$3,next_check=$4 WHERE tenant=$1 AND run_id=$2", &[&tenant,&run,&encode(&ledger)?,&next]).await.map_err(unavailable)?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "resources_tests.rs"]
mod tests;
