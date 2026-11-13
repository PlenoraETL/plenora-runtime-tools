//! Product state transitions share primary verification, serialization and retention policy.
use crate::{PostgresStore, StoreError};
use async_trait::async_trait;
use plenora_runtime_boundary::{
    DeliveryChange, DeliveryJournal, DeliveryOpen, DeliveryPlan, DeliveryStage,
    DurableExecutionStore, IdempotencyOpen, IdempotencyReceipt, SettlementError,
};
use tokio_postgres::Transaction;
fn failure(_: impl std::fmt::Debug) -> SettlementError {
    SettlementError
}
fn valid_key(key: &str) -> Result<(), SettlementError> {
    if key.is_empty() || key.len() > 256 || !key.bytes().all(|b| b.is_ascii_graphic()) {
        Err(SettlementError)
    } else {
        Ok(())
    }
}
async fn policy(tx: &Transaction<'_>) -> Result<(u64, u64, u64), StoreError> {
    let row=tx.query_one("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint,delivery_seconds*1000,report_seconds*1000 FROM plenora_chain_schema WHERE singleton",&[]).await?;
    Ok((
        u64::try_from(row.try_get::<_, i64>(0)?).map_err(|_| StoreError::Invalid)?,
        u64::try_from(row.try_get::<_, i64>(1)?).map_err(|_| StoreError::Invalid)?,
        u64::try_from(row.try_get::<_, i64>(2)?).map_err(|_| StoreError::Invalid)?,
    ))
}
async fn read(
    tx: &Transaction<'_>,
    key: &str,
    now: u64,
    delivery: u64,
    reports: u64,
) -> Result<DeliveryJournal, SettlementError> {
    let row = tx
        .query_opt(
            "SELECT journal FROM plenora_chain_delivery WHERE key=$1",
            &[&key],
        )
        .await
        .map_err(failure)?
        .ok_or(SettlementError)?;
    let mut j = DeliveryJournal::from_json(&row.try_get::<_, Vec<u8>>(0).map_err(failure)?)?;
    j.maintain(now, delivery, reports)?;
    Ok(j)
}
async fn write(
    tx: &Transaction<'_>,
    key: &str,
    j: &DeliveryJournal,
) -> Result<(), SettlementError> {
    let stage = match j.stage() {
        DeliveryStage::Ready => "ready",
        DeliveryStage::Running => "running",
        DeliveryStage::Evidence => "evidence",
        DeliveryStage::Finished => "finished",
    };
    let bytes = j.to_json()?;
    tx.execute(
        "UPDATE plenora_chain_delivery SET journal=$2,stage=$3,delivered=$4 WHERE key=$1",
        &[&key, &bytes, &stage, &j.delivered()],
    )
    .await
    .map_err(failure)?;
    Ok(())
}
#[async_trait]
impl DurableExecutionStore for PostgresStore {
    async fn idempotency(
        &self,
        key: &str,
        binding: [u8; 32],
        candidate: Option<IdempotencyReceipt>,
    ) -> Result<IdempotencyOpen, SettlementError> {
        valid_key(key)?;
        let mut client = self.connect().await.map_err(failure)?;
        let tx = Self::transaction(&mut client).await.map_err(failure)?;
        Self::expire(&tx).await.map_err(failure)?;
        let (now, delivery, _) = policy(&tx).await.map_err(failure)?;
        let existing = tx
            .query_opt(
                "SELECT binding, created_ms, carrier FROM plenora_chain_idempotency WHERE key=$1",
                &[&key],
            )
            .await
            .map_err(failure)?;
        let result = if let Some(row) = existing {
            let created =
                u64::try_from(row.try_get::<_, i64>(1).map_err(failure)?).map_err(failure)?;
            if now < created || now - created >= delivery {
                return Err(SettlementError);
            }
            if row.try_get::<_, Vec<u8>>(0).map_err(failure)? == binding {
                let bytes = row
                    .try_get::<_, Option<Vec<u8>>>(2)
                    .map_err(failure)?
                    .ok_or(SettlementError)?;
                IdempotencyOpen::Selected(serde_json::from_slice(&bytes).map_err(failure)?)
            } else {
                IdempotencyOpen::Conflict
            }
        } else if let Some(candidate) = candidate {
            let bytes = serde_json::to_vec(&candidate).map_err(failure)?;
            if bytes.len() > 8 * 1024 * 1024 {
                return Err(SettlementError);
            }
            tx.execute("INSERT INTO plenora_chain_idempotency(key,binding,created_ms,carrier) VALUES ($1,$2,$3,$4)", &[&key,&binding.as_slice(),&i64::try_from(now).map_err(failure)?,&bytes]).await.map_err(failure)?;
            IdempotencyOpen::Selected(candidate)
        } else {
            IdempotencyOpen::Absent
        };
        Self::commit(tx).await.map_err(failure)?;
        Ok(result)
    }
    async fn open(
        &self,
        key: &str,
        binding: [u8; 32],
        plan: &DeliveryPlan,
    ) -> Result<DeliveryOpen, SettlementError> {
        valid_key(key)?;
        let mut client = self.connect().await.map_err(failure)?;
        let tx = Self::transaction(&mut client).await.map_err(failure)?;
        Self::expire(&tx).await.map_err(failure)?;
        let (now, delivery, reports) = policy(&tx).await.map_err(failure)?;
        let exists = tx
            .query_opt(
                "SELECT key FROM plenora_chain_delivery WHERE key=$1",
                &[&key],
            )
            .await
            .map_err(failure)?
            .is_some();
        let j = if exists {
            let j = read(&tx, key, now, delivery, reports).await?;
            if j.verify(binding, plan).is_err() {
                Self::commit(tx).await.map_err(failure)?;
                return Ok(DeliveryOpen::Conflict);
            }
            write(&tx, key, &j).await?;
            j
        } else {
            if let Some(request_id) = request_identity(key)
                && tx.query_opt("SELECT result_id FROM plenora_chain_delivery WHERE result_id=$1 OR request_id=$1 UNION ALL SELECT result_id FROM plenora_chain_steps WHERE result_id=$1 OR id=$1",&[&request_id]).await.map_err(failure)?.is_some(){Self::commit(tx).await.map_err(failure)?;return Ok(DeliveryOpen::Conflict);}
            let mut j = DeliveryJournal::new(key, binding, plan.clone(), now, delivery, reports)?;
            while tx.query_opt("SELECT id FROM plenora_chain_acceptance WHERE id=$1 UNION ALL SELECT result_id FROM plenora_chain_steps WHERE result_id=$1",&[&j.result_id()]).await.map_err(failure)?.is_some(){j=DeliveryJournal::new(key,binding,plan.clone(),now,delivery,reports)?;}
            // This is a new response identity with the host's explicit v2 emission policy,
            // not reconstructed forwarding context from the rejected request.
            tx.execute("INSERT INTO plenora_chain_acceptance(id,requires,downgrade,accepted_at,expires_at) SELECT $1,'plenora-runtime-binding-v2',false,clock_timestamp(),clock_timestamp()+delivery_seconds*interval '1 second' FROM plenora_chain_schema WHERE singleton",&[&j.result_id()]).await.map_err(failure)?;
            tx.execute("INSERT INTO plenora_chain_delivery(key,result_id,route,stage,journal,request_id) VALUES ($1,$2,$3,'ready',$4,$5)",&[&key,&j.result_id(),&plan.route(),&j.to_json()?,&request_identity(key)]).await.map_err(failure)?;
            j
        };
        Self::commit(tx).await.map_err(failure)?;
        Ok(DeliveryOpen::Opened(j, now))
    }
    async fn result_receipt(
        &self,
        scope_prefix: &str,
        result_id: &str,
    ) -> Result<(String, DeliveryJournal), SettlementError> {
        valid_key(scope_prefix)?;
        let mut client = self.connect().await.map_err(failure)?;
        let tx = Self::transaction(&mut client).await.map_err(failure)?;
        Self::expire(&tx).await.map_err(failure)?;
        let (now, delivery, reports) = policy(&tx).await.map_err(failure)?;
        let row = tx.query_opt("SELECT key FROM plenora_chain_delivery WHERE result_id=$1 AND left(key,length($2))=$2", &[&result_id,&scope_prefix]).await.map_err(failure)?.ok_or(SettlementError)?;
        let key: String = row.try_get(0).map_err(failure)?;
        let j = read(&tx, &key, now, delivery, reports).await?;
        Self::commit(tx).await.map_err(failure)?;
        Ok((key, j))
    }
    async fn load(&self, key: &str) -> Result<(DeliveryJournal, u64), SettlementError> {
        valid_key(key)?;
        let mut client = self.connect().await.map_err(failure)?;
        let tx = Self::transaction(&mut client).await.map_err(failure)?;
        Self::expire(&tx).await.map_err(failure)?;
        let (now, delivery, reports) = policy(&tx).await.map_err(failure)?;
        let j = read(&tx, key, now, delivery, reports).await?;
        write(&tx, key, &j).await?;
        Self::commit(tx).await.map_err(failure)?;
        Ok((j, now))
    }
    async fn change(
        &self,
        key: &str,
        revision: u64,
        change: DeliveryChange,
    ) -> Result<(DeliveryJournal, u64), SettlementError> {
        valid_key(key)?;
        let mut client = self.connect().await.map_err(failure)?;
        let tx = Self::transaction(&mut client).await.map_err(failure)?;
        Self::expire(&tx).await.map_err(failure)?;
        let (now, delivery, reports) = policy(&tx).await.map_err(failure)?;
        let mut j = read(&tx, key, now, delivery, reports).await?;
        j.apply(revision, change, now)?;
        write(&tx, key, &j).await?;
        Self::commit(tx).await.map_err(failure)?;
        Ok((j, now))
    }
    async fn pending(
        &self,
        scope_prefix: &str,
        route: &str,
        limit: usize,
    ) -> Result<Vec<String>, SettlementError> {
        if limit == 0 || limit > 256 {
            return Err(SettlementError);
        }
        let mut client = self.connect().await.map_err(failure)?;
        let tx = Self::transaction(&mut client).await.map_err(failure)?;
        Self::expire(&tx).await.map_err(failure)?;
        let rows=tx.query("SELECT d.key FROM plenora_chain_delivery d JOIN plenora_chain_acceptance a ON a.id=d.result_id WHERE d.route=$1 AND d.stage IN ('evidence','finished') AND NOT d.delivered AND NOT a.tombstone AND left(d.key,length($3))=$3 ORDER BY d.key LIMIT $2",&[&route,&i64::try_from(limit).map_err(failure)?,&scope_prefix]).await.map_err(failure)?;
        let keys = rows
            .iter()
            .map(|r| r.try_get::<_, String>(0).map_err(failure))
            .collect::<Result<Vec<_>, _>>()?;
        Self::commit(tx).await.map_err(failure)?;
        Ok(keys)
    }
}

fn request_identity(key: &str) -> Option<&str> {
    let (_, id) = key.rsplit_once(":m:")?;
    let parsed = id.parse::<plenora_runtime_messaging::MessageId>().ok()?;
    (parsed.to_string() == id).then_some(id)
}
