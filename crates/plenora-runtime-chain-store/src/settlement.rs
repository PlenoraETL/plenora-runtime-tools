//! PostgreSQL implementation of the evidenced dispatcher ledger.
use crate::{Invocation, MAX_BYTES, PostgresStore, StartDecision, StartPermit, StoreError};
use async_trait::async_trait;
use plenora_runtime_boundary::{DurableExecutionStore, ExecutionDecision, SettlementError};
use plenora_runtime_messaging::MessageId;
use sha2::{Digest, Sha256};

#[async_trait]
impl DurableExecutionStore for PostgresStore {
    type Permit = StartPermit;
    async fn begin(
        &self,
        id: MessageId,
        scope: &str,
        step: &str,
        input: &[u8],
    ) -> Result<ExecutionDecision<StartPermit>, SettlementError> {
        self.start(Invocation {
            message_id: id,
            scope,
            step,
            key: None,
            canonical_input: input,
        })
        .await
        .map(|decision| match decision {
            StartDecision::Started(p) => ExecutionDecision::Start(p),
            StartDecision::Replay(_) => ExecutionDecision::Replay,
            StartDecision::RequiresRecovery(_) => ExecutionDecision::Recovery,
        })
        .map_err(|_| SettlementError)
    }
    async fn evidence(&self, permit: &StartPermit, report: &[u8]) -> Result<(), SettlementError> {
        self.record_evidence(permit, report)
            .await
            .map_err(|_| SettlementError)
    }
    async fn finalize(
        &self,
        permit: &StartPermit,
        id: MessageId,
        report: &[u8],
        envelope: &[u8],
    ) -> Result<(), SettlementError> {
        self.finish(permit, id, report, envelope)
            .await
            .map_err(|_| SettlementError)
    }
}
impl PostgresStore {
    /// Persists immutable component evidence under the confirmed start fence.
    /// # Errors
    /// Rejects stale fences, differing reports and unconfirmed commits.
    pub async fn record_evidence(
        &self,
        permit: &StartPermit,
        report: &[u8],
    ) -> Result<(), StoreError> {
        if report.is_empty() || report.len() > MAX_BYTES {
            return Err(StoreError::Invalid);
        }
        let mut client = self.connect().await?;
        let tx = Self::transaction(&mut client).await?;
        Self::expire(&tx).await?;
        let row = tx
            .query_opt(
                "SELECT fence,finished FROM plenora_chain_steps WHERE id=$1",
                &[&permit.id],
            )
            .await?
            .ok_or(StoreError::Missing)?;
        if row.try_get::<_, String>(0)? != permit.fence {
            return Err(StoreError::StaleFence);
        }
        let digest = Sha256::digest(report).to_vec();
        if let Some(row) = tx
            .query_opt(
                "SELECT version,digest FROM plenora_chain_evidence WHERE id=$1",
                &[&permit.id],
            )
            .await?
        {
            if row.try_get::<_, i32>(0)? != 1 || row.try_get::<_, Vec<u8>>(1)? != digest {
                return Err(StoreError::Conflict);
            }
        } else {
            // Existing terminal evidence is immutable, including after report retention.
            if row.try_get::<_, bool>(1)? {
                return Err(StoreError::Conflict);
            }
            tx.execute(
                "INSERT INTO plenora_chain_evidence(id,version,report,digest) VALUES ($1,1,$2,$3)",
                &[&permit.id, &report, &digest],
            )
            .await?;
        }
        Self::commit(tx).await
    }

    /// Reads retained complete evidence after an interrupted dispatcher/finalizer.
    /// # Errors
    /// Missing, expired, incompatible or unconfirmed state is explicit.
    pub async fn evidence_report(&self, id: MessageId) -> Result<Vec<u8>, StoreError> {
        let mut client = self.connect().await?;
        let tx = Self::transaction(&mut client).await?;
        Self::expire(&tx).await?;
        let row = tx
            .query_opt(
                "SELECT version,report FROM plenora_chain_evidence WHERE id=$1",
                &[&id.to_string()],
            )
            .await?
            .ok_or(StoreError::Missing)?;
        if row.try_get::<_, i32>(0)? != 1 {
            return Err(StoreError::Unavailable);
        }
        let report = row
            .try_get::<_, Option<Vec<u8>>>(1)?
            .ok_or(StoreError::Expired)?;
        Self::commit(tx).await?;
        Ok(report)
    }
}
