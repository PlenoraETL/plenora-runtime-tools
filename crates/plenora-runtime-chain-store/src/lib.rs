//! Durable PostgreSQL primitives, independent of ETL engines.
//!
//! The generic outbox trait lacks a publication fence. This crate instead exposes
//! explicit lease authorities; see docs/adr/0009-chain-postgres.md and
//! docs/contract-alignment.md. A recorded start is never automatically reclaimed.
#![forbid(unsafe_code)]

mod delivery;
mod runs;
mod settlement;
mod types;
pub use types::{
    Invocation, Publication, Retention, ScopedKey, StartDecision, StartPermit, StoreError,
    StoredStep,
};

use async_trait::async_trait;
use plenora_runtime_capabilities::{
    AcceptanceRecord, AcceptanceStore, NegotiationContext, NegotiationError,
};
use plenora_runtime_messaging::MessageId;
use plenora_runtime_outbox::InboxStore;
use sha2::{Digest, Sha256};
use std::{fmt, str::FromStr};
use tokio_postgres::{Client, Config, NoTls, Transaction};
use uuid::Uuid;

// Ordered source of truth shared by migration execution and qualification.
const MIGRATIONS: &[(&str, &str)] = &[
    ("migration.sql", include_str!("migration.sql")),
    ("migration-v2.sql", include_str!("migration-v2.sql")),
    ("evidence-v1.sql", include_str!("evidence-v1.sql")),
    ("delivery-v1.sql", include_str!("delivery-v1.sql")),
    ("runs-v1.sql", include_str!("runs-v1.sql")),
    ("idempotency-v1.sql", include_str!("idempotency-v1.sql")),
    ("work-v1.sql", include_str!("work-v1.sql")),
];

/// Connection factory. Each operation opens a primary connection and a transaction;
/// no cached acceptance, replica fallback, or in-memory production profile exists.
/// Connections use PostgreSQL's local/trusted-network transport (`NoTls`); deployment
/// TLS qualification is tracked in docs/security-review.md.
#[derive(Clone)]
pub struct PostgresStore {
    config: Config,
}
impl fmt::Debug for PostgresStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PostgresStore(<redacted>)")
    }
}

impl PostgresStore {
    /// Parses a connection configuration without retaining driver errors.
    /// Does not assert readiness: call `migrate`, and every operation verifies the primary.
    /// # Errors
    /// Returns `Invalid` on malformed configuration.
    pub fn new(connection: &str) -> Result<Self, StoreError> {
        let mut config = Config::from_str(connection).map_err(|_| StoreError::Invalid)?;
        config.connect_timeout(std::time::Duration::from_secs(10));
        Ok(Self { config })
    }

    async fn connect(&self) -> Result<Client, StoreError> {
        let (client, connection) = self.config.connect(NoTls).await?;
        // Driver failures close the client channel; subsequent operations fail closed.
        tokio::spawn(async move {
            let _closed = connection.await;
        });
        Ok(client)
    }

    async fn transaction(client: &mut Client) -> Result<Transaction<'_>, StoreError> {
        let tx = client
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::ReadCommitted)
            .start()
            .await?;
        tx.batch_execute("SET LOCAL synchronous_commit = on; SET LOCAL statement_timeout = '15s'; SET LOCAL lock_timeout = '10s'").await?;
        let row = tx.query_one("SELECT pg_is_in_recovery(), current_setting('transaction_read_only'), current_setting('fsync')", &[]).await?;
        if row.try_get::<_, bool>(0)?
            || row.try_get::<_, String>(1)? != "off"
            || row.try_get::<_, String>(2)? != "on"
        {
            return Err(StoreError::Unavailable);
        }
        // One database-wide lock orders migration, policy reduction, reads and CAS.
        // This deliberately trades throughput for a small, auditable serialization point.
        tx.query_one("SELECT pg_advisory_xact_lock(72466322612002)", &[])
            .await?;
        Ok(tx)
    }

    async fn commit(tx: Transaction<'_>) -> Result<(), StoreError> {
        tx.commit().await.map_err(|_| StoreError::Unconfirmed)
    }

    /// Upgrades schema v1 to v2 atomically and persists only reductions of retention.
    /// Concurrent processes serialize through the same lock used by all operations.
    /// # Errors
    /// Refuses incompatible schemas, unconfirmed commits, or policy extensions.
    pub async fn migrate(&self, retention: Retention) -> Result<(), StoreError> {
        let mut client = self.connect().await?;
        let tx = Self::transaction(&mut client).await?;
        for (_, sql) in MIGRATIONS {
            tx.batch_execute(sql).await?;
        }
        let old = tx.query_opt("SELECT version, delivery_seconds, report_seconds FROM plenora_chain_schema WHERE singleton", &[]).await?;
        if let Some(old) = old {
            if !matches!(old.try_get::<_, i32>(0)?, 1 | 2)
                || retention.delivery > old.try_get::<_, i64>(1)?
                || retention.reports > old.try_get::<_, i64>(2)?
            {
                return Err(StoreError::Invalid);
            }
            tx.execute("UPDATE plenora_chain_schema SET version=2, delivery_seconds=$1, report_seconds=$2 WHERE singleton", &[&retention.delivery, &retention.reports]).await?;
            tx.execute("UPDATE plenora_chain_acceptance SET expires_at=LEAST(expires_at, accepted_at + $1::bigint * interval '1 second')", &[&retention.delivery]).await?;
        } else {
            tx.execute(
                "INSERT INTO plenora_chain_schema VALUES (true, 2, $1, $2)",
                &[&retention.delivery, &retention.reports],
            )
            .await?;
        }
        Self::expire(&tx).await?;
        Self::commit(tx).await
    }

    async fn expire(tx: &Transaction<'_>) -> Result<(), StoreError> {
        let version: i32 = tx
            .query_one(
                "SELECT version FROM plenora_chain_schema WHERE singleton",
                &[],
            )
            .await?
            .try_get(0)?;
        if version != 2 {
            return Err(StoreError::Unavailable);
        }
        tx.execute("UPDATE plenora_chain_idempotency i SET carrier=NULL FROM plenora_chain_schema p WHERE p.singleton AND i.carrier IS NOT NULL AND i.created_ms + p.report_seconds*1000 <= extract(epoch FROM clock_timestamp())*1000", &[]).await?;
        // Purge retained run documents under the same monotone policy; keep scoped identities forever.
        tx.execute("UPDATE plenora_chain_runs r SET journal=NULL FROM plenora_chain_schema p WHERE p.singleton AND r.journal IS NOT NULL AND r.created_ms + p.report_seconds*1000 <= extract(epoch FROM clock_timestamp())*1000", &[]).await?;
        tx.execute("UPDATE plenora_chain_acceptance SET tombstone=true WHERE expires_at <= clock_timestamp() AND NOT tombstone", &[]).await?;
        tx.execute("UPDATE plenora_chain_outbox o SET expired=true, lease=NULL, lease_until=NULL FROM plenora_chain_acceptance a WHERE o.id=a.id AND a.tombstone AND NOT o.delivered", &[]).await?;
        // The product journal shares the report clock even when nobody reads the result.
        tx.execute("UPDATE plenora_chain_delivery d SET journal=convert_to(jsonb_set(jsonb_set(convert_from(d.journal,'UTF8')::jsonb,'{history}','[]'::jsonb),'{expired_report}','true'::jsonb)::text,'UTF8') FROM plenora_chain_schema p WHERE p.singleton AND NOT (convert_from(d.journal,'UTF8')::jsonb->>'expired_report')::boolean AND (convert_from(d.journal,'UTF8')::jsonb->>'created')::numeric + p.report_seconds*1000 <= extract(epoch FROM clock_timestamp())*1000", &[]).await?;
        // Report expiry removes retained data, never the explicit outcome/delivery evidence.
        tx.execute("UPDATE plenora_chain_steps s SET report=NULL, report_expired=true FROM plenora_chain_schema p, plenora_chain_outbox o WHERE p.singleton AND o.id=s.id AND NOT s.report_expired AND s.finished_at + p.report_seconds * interval '1 second' <= clock_timestamp()", &[]).await?;
        tx.execute("UPDATE plenora_chain_outbox o SET payload=''::bytea, expired=(o.expired OR NOT o.delivered), lease=NULL, lease_until=NULL FROM plenora_chain_steps s WHERE s.id=o.id AND s.report_expired AND octet_length(o.payload)>0", &[]).await?;
        tx.execute("UPDATE plenora_chain_evidence e SET report=NULL FROM plenora_chain_schema p WHERE p.singleton AND e.recorded_at + p.report_seconds * interval '1 second' <= clock_timestamp()", &[]).await?;
        Ok(())
    }

    /// Performs retention maintenance without deleting identities, keys or fences.
    /// # Errors
    /// Fails if authoritative maintenance cannot be committed.
    pub async fn maintain(&self) -> Result<(), StoreError> {
        let mut client = self.connect().await?;
        let tx = Self::transaction(&mut client).await?;
        Self::expire(&tx).await?;
        Self::commit(tx).await
    }

    /// Atomic authoritative acceptance, also used by the synchronous common trait.
    /// # Errors
    /// Missing lookup, expiry, unavailable primary or unconfirmed write refuses acceptance.
    pub async fn accept_async(
        &self,
        id: MessageId,
        proposed: Option<NegotiationContext>,
    ) -> Result<AcceptanceRecord, StoreError> {
        let mut client = self.connect().await?;
        let tx = Self::transaction(&mut client).await?;
        Self::expire(&tx).await?;
        let id = id.to_string();
        let existing = tx
            .query_opt(
                "SELECT requires, downgrade, tombstone FROM plenora_chain_acceptance WHERE id=$1",
                &[&id],
            )
            .await?;
        let outcome = if let Some(row) = existing {
            if row.try_get::<_, bool>(2)? {
                Self::commit(tx).await?;
                return Err(StoreError::Expired);
            }
            let requires: String = row.try_get(0)?;
            let downgrade: bool = row.try_get(1)?;
            let context = NegotiationContext::from_headers(
                Some(&requires),
                Some(if downgrade { "true" } else { "false" }),
            )
            .map_err(|_| StoreError::Unavailable)?
            .ok_or(StoreError::Unavailable)?;
            AcceptanceRecord::Existing(context)
        } else {
            let context = proposed.ok_or(StoreError::Missing)?;
            if tx
                .query_opt(
                    "SELECT id FROM plenora_chain_steps WHERE result_id=$1",
                    &[&id],
                )
                .await?
                .is_some()
            {
                return Err(StoreError::Conflict);
            }
            tx.execute("WITH instant AS (SELECT clock_timestamp() AS t) INSERT INTO plenora_chain_acceptance(id, requires, downgrade, accepted_at, expires_at) SELECT $1,$2,$3,t,t + delivery_seconds * interval '1 second' FROM plenora_chain_schema, instant WHERE singleton", &[&id, &context.headers().0, &context.accepts_downgrade]).await?;
            AcceptanceRecord::Created
        };
        Self::commit(tx).await?;
        Ok(outcome)
    }

    /// Records an invocation intention and allocates its sole start authority.
    /// A crash after this commit requires reconciliation, even if invocation never began.
    /// # Errors
    /// Refuses missing/expired acceptance, malformed input, conflict or uncertain persistence.
    #[allow(
        clippy::too_many_lines,
        reason = "One transaction checks both durable authorities before reserving a legacy step"
    )]
    pub async fn start(&self, input: Invocation<'_>) -> Result<StartDecision, StoreError> {
        validate(&input)?;
        let id = input.message_id.to_string();
        let mut hash = Sha256::new();
        for field in [
            input.key.map_or("", |k| k.scope).as_bytes(),
            input.key.map_or("", |k| k.value).as_bytes(),
            input.canonical_input,
        ] {
            hash.update((field.len() as u64).to_be_bytes());
            hash.update(field);
        }
        let digest = hash.finalize().to_vec();
        let mut client = self.connect().await?;
        let tx = Self::transaction(&mut client).await?;
        Self::expire(&tx).await?;
        let accepted = tx
            .query_opt(
                "SELECT tombstone FROM plenora_chain_acceptance WHERE id=$1",
                &[&id],
            )
            .await?
            .ok_or(StoreError::Missing)?;
        if accepted.try_get::<_, bool>(0)? {
            Self::commit(tx).await?;
            return Err(StoreError::Expired);
        }
        if tx
            .query_opt(
                "SELECT key FROM plenora_chain_delivery WHERE request_id=$1 OR result_id=$1",
                &[&id],
            )
            .await?
            .is_some()
        {
            return Err(StoreError::Conflict);
        }
        if let Some(binding) = tx
            .query_opt(
                "SELECT scope, step FROM plenora_chain_aliases WHERE id=$1",
                &[&id],
            )
            .await?
            && (binding.try_get::<_, String>(0)? != input.scope
                || binding.try_get::<_, String>(1)? != input.step)
        {
            return Err(StoreError::Conflict);
        }
        let key_scope = input.key.map(|k| k.scope);
        let key_value = input.key.map(|k| k.value.as_bytes());
        let rows = tx.query("SELECT DISTINCT s.id, s.scope, s.step, s.fingerprint, s.finished, a.tombstone, k.scope, k.key FROM plenora_chain_steps s JOIN plenora_chain_acceptance a ON a.id=s.id LEFT JOIN plenora_chain_keys k ON k.id=s.id LEFT JOIN plenora_chain_aliases x ON x.original=s.id LEFT JOIN plenora_chain_slots b ON b.original=s.id WHERE s.id=$1 OR x.id=$1 OR (b.scope=$2 AND b.step=$3) OR (k.scope=$5 AND k.key=$4)", &[&id, &input.scope, &input.step, &key_value, &key_scope]).await?;
        if !rows.is_empty() {
            if rows.len() != 1 {
                return Err(StoreError::Conflict);
            }
            let row = rows.first().ok_or(StoreError::Unavailable)?;
            let same_key = key_scope.is_some()
                && row.try_get::<_, Option<String>>(6)?.as_deref() == key_scope
                && row.try_get::<_, Option<Vec<u8>>>(7)?.as_deref() == key_value;
            if (!same_key
                && (row.try_get::<_, String>(1)? != input.scope
                    || row.try_get::<_, String>(2)? != input.step))
                || row.try_get::<_, Vec<u8>>(3)? != digest
            {
                return Err(StoreError::Conflict);
            }
            if row.try_get::<_, bool>(5)? {
                Self::commit(tx).await?;
                return Err(StoreError::Expired);
            }
            let original: String = row.try_get(0)?;
            tx.execute("INSERT INTO plenora_chain_aliases(id,original,scope,step) VALUES ($1,$2,$3,$4) ON CONFLICT (id) DO NOTHING", &[&id, &original, &input.scope, &input.step]).await?;
            tx.execute("INSERT INTO plenora_chain_slots(scope,step,original) VALUES ($1,$2,$3) ON CONFLICT (scope,step) DO NOTHING", &[&input.scope, &input.step, &original]).await?;
            let original = MessageId::from_str(&original).map_err(|_| StoreError::Unavailable)?;
            let decision = if row.try_get::<_, bool>(4)? {
                StartDecision::Replay(original)
            } else {
                StartDecision::RequiresRecovery(original)
            };
            Self::commit(tx).await?;
            return Ok(decision);
        }
        let fence = Uuid::new_v4().to_string();
        tx.execute("INSERT INTO plenora_chain_steps(id,scope,step,fingerprint,fence) VALUES ($1,$2,$3,$4,$5)", &[&id, &input.scope, &input.step, &digest, &fence]).await?;
        tx.execute(
            "INSERT INTO plenora_chain_aliases(id,original,scope,step) VALUES ($1,$1,$2,$3)",
            &[&id, &input.scope, &input.step],
        )
        .await?;
        tx.execute(
            "INSERT INTO plenora_chain_slots(scope,step,original) VALUES ($1,$2,$3)",
            &[&input.scope, &input.step, &id],
        )
        .await?;
        if let Some(key) = input.key {
            tx.execute(
                "INSERT INTO plenora_chain_keys(scope,key,id) VALUES ($1,$2,$3)",
                &[&key.scope, &key.value.as_bytes(), &id],
            )
            .await?;
        }
        Self::commit(tx).await?;
        Ok(StartDecision::Started(StartPermit { id, fence }))
    }

    /// Atomically commits complete report, immutable result envelope and processed inbox.
    /// `envelope` is supplied by the boundary serializer with the given result identity.
    /// Repeating the exact operation with the same authority is safe after lost acknowledgement.
    /// # Errors
    /// Refuses stale authority, differing replay, invalid identity or unconfirmed commit.
    pub async fn finish(
        &self,
        permit: &StartPermit,
        result_id: MessageId,
        report: &[u8],
        envelope: &[u8],
    ) -> Result<(), StoreError> {
        let result_id = result_id.to_string();
        if result_id == permit.id
            || report.is_empty()
            || envelope.is_empty()
            || report.len() > MAX_BYTES
            || envelope.len() > MAX_BYTES
        {
            return Err(StoreError::Invalid);
        }
        let report_digest = Sha256::digest(report).to_vec();
        let payload_digest = Sha256::digest(envelope).to_vec();
        let mut client = self.connect().await?;
        let tx = Self::transaction(&mut client).await?;
        Self::expire(&tx).await?;
        let row = tx.query_opt("SELECT fence,finished,result_id,report_digest FROM plenora_chain_steps WHERE id=$1", &[&permit.id]).await?.ok_or(StoreError::Missing)?;
        if row.try_get::<_, String>(0)? != permit.fence {
            return Err(StoreError::StaleFence);
        }
        if let Some(evidence) = tx
            .query_opt(
                "SELECT version,digest FROM plenora_chain_evidence WHERE id=$1",
                &[&permit.id],
            )
            .await?
            && (evidence.try_get::<_, i32>(0)? != 1
                || evidence.try_get::<_, Vec<u8>>(1)? != report_digest)
        {
            return Err(StoreError::Conflict);
        }
        if row.try_get::<_, bool>(1)? {
            let payload = tx
                .query_one(
                    "SELECT payload_digest FROM plenora_chain_outbox WHERE id=$1",
                    &[&permit.id],
                )
                .await?;
            if row.try_get::<_, Option<String>>(2)?.as_deref() != Some(&result_id)
                || row.try_get::<_, Option<Vec<u8>>>(3)?.as_deref()
                    != Some(report_digest.as_slice())
                || payload.try_get::<_, Vec<u8>>(0)? != payload_digest
            {
                return Err(StoreError::Conflict);
            }
        } else {
            if tx
                .query_opt(
                    "SELECT id FROM plenora_chain_acceptance WHERE id=$1 UNION ALL SELECT id FROM plenora_chain_steps WHERE result_id=$1",
                    &[&result_id],
                )
                .await?
                .is_some()
            {
                return Err(StoreError::Conflict);
            }
            tx.execute("UPDATE plenora_chain_steps SET finished=true,finished_at=clock_timestamp(),result_id=$2,report=$3,report_digest=$4 WHERE id=$1", &[&permit.id, &result_id, &report, &report_digest]).await?;
            tx.execute("INSERT INTO plenora_chain_outbox(id,payload,payload_digest,expired) SELECT $1,$2,$3,tombstone FROM plenora_chain_acceptance WHERE id=$1", &[&permit.id, &envelope, &payload_digest]).await?;
            tx.execute(
                "INSERT INTO plenora_chain_inbox(id) VALUES ($1)",
                &[&permit.id],
            )
            .await?;
        }
        Self::commit(tx).await
    }

    /// Reads state from the verified primary, applying expiry before returning.
    /// # Errors
    /// Refuses missing state, nonauthoritative reads or unconfirmed retention updates.
    pub async fn read(&self, id: MessageId) -> Result<StoredStep, StoreError> {
        let mut client = self.connect().await?;
        let tx = Self::transaction(&mut client).await?;
        Self::expire(&tx).await?;
        let row = tx.query_opt("SELECT s.finished,s.result_id,s.report,s.report_expired,COALESCE(o.delivered,false),(a.tombstone OR COALESCE(o.expired,false)) FROM plenora_chain_steps s JOIN plenora_chain_acceptance a ON a.id=s.id LEFT JOIN plenora_chain_outbox o ON o.id=s.id WHERE s.id=$1 OR s.id=(SELECT original FROM plenora_chain_aliases WHERE id=$1)", &[&id.to_string()]).await?.ok_or(StoreError::Missing)?;
        let result = StoredStep {
            finished: row.try_get(0)?,
            result_id: row.try_get(1)?,
            report: row.try_get(2)?,
            report_expired: row.try_get(3)?,
            delivered: row.try_get(4)?,
            delivery_expired: row.try_get(5)?,
        };
        Self::commit(tx).await?;
        Ok(result)
    }

    /// Claims at most one result for a bounded 30-second publication lease.
    /// Reclaims only publication, never the domain invocation. The broker may see
    /// duplicates after a crash: the persisted result message identity is unchanged.
    /// # Errors
    /// Returns no authority unless the lease commit is confirmed.
    pub async fn claim(&self) -> Result<Option<Publication>, StoreError> {
        let mut client = self.connect().await?;
        let tx = Self::transaction(&mut client).await?;
        Self::expire(&tx).await?;
        let row = tx.query_opt("SELECT o.id,s.result_id,o.payload FROM plenora_chain_outbox o JOIN plenora_chain_steps s ON s.id=o.id WHERE NOT o.delivered AND NOT o.expired AND (o.lease_until IS NULL OR o.lease_until <= clock_timestamp()) ORDER BY o.id LIMIT 1", &[]).await?;
        let result = if let Some(row) = row {
            let id: String = row.try_get(0)?;
            let fence = Uuid::new_v4().to_string();
            tx.execute("UPDATE plenora_chain_outbox SET lease=$2,lease_until=clock_timestamp()+interval '30 seconds',attempts=attempts+1 WHERE id=$1", &[&id,&fence]).await?;
            Some(Publication {
                id,
                fence,
                result_id: row.try_get(1)?,
                payload: row.try_get(2)?,
            })
        } else {
            None
        };
        Self::commit(tx).await?;
        Ok(result)
    }

    /// Records a broker-confirmed publication against its exact current fence.
    /// # Errors
    /// An expired/replaced lease, stale confirmation or uncertain commit fails explicitly.
    pub async fn confirm(&self, publication: &Publication) -> Result<(), StoreError> {
        let mut client = self.connect().await?;
        let tx = Self::transaction(&mut client).await?;
        Self::expire(&tx).await?;
        let count = tx.execute("UPDATE plenora_chain_outbox SET delivered=true WHERE id=$1 AND lease=$2 AND lease_until>clock_timestamp() AND NOT expired", &[&publication.id, &publication.fence]).await?;
        if count != 1 {
            Self::commit(tx).await?;
            return Err(StoreError::StaleFence);
        }
        Self::commit(tx).await
    }
}

impl AcceptanceStore for PostgresStore {
    fn accept(
        &self,
        id: &MessageId,
        proposed: Option<NegotiationContext>,
    ) -> Result<AcceptanceRecord, NegotiationError> {
        // The common trait is synchronous. A separate thread avoids nested-runtime
        // panics; async hosts should use accept_async or their blocking executor.
        std::thread::scope(|scope| {
            std::thread::Builder::new()
                .spawn_scoped(scope, || {
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|_| NegotiationError)?
                        .block_on(self.accept_async(*id, proposed))
                        .map_err(|_| NegotiationError)
                })
                .map_err(|_| NegotiationError)?
                .join()
                .map_err(|_| NegotiationError)?
        })
    }
}

#[async_trait]
impl InboxStore for PostgresStore {
    type Error = StoreError;
    async fn contains(&self, id: MessageId) -> Result<bool, StoreError> {
        match self.read(id).await {
            Ok(step) => Ok(step.finished),
            Err(StoreError::Missing) => Ok(false),
            Err(error) => Err(error),
        }
    }
    async fn record_processed(&self, id: MessageId) -> Result<(), StoreError> {
        // Only atomic finish may create the inbox entry. The legacy standalone
        // mutation cannot prove an outcome/outbox and therefore fails closed.
        if self.contains(id).await? {
            Ok(())
        } else {
            Err(StoreError::Missing)
        }
    }
}

const MAX_BYTES: usize = 64 * 1024 * 1024;
fn validate(input: &Invocation<'_>) -> Result<(), StoreError> {
    let step = input.step.as_bytes();
    if input.scope.is_empty()
        || input.scope.len() > 256
        || !input.scope.bytes().all(|b| b.is_ascii_graphic())
        || step.is_empty()
        || step.len() > 64
        || !step.first().is_some_and(u8::is_ascii_lowercase)
        || !step
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'_' || *b == b'-')
        || input.key.is_some_and(|k| {
            k.value.is_empty()
                || k.value.len() > 256
                || k.scope.is_empty()
                || k.scope.len() > 256
                || !k.scope.bytes().all(|b| b.is_ascii_graphic())
        })
        || input.canonical_input.is_empty()
        || input.canonical_input.len() > MAX_BYTES
    {
        return Err(StoreError::Invalid);
    }
    Ok(())
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod delivery_tests;

#[cfg(test)]
mod migration_tests;
