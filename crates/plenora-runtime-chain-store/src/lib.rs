//! Durable PostgreSQL primitives, independent of ETL engines.
//!
//! The generic outbox trait lacks a publication fence. This crate instead exposes
//! explicit lease authorities; see docs/adr/0009-chain-postgres.md and
//! docs/contract-alignment.md. A recorded start is never automatically reclaimed.
#![forbid(unsafe_code)]

mod delivery;
mod diagnostics;
mod resources;
mod runs;
mod types;
pub use types::{Retention, StoreError};

use plenora_runtime_capabilities::{
    AcceptanceRecord, AcceptanceStore, NegotiationContext, NegotiationError,
};
use plenora_runtime_messaging::MessageId;
use std::{fmt, str::FromStr};
use tokio_postgres::{Client, Config, NoTls, Transaction};

mod schema;
use schema::{BOOTSTRAP, current_version};

/// Primary connection establishment bound in milliseconds.
pub const PRIMARY_CONNECT_MS: u64 = 10_000;
/// Per-statement primary bound in milliseconds; not a whole-transaction deadline.
pub const PRIMARY_STATEMENT_MS: u64 = 15_000;

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
        config.connect_timeout(std::time::Duration::from_millis(PRIMARY_CONNECT_MS));
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
        tx.batch_execute(&format!("SET LOCAL search_path = public; SET LOCAL synchronous_commit = on; SET LOCAL statement_timeout = '{PRIMARY_STATEMENT_MS}ms'; SET LOCAL lock_timeout = '10s'")).await?;
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
        tx.commit().await.map_err(|error| {
            diagnostics::report(&error);
            StoreError::Unconfirmed
        })
    }

    /// Initializes the current schema atomically and persists only reductions of retention.
    /// Concurrent processes serialize through the same lock used by all operations.
    /// # Errors
    /// Refuses incompatible schemas, unconfirmed commits, or policy extensions.
    pub async fn migrate(&self, retention: Retention) -> Result<(), StoreError> {
        let mut client = self.connect().await?;
        let tx = Self::transaction(&mut client).await?;
        // Inspect the identity before any DDL; no previous schema is an upgrade input.
        let exists: bool = tx
            .query_one(
                "SELECT to_regclass('plenora_chain_schema') IS NOT NULL",
                &[],
            )
            .await?
            .try_get(0)?;
        if exists {
            // Query the version first so previous schemas have an explicit Invalid category.
            let rows = tx
                .query(
                    "SELECT version FROM plenora_chain_schema WHERE singleton",
                    &[],
                )
                .await?;
            if rows.len() != 1
                || rows
                    .first()
                    .ok_or(StoreError::Invalid)?
                    .try_get::<_, i32>(0)?
                    != current_version()
            {
                return Err(StoreError::Invalid);
            }
        } else {
            let partial: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM pg_tables WHERE schemaname=current_schema() AND left(tablename,14) = 'plenora_chain_')", &[]).await?.try_get(0)?;
            if partial {
                return Err(StoreError::Invalid);
            }
            tx.batch_execute(BOOTSTRAP).await?;
        }
        schema::validate_layout(&tx).await?;
        let version = current_version();
        let old = tx.query_opt("SELECT version, delivery_seconds, report_seconds FROM plenora_chain_schema WHERE singleton", &[]).await?;
        if let Some(old) = old {
            if old.try_get::<_, i32>(0)? != version
                || retention.delivery > old.try_get::<_, i64>(1)?
                || retention.reports > old.try_get::<_, i64>(2)?
            {
                return Err(StoreError::Invalid);
            }
            tx.execute("UPDATE plenora_chain_schema SET version=$3, delivery_seconds=$1, report_seconds=$2 WHERE singleton", &[&retention.delivery, &retention.reports, &version]).await?;
            tx.execute("UPDATE plenora_chain_acceptance SET expires_at=LEAST(expires_at, accepted_at + $1::bigint * interval '1 second')", &[&retention.delivery]).await?;
        } else {
            tx.execute(
                "INSERT INTO plenora_chain_schema VALUES (true,$3,$1,$2)",
                &[&retention.delivery, &retention.reports, &version],
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
        if version != current_version() {
            return Err(StoreError::Unavailable);
        }
        tx.execute("UPDATE plenora_chain_idempotency i SET carrier=NULL FROM plenora_chain_schema p WHERE p.singleton AND i.carrier IS NOT NULL AND i.created_ms + p.report_seconds*1000 <= extract(epoch FROM clock_timestamp())*1000", &[]).await?;
        // Purge retained run documents under the same monotone policy; keep scoped identities forever.
        tx.execute("UPDATE plenora_chain_runs r SET journal=NULL FROM plenora_chain_schema p WHERE p.singleton AND r.journal IS NOT NULL AND r.created_ms + p.report_seconds*1000 <= extract(epoch FROM clock_timestamp())*1000", &[]).await?;
        tx.execute("UPDATE plenora_chain_acceptance SET tombstone=true WHERE expires_at <= clock_timestamp() AND NOT tombstone", &[]).await?;
        // The product journal shares the report clock even when nobody reads the result.
        Self::expire_delivery_reports(tx).await?;
        Ok(())
    }

    /// Performs retention maintenance without deleting identities, keys or fences.
    /// # Errors
    /// Fails if authoritative maintenance cannot be committed.
    pub async fn maintain(&self) -> Result<(), StoreError> {
        let mut client = self.connect().await?;
        let tx = Self::transaction(&mut client).await?;
        Self::expire(&tx).await?;
        Self::sweep_owned(&tx, None)
            .await
            .map_err(|_| StoreError::Unavailable)?;
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
            tx.execute("WITH instant AS (SELECT clock_timestamp() AS t) INSERT INTO plenora_chain_acceptance(id, requires, downgrade, accepted_at, expires_at) SELECT $1,$2,$3,t,t + delivery_seconds * interval '1 second' FROM plenora_chain_schema, instant WHERE singleton", &[&id, &context.headers().0, &context.accepts_downgrade]).await?;
            AcceptanceRecord::Created
        };
        Self::commit(tx).await?;
        Ok(outcome)
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

#[cfg(test)]
mod tests;

#[cfg(test)]
mod delivery_tests;

#[cfg(test)]
mod migration_tests;
