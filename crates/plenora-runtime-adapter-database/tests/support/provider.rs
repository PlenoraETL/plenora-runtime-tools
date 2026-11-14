//! Provider double at the real library's trait boundary, not a scripted `StepResult`.
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorPhase,
    arrow::{RecordBatch, SchemaRef},
    capabilities::ProviderCapabilities,
    loss::{LossCategory, LossConsent, LossReport, LossSeverity, MappingLoss},
    outcome::{CertainPhase, Recovery, RowCounts, WriteOutcome, WriteStatus},
    plan::{Operation, ProviderKind, ReadOperation, WriteOperation},
    provider::{
        BatchStream, ConnectionInfo, Inspection, ParameterBag, PreparedWrite, Provider,
        ProviderFuture, SecretString,
    },
    read_loss::{ReadLossReport, ReadStatus},
    resource::{ResourceBudget, ResourceKind},
};
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

pub struct TestProvider {
    pub batch: RecordBatch,
    pub status: WriteStatus,
    pub loss: bool,
    pub wait_read: bool,
    pub fail_after_batch: bool,
    pub entered: tokio::sync::Notify,
    pub reads: AtomicUsize,
    pub writes: AtomicUsize,
    pub received: Mutex<Vec<RecordBatch>>,
}
impl TestProvider {
    pub fn new(batch: RecordBatch, status: WriteStatus, loss: bool) -> Self {
        Self {
            batch,
            status,
            loss,
            wait_read: false,
            fail_after_batch: false,
            entered: tokio::sync::Notify::new(),
            reads: AtomicUsize::new(0),
            writes: AtomicUsize::new(0),
            received: Mutex::new(vec![]),
        }
    }
}
fn unsupported<T: Send>() -> plenora_database_core::Result<T> {
    Err(DatabaseError::unsupported(
        ProviderKind::Postgres,
        ErrorPhase::Prepare,
        "test operation unavailable",
    ))
}
struct Stream {
    fail_after_batch: bool,
    batch: Option<RecordBatch>,
    schema: SchemaRef,
}
impl BatchStream for Stream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn read_loss(&self) -> Option<ReadLossReport> {
        if self.fail_after_batch {
            return None;
        }
        Some(ReadLossReport {
            schema_version: 1,
            status: if self.batch.is_some() {
                ReadStatus::Open
            } else {
                ReadStatus::Complete
            },
            entries: vec![],
        })
    }
    fn next_batch<'a>(
        &'a mut self,
        _: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(async move {
            if self.fail_after_batch && self.batch.is_none() {
                let mut error = DatabaseError::new(
                    plenora_database_core::ErrorCategory::DataMapping,
                    ErrorPhase::Read,
                    None,
                    "SECRET_SENTINEL",
                );
                error.read_loss = Some(Box::new(ReadLossReport {
                    schema_version: 1,
                    status: ReadStatus::Failed,
                    entries: vec![],
                }));
                return Err(error);
            }
            Ok(self.batch.take())
        })
    }
}
impl Provider for TestProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Postgres
    }
    fn test_connection<'a>(
        &'a self,
        _: &'a SecretString,
        _: &'a CancellationToken,
    ) -> ProviderFuture<'a, ConnectionInfo> {
        Box::pin(async { unsupported() })
    }
    fn probe_capabilities<'a>(
        &'a self,
        _: &'a SecretString,
        _: &'a CancellationToken,
    ) -> ProviderFuture<'a, ProviderCapabilities> {
        Box::pin(async { unsupported() })
    }
    fn inspect<'a>(
        &'a self,
        _: &'a SecretString,
        _: &'a Operation,
        _: &'a CancellationToken,
    ) -> ProviderFuture<'a, Inspection> {
        Box::pin(async { unsupported() })
    }
    fn read<'a>(
        &'a self,
        _: &'a SecretString,
        _: &'a ReadOperation,
        _: &'a ParameterBag,
        _: &'a ResourceBudget,
        token: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn BatchStream>> {
        Box::pin(async move {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            if self.wait_read {
                token.cancelled().await;
                return Err(DatabaseError::new(
                    plenora_database_core::ErrorCategory::Cancelled,
                    ErrorPhase::Read,
                    None,
                    "test cancellation",
                ));
            }
            Ok(Box::new(Stream {
                fail_after_batch: self.fail_after_batch,
                batch: Some(self.batch.clone()),
                schema: self.batch.schema(),
            }) as Box<dyn BatchStream>)
        })
    }
    fn prepare_write<'a>(
        &'a self,
        _: &'a SecretString,
        operation: &'a WriteOperation,
        schema: SchemaRef,
        budget: &'a ResourceBudget,
        _: &'a CancellationToken,
    ) -> ProviderFuture<'a, PreparedWrite> {
        Box::pin(async move {
            let losses = if self.loss {
                vec![MappingLoss {
                    field_id: 1,
                    category: LossCategory::Precision,
                    severity: LossSeverity::DataLoss,
                    reason: "test measured conversion".into(),
                    source_type: None,
                    target_type: None,
                }]
            } else {
                vec![]
            };
            Ok(PreparedWrite::new(
                operation.clone(),
                schema,
                LossReport {
                    schema_version: 2,
                    policy: operation.mapping_policy,
                    losses,
                },
                budget.clone(),
                budget.try_lease(ResourceKind::ConcurrentOperations, 1)?,
                budget.try_lease(ResourceKind::Columns, 1)?,
            ))
        })
    }
    fn write<'a>(
        &'a self,
        _: &'a SecretString,
        _: PreparedWrite,
        mut input: Box<dyn BatchStream>,
        _: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
        _: LossConsent,
    ) -> ProviderFuture<'a, WriteOutcome> {
        Box::pin(async move {
            self.writes.fetch_add(1, Ordering::SeqCst);
            let mut rows = 0;
            while let Some(batch) = input.next_batch(cancellation).await? {
                rows += batch.num_rows() as u64;
                self.received
                    .lock()
                    .map_err(|_| DatabaseError::resource_limit("test lock"))?
                    .push(batch);
            }
            let confirmed = match self.status {
                WriteStatus::Committed => rows,
                WriteStatus::PartiallyCommitted => rows / 2,
                _ => 0,
            };
            Ok(WriteOutcome {
                schema_version: 2,
                loss_report: None,
                status: self.status,
                execution_id: "database-test".into(),
                provider: ProviderKind::Postgres,
                rows: RowCounts {
                    received: rows,
                    confirmed,
                    inserted: Some(confirmed),
                    updated: Some(0),
                    deleted: Some(0),
                    failed: 0,
                    skipped: 0,
                },
                recovery: matches!(
                    self.status,
                    WriteStatus::PartiallyCommitted | WriteStatus::OutcomeUnknown
                )
                .then(|| Recovery {
                    last_certain_phase: CertainPhase::CommitRequested,
                    automatic_retry_allowed: false,
                    idempotency_key: None,
                    staging_object: None,
                    verification_action: Some("verify transaction".into()),
                    cause: None,
                }),
            })
        })
    }
}
