//! Real provider-created source preserves registered Arrow metadata; SQL-only sources do not.
use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, SchemaRef};
use plenora_database_core::{
    CancellationToken,
    loss::{LossConsent, MappingPolicy},
    plan::{ObjectRef, ReadOperation, TransactionProfile, WriteMode, WriteOperation},
    provider::{BatchStream, ParameterBag, Provider, ProviderFuture, SecretString},
    resource::{ResourceBudget, ResourceLimits},
};
use std::sync::Arc;
struct Input(SchemaRef, Option<RecordBatch>);
impl BatchStream for Input {
    fn schema(&self) -> SchemaRef {
        self.0.clone()
    }
    fn next_batch<'a>(
        &'a mut self,
        _: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        let batch = self.1.take();
        Box::pin(async move { Ok(batch) })
    }
}
pub async fn source(connection: &str, table: &str) -> Result<(), Box<dyn std::error::Error>> {
    let provider = plenora_db_postgres::PostgresProvider::insecure_local();
    let secret = SecretString::new(connection.to_owned());
    let schema = plenora_database_core::protocol::contract_schema(vec![Field::new(
        "value",
        DataType::Utf8,
        true,
    )])?;
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![
            None,
            Some(""),
            Some("hello"),
        ]))],
    )?;
    let operation = WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some("public".into()),
            object: table.into(),
        },
        mode: WriteMode::Create,
        mapping_policy: MappingPolicy::Strict,
        transaction_profile: TransactionProfile::SingleTransaction,
        keys: vec![],
        update_columns: vec![],
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    };
    let budget = ResourceBudget::new(ResourceLimits::default())?;
    let cancel = CancellationToken::new();
    let prepared = provider
        .prepare_write(&secret, &operation, schema.clone(), &budget, &cancel)
        .await?;
    let outcome = provider
        .write(
            &secret,
            prepared,
            Box::new(Input(schema.clone(), Some(batch.clone()))),
            &budget,
            &cancel,
            LossConsent::Deny,
        )
        .await?;
    outcome
        .settle()
        .map_err(|_| "fixture write not confirmed")?;
    let read = ReadOperation {
        source: operation.target,
        projection: vec![],
        order_by: vec![],
        row_limit: None,
        row_offset: None,
        filter: None,
        declared_crs: vec![],
    };
    let read_budget = ResourceBudget::new(ResourceLimits::default())?;
    let mut stream = provider
        .read(
            &secret,
            &read,
            &ParameterBag::default(),
            &read_budget,
            &cancel,
        )
        .await?;
    assert_eq!(stream.schema(), schema);
    let mut values = Vec::new();
    while let Some(actual) = stream.next_batch(&cancel).await? {
        assert_eq!(actual.schema(), schema);
        let text = actual
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("fixture text type")?;
        values.extend(text.iter().map(|v| v.map(str::to_owned)));
    }
    values.sort();
    assert_eq!(values.len(), batch.num_rows());
    assert_eq!(
        values,
        vec![None, Some(String::new()), Some("hello".into())]
    );
    let loss = stream.read_loss().ok_or("fixture read_loss absent")?;
    assert_eq!(
        loss.status,
        plenora_database_core::read_loss::ReadStatus::Complete
    );
    assert!(loss.entries.is_empty());
    Ok(())
}
