//! Exact IPC boundary; no casts, schema normalization or discarded metadata.
use crate::{Error, digest};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorPhase,
    arrow::{RecordBatch, Schema, SchemaRef},
    provider::{BatchStream, ProviderFuture},
};
use plenora_runtime_chains::contracts::Port;
use std::{
    collections::VecDeque,
    io::{Cursor, Write},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

pub(crate) struct Limited {
    pub bytes: Vec<u8>,
    pub limit: usize,
}
impl Write for Limited {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other("database artifact limit"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(crate) fn schema(schema: &Schema, port: &Port) -> Result<(), Error> {
    plenora_database_core::field_contract::validate_published_schema(schema)
        .map_err(|_| Error::Schema)?;
    let value = serde_json::to_value(schema).map_err(|_| Error::Schema)?;
    let actual = digest(&serde_json::to_vec(&value).map_err(|_| Error::Schema)?);
    if schema.fields().len() > 32 || crate::encode(&value)?.len() > 16 * 1024 {
        return Err(Error::ResourceLimit);
    }
    if port
        .schema_digest
        .as_ref()
        .is_some_and(|expected| expected != &actual)
    {
        return Err(Error::Schema);
    }
    // Primary Geometry requires semantic qualification, not opaque key transport.
    // Initial database profile is tabular; no unproved spatial claim is advertised.
    if port.primary_geometry
        || schema.metadata().contains_key("plenora.primary_geometry")
        || schema.fields().iter().any(|field| {
            plenora_database_core::field_contract::FieldContract::parse(field)
                .is_ok_and(|contract| contract.is_geometry() || contract.is_geography())
        })
    {
        return Err(Error::Unsupported);
    }
    Ok(())
}

pub(crate) struct Input {
    pub schema: SchemaRef,
    pub batches: VecDeque<RecordBatch>,
    pub rows: u64,
    pub memory: u64,
    pub consumed: Arc<AtomicU64>,
}
impl BatchStream for Input {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn declared_input_rows(&self) -> Option<u64> {
        Some(self.rows)
    }
    fn next_batch<'a>(
        &'a mut self,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(async move {
            check(cancellation)?;
            let batch = self.batches.pop_front();
            if let Some(batch) = &batch {
                self.consumed
                    .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
            }
            Ok(batch)
        })
    }
}
pub(crate) fn decode(
    bytes: &[u8],
    port: &Port,
    row_limit: u64,
    memory_limit: u64,
) -> Result<Input, Error> {
    framing(bytes, memory_limit)?;
    // Reject suffixes after EOS. IPC accepts concatenation, this single-artifact profile does not.
    let mut cursor = Cursor::new(bytes);
    let mut reader = StreamReader::try_new(&mut cursor, None).map_err(|_| Error::Schema)?;
    let fields = reader.schema();
    schema(&fields, port)?;
    let mut result = Input {
        schema: fields,
        batches: VecDeque::new(),
        rows: 0,
        memory: bytes.len() as u64,
        consumed: Arc::new(AtomicU64::new(0)),
    };
    for batch in &mut reader {
        let batch = batch.map_err(|_| Error::Schema)?;
        plenora_database_core::arrow_validation::validate_batch(&batch, ErrorPhase::Validate)
            .map_err(|_| Error::Schema)?;
        result.rows = result
            .rows
            .checked_add(batch.num_rows() as u64)
            .ok_or(Error::ResourceLimit)?;
        result.memory = result
            .memory
            .checked_add(batch.get_array_memory_size() as u64)
            .ok_or(Error::ResourceLimit)?;
        if result.rows > row_limit || result.memory > memory_limit {
            return Err(Error::ResourceLimit);
        }
        result.batches.push_back(batch);
    }
    drop(reader);
    if cursor.position() != bytes.len() as u64 {
        return Err(Error::Invalid);
    }
    Ok(result)
}

pub(crate) async fn encode(
    stream: &mut dyn BatchStream,
    port: &Port,
    token: &CancellationToken,
    byte_limit: usize,
    row_limit: u64,
    memory_limit: u64,
    consumed: &mut u64,
) -> Result<(Vec<u8>, u64, u64, u64), DatabaseError> {
    let convert = |error: Error| match error {
        Error::ResourceLimit => DatabaseError::resource_limit("database artifact limit"),
        _ => DatabaseError::new(
            plenora_database_core::ErrorCategory::Schema,
            ErrorPhase::Read,
            None,
            "database Arrow schema rejected",
        ),
    };
    let fields = stream.schema();
    schema(&fields, port).map_err(convert)?;
    let mut sink = Limited {
        bytes: Vec::new(),
        limit: byte_limit,
    };
    let mut writer =
        StreamWriter::try_new(&mut sink, &fields).map_err(|_| convert(Error::ResourceLimit))?;
    let mut rows = 0_u64;
    let mut batches = 0_u64;
    while let Some(batch) = stream.next_batch(token).await? {
        rows = rows
            .checked_add(batch.num_rows() as u64)
            .ok_or_else(|| convert(Error::ResourceLimit))?;
        *consumed = rows;
        plenora_database_core::arrow_validation::validate_batch(&batch, ErrorPhase::Read)?;
        check(token)?;
        if batch.schema() != fields {
            return Err(convert(Error::Schema));
        }
        if rows > row_limit || batch.get_array_memory_size() as u64 > memory_limit {
            return Err(convert(Error::ResourceLimit));
        }
        writer
            .write(&batch)
            .map_err(|_| convert(Error::ResourceLimit))?;
        batches = batches
            .checked_add(1)
            .ok_or_else(|| convert(Error::ResourceLimit))?;
    }
    writer.finish().map_err(|_| convert(Error::ResourceLimit))?;
    drop(writer);
    Ok((sink.bytes, rows, fields.fields().len() as u64, batches))
}

// Check lengths and compression BEFORE Arrow allocates from untrusted message headers.
fn framing(bytes: &[u8], memory_limit: u64) -> Result<(), Error> {
    let mut rest = bytes;
    while !rest.is_empty() {
        let (word, tail) = rest.split_first_chunk::<4>().ok_or(Error::Invalid)?;
        let mut length = u32::from_le_bytes(*word);
        rest = tail;
        if length == u32::MAX {
            let (word, tail) = rest.split_first_chunk::<4>().ok_or(Error::Invalid)?;
            length = u32::from_le_bytes(*word);
            rest = tail;
        }
        if length == 0 {
            return if rest.is_empty() {
                Ok(())
            } else {
                Err(Error::Invalid)
            };
        }
        let length = usize::try_from(length).map_err(|_| Error::ResourceLimit)?;
        let metadata = rest.get(..length).ok_or(Error::Invalid)?;
        rest = rest.get(length..).ok_or(Error::Invalid)?;
        let message = arrow_ipc::root_as_message(metadata).map_err(|_| Error::Schema)?;
        if message
            .header_as_record_batch()
            .is_some_and(|b| b.compression().is_some())
            || message
                .header_as_dictionary_batch()
                .and_then(|b| b.data())
                .is_some_and(|b| b.compression().is_some())
        {
            return Err(Error::Unsupported);
        }
        let body = usize::try_from(message.bodyLength()).map_err(|_| Error::Invalid)?;
        if body as u64 > memory_limit {
            return Err(Error::ResourceLimit);
        }
        rest = rest.get(body..).ok_or(Error::Invalid)?;
    }
    Err(Error::Invalid) // This profile requires an explicit stream EOS.
}

fn check(token: &CancellationToken) -> Result<(), DatabaseError> {
    if token.is_cancelled() {
        let category = if matches!(
            token.reason(),
            Some(plenora_database_core::cancellation::CancellationReason::Deadline)
        ) {
            plenora_database_core::ErrorCategory::Timeout
        } else {
            plenora_database_core::ErrorCategory::Cancelled
        };
        return Err(DatabaseError::new(
            category,
            ErrorPhase::Read,
            None,
            "Database operation stopped.",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ipc_rejects_truncation_trailing_bytes_and_schema_substitution()
    -> Result<(), Box<dyn std::error::Error>> {
        let fields = plenora_database_core::protocol::contract_schema(vec![
            plenora_database_core::arrow::Field::new(
                "value",
                plenora_database_core::arrow::DataType::Utf8,
                true,
            ),
        ])?;
        let mut bytes = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut bytes, &fields)?;
            writer.finish()?;
        }
        let port = crate::Ports {
            read_schema: None,
            write_schema: None,
            primary_geometry: false,
        }
        .read();
        assert!(decode(&bytes, &port, 100, crate::MAX_BLOB as u64).is_ok());
        for length in 0..bytes.len() {
            assert!(decode(&bytes[..length], &port, 100, crate::MAX_BLOB as u64).is_err());
        }
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(decode(&trailing, &port, 100, crate::MAX_BLOB as u64).is_err());
        assert!(framing(&[255; 8], 1024).is_err());
        let mut spatial_metadata = fields.metadata().clone();
        spatial_metadata.insert("plenora.primary_geometry", "none");
        let spatial = Schema::new_with_metadata(fields.fields().clone(), spatial_metadata);
        assert!(matches!(schema(&spatial, &port), Err(Error::Unsupported)));
        assert!(
            crate::registry(
                &"0".repeat(64),
                &crate::Ports {
                    read_schema: None,
                    write_schema: None,
                    primary_geometry: true
                }
            )
            .is_err()
        );
        let mut other = port;
        other.schema_digest = Some("0".repeat(64));
        assert!(matches!(
            decode(&bytes, &other, 100, crate::MAX_BLOB as u64),
            Err(Error::Schema)
        ));
        Ok(())
    }
}
