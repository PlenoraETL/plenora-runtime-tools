//! Exact Arrow handoff and real DATA Primary Geometry validation; no casts.
use crate::{Error, Invocation, digest};
use arrow_ipc::reader::StreamReader;
use arrow_schema::Schema;
use plenora_runtime_chains::contracts::Port;
use std::{
    collections::BTreeSet,
    io::{Cursor, Write},
    sync::Arc,
};
pub(crate) struct Limited {
    pub bytes: Vec<u8>,
    pub limit: usize,
}
impl Write for Limited {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other("DATA artifact limit"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(crate) fn schema(fields: &Schema, port: &Port) -> Result<(), Error> {
    if fields
        .metadata()
        .get("plenora.contract.version")
        .map(String::as_str)
        != Some("1")
    {
        return Err(Error::Schema);
    }
    let mut ids = BTreeSet::new();
    for field in fields.fields() {
        let id = field
            .metadata()
            .get("plenora.field_id")
            .ok_or(Error::Schema)?;
        if id.is_empty()
            || !id.bytes().all(|b| b.is_ascii_digit())
            || (id.len() > 1 && id.starts_with('0'))
            || !ids.insert(id)
        {
            return Err(Error::Schema);
        }
    }
    plenora_core::contract::arrow_schema::contract_from_arrow_schema(
        Arc::new(fields.clone()),
        plenora_core::crs::resolve_crs,
    )
    .map_err(|_| Error::Schema)?;
    let primary = plenora_core::contract::arrow_schema::primary_geometry_position(fields)
        .map_err(|_| Error::Schema)?
        .is_some();
    if primary != port.primary_geometry {
        return Err(Error::Schema);
    }
    let value = serde_json::to_value(fields).map_err(|_| Error::Schema)?;
    let raw = crate::encode(&value)?;
    if raw.len() > 16 * 1024 || fields.fields().len() > 32 {
        return Err(Error::ResourceLimit);
    }
    if port
        .schema_digest
        .as_ref()
        .is_some_and(|d| *d != digest(&raw))
    {
        return Err(Error::Schema);
    }
    Ok(())
}
pub(crate) fn check(
    bytes: &[u8],
    port: &Port,
    i: &Invocation,
    row_limit: u64,
) -> Result<(u64, u64), Error> {
    if bytes.len() > crate::MAX_BLOB || port.content_type != "application/vnd.apache.arrow.stream" {
        return Err(Error::Unsupported);
    }
    framing(bytes, i.reservation.limits.memory_bytes / 4)?;
    let reader = StreamReader::try_new(Cursor::new(bytes), None).map_err(|_| Error::Schema)?;
    schema(&reader.schema(), port)?;
    let columns = reader.schema().fields().len() as u64;
    let mut rows = 0_u64;
    for batch in reader {
        let batch = batch.map_err(|_| Error::Schema)?;
        for array in batch.columns() {
            array.to_data().validate_full().map_err(|_| Error::Schema)?;
        }
        rows = rows
            .checked_add(batch.num_rows() as u64)
            .ok_or(Error::ResourceLimit)?;
        if rows > row_limit
            || batch.get_array_memory_size() as u64 > i.reservation.limits.memory_bytes / 4
        {
            return Err(Error::ResourceLimit);
        }
    }
    Ok((rows, columns))
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
