//! Bounded reads of immutable raw journal evidence, including nonconforming adapter reports.
use crate::{ChainEngine, Error, Store};
use serde::{Deserialize, Serialize};
/// Byte cursor into the append-only list of complete raw receipts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReportCursor {
    /// Zero-based report index, stable as more receipts are appended.
    pub index: usize,
    /// Exact byte offset within that receipt; not a character offset.
    pub offset: usize,
}
/// At most 64 KiB of original bytes. JSON expansion remains below every facade envelope.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReportPage {
    /// Position of these bytes.
    pub cursor: ReportCursor,
    /// Complete unmodified byte slice; callers join slices of the same report index.
    pub bytes: Vec<u8>,
    /// Full byte length of this report.
    pub report_bytes: usize,
    /// Number of receipts visible at this read; later appends do not rewrite earlier indices.
    pub total_reports: usize,
    /// Next position, or none at the end observed by this read.
    pub next: Option<ReportCursor>,
}
impl<S: Store + plenora_runtime_boundary::DurableExecutionStore> ChainEngine<S> {
    /// Read immutable raw report bytes without requiring the complete aggregate in one response.
    /// # Errors
    /// Rejects a cursor beyond the report or byte boundary, missing identity or expired retention.
    pub async fn report_page(
        &self,
        run_id: &str,
        cursor: ReportCursor,
    ) -> Result<ReportPage, Error> {
        let j = self.load(run_id).await?;
        let total = j.history.len() + usize::from(j.evidence.is_some());
        let raw = if cursor.index < j.history.len() {
            j.history
                .get(cursor.index)
                .ok_or(Error::Invalid)?
                .as_slice()
        } else if cursor.index == j.history.len() && j.evidence.is_some() {
            j.evidence.as_deref().ok_or(Error::Invalid)?
        } else if cursor.index == total && cursor.offset == 0 {
            &[]
        } else {
            return Err(Error::Invalid);
        };
        if cursor.offset > raw.len() {
            return Err(Error::Invalid);
        }
        let end = cursor
            .offset
            .checked_add(64 * 1024)
            .ok_or(Error::Invalid)?
            .min(raw.len());
        let next = if end < raw.len() {
            Some(ReportCursor {
                index: cursor.index,
                offset: end,
            })
        } else if cursor.index + 1 < total {
            Some(ReportCursor {
                index: cursor.index + 1,
                offset: 0,
            })
        } else {
            None
        };
        Ok(ReportPage {
            cursor,
            bytes: raw[cursor.offset..end].to_vec(),
            report_bytes: raw.len(),
            total_reports: total,
            next,
        })
    }
}
