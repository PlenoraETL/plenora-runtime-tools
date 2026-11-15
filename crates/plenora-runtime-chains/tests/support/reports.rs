//! Test oracle reconstruction through the sole public bounded report interface.
use plenora_runtime_boundary::DurableExecutionStore;
use plenora_runtime_chains::{ChainEngine, Error, ReportCursor, Store};
pub async fn collect<S: Store + DurableExecutionStore>(
    engine: &ChainEngine<S>,
    run: &str,
) -> Result<Vec<Vec<u8>>, Error> {
    let mut next = Some(ReportCursor {
        index: 0,
        offset: 0,
    });
    let mut result: Vec<Vec<u8>> = Vec::new();
    while let Some(cursor) = next {
        let page = engine.report_page(run, cursor).await?;
        if page.total_reports == 0 {
            break;
        }
        if cursor.offset == 0 {
            result.push(Vec::new());
        }
        result.last_mut().ok_or(Error::Invalid)?.extend(page.bytes);
        next = page.next;
    }
    Ok(result)
}
