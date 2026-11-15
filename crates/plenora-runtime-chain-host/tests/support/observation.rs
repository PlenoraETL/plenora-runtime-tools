//! Qualification observation allowance, independent of authorized execution time.
// Admission and final observation each receive one connect + statement allowance.
// This is a finite test wait, not a bound on every statement of a primary transaction.
pub const PRIMARY_ALLOWANCE_MS: u64 = 2
    * (plenora_runtime_chain_store::PRIMARY_CONNECT_MS
        + plenora_runtime_chain_store::PRIMARY_STATEMENT_MS);

pub const fn budget(execution: u64, cleanup: u64) -> u64 {
    execution + cleanup + PRIMARY_ALLOWANCE_MS
}

#[test]
fn observation_uses_execution_cleanup_and_primary_allowance() {
    for execution in [0, 60_000, 100_000, 180_000] {
        for cleanup in [0, 1_000, 5_000] {
            assert_eq!(budget(execution, cleanup), execution + cleanup + 50_000);
        }
    }
}
