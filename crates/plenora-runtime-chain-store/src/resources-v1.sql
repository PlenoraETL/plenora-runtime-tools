-- Independent ownership survives report retention; no cascading deletion of receipts or tombstones.
CREATE TABLE IF NOT EXISTS plenora_chain_resources (
    tenant text NOT NULL,
    run_id text NOT NULL,
    ledger bytea NOT NULL,
    next_check bigint NOT NULL,
    PRIMARY KEY (tenant, run_id)
);
CREATE INDEX IF NOT EXISTS plenora_chain_resources_due ON plenora_chain_resources (tenant, next_check);
