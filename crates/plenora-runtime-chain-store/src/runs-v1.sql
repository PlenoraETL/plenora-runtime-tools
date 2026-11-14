CREATE TABLE IF NOT EXISTS plenora_chain_runs (
    tenant text NOT NULL,
    run_id text NOT NULL,
    operation text NOT NULL CHECK (operation = 'chain.submit@2'),
    idempotency_key text,
    revision bigint NOT NULL CHECK (revision > 0),
    created_ms bigint NOT NULL,
    journal bytea,
    PRIMARY KEY (tenant, run_id),
    UNIQUE (tenant, operation, idempotency_key)
);
