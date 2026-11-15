-- Test-only oracle from a973b6e08a5723aa2cfebb6ccf537d204849e122; never compiled into production.

-- Source: migration.sql
CREATE TABLE IF NOT EXISTS plenora_chain_schema (
    singleton boolean PRIMARY KEY CHECK (singleton),
    version integer NOT NULL CHECK (version = 1),
    delivery_seconds bigint NOT NULL CHECK (delivery_seconds BETWEEN 1 AND 604800),
    report_seconds bigint NOT NULL CHECK (report_seconds BETWEEN delivery_seconds AND 7776000)
);
CREATE TABLE IF NOT EXISTS plenora_chain_acceptance (
    id text PRIMARY KEY,
    requires text NOT NULL,
    downgrade boolean NOT NULL,
    accepted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at timestamptz NOT NULL,
    tombstone boolean NOT NULL DEFAULT false
);
CREATE TABLE IF NOT EXISTS plenora_chain_steps (
    id text PRIMARY KEY REFERENCES plenora_chain_acceptance(id),
    scope text NOT NULL,
    step text NOT NULL,
    fingerprint bytea NOT NULL,
    fence text NOT NULL UNIQUE,
    finished boolean NOT NULL DEFAULT false,
    finished_at timestamptz,
    result_id text UNIQUE,
    report bytea,
    report_digest bytea,
    report_expired boolean NOT NULL DEFAULT false,
    UNIQUE(scope, step)
);
CREATE TABLE IF NOT EXISTS plenora_chain_keys (
    scope text NOT NULL,
    key bytea NOT NULL,
    id text NOT NULL REFERENCES plenora_chain_steps(id),
    PRIMARY KEY(scope, key)
);
CREATE TABLE IF NOT EXISTS plenora_chain_inbox (
    id text PRIMARY KEY REFERENCES plenora_chain_steps(id)
);
CREATE TABLE IF NOT EXISTS plenora_chain_outbox (
    id text PRIMARY KEY REFERENCES plenora_chain_steps(id),
    payload bytea NOT NULL,
    payload_digest bytea NOT NULL,
    delivered boolean NOT NULL DEFAULT false,
    expired boolean NOT NULL DEFAULT false,
    lease text,
    lease_until timestamptz,
    attempts bigint NOT NULL DEFAULT 0 CHECK (attempts >= 0)
);

CREATE TABLE IF NOT EXISTS plenora_chain_aliases (
    id text PRIMARY KEY REFERENCES plenora_chain_acceptance(id),
    original text NOT NULL REFERENCES plenora_chain_steps(id),
    scope text NOT NULL,
    step text NOT NULL
);

CREATE TABLE IF NOT EXISTS plenora_chain_slots (
    scope text NOT NULL,
    step text NOT NULL,
    original text NOT NULL REFERENCES plenora_chain_steps(id),
    PRIMARY KEY(scope, step)
);

-- Source: migration-v2.sql
-- Older binaries require version=1 and therefore fail closed after this atomic upgrade.
ALTER TABLE plenora_chain_schema DROP CONSTRAINT IF EXISTS plenora_chain_schema_version_check;
-- The ordered Rust schema catalogue supplies the closed version constraint atomically.

-- Source: evidence-v1.sql
-- Versioned supplemental ledger; existing P2.2 rows and authorities are unchanged.
CREATE TABLE IF NOT EXISTS plenora_chain_evidence (
    id text PRIMARY KEY REFERENCES plenora_chain_steps(id),
    version integer NOT NULL CHECK (version = 1),
    report bytea,
    digest bytea NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- Source: delivery-v1.sql
-- Versioned product receipt/journal. IDs and fingerprints survive report retention forever.
CREATE TABLE IF NOT EXISTS plenora_chain_delivery (
    key text PRIMARY KEY,
    request_id text UNIQUE,
    result_id text NOT NULL UNIQUE REFERENCES plenora_chain_acceptance(id),
    route text NOT NULL,
    stage text NOT NULL,
    delivered boolean NOT NULL DEFAULT false,
    journal bytea NOT NULL CHECK (octet_length(journal) <= 67108864)
);

-- Source: runs-v1.sql
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

-- Source: idempotency-v1.sql
-- Permanent scoped host identities, separate from exact per-message receipts.
CREATE TABLE IF NOT EXISTS plenora_chain_idempotency (
    key text PRIMARY KEY,
    binding bytea NOT NULL CHECK (octet_length(binding) = 32),
    created_ms bigint NOT NULL,
    carrier bytea
);

-- Source: work-v1.sql
-- Coordination fences do not grant permission to restart a Running domain receipt.
ALTER TABLE plenora_chain_runs ADD COLUMN IF NOT EXISTS work_owner text;
ALTER TABLE plenora_chain_runs ADD COLUMN IF NOT EXISTS work_fence bigint NOT NULL DEFAULT 0 CHECK (work_fence >= 0);
ALTER TABLE plenora_chain_runs ADD COLUMN IF NOT EXISTS work_until bigint NOT NULL DEFAULT 0 CHECK (work_until >= 0);

-- Source: resources-v1.sql
-- Independent ownership survives report retention; no cascading deletion of receipts or tombstones.
CREATE TABLE IF NOT EXISTS plenora_chain_resources (
    tenant text NOT NULL,
    run_id text NOT NULL,
    ledger bytea NOT NULL,
    next_check bigint NOT NULL,
    PRIMARY KEY (tenant, run_id)
);
CREATE INDEX IF NOT EXISTS plenora_chain_resources_due ON plenora_chain_resources (tenant, next_check);

-- Source: delivery-retention-v3.sql
-- Durable selection metadata: no request decodes every retained journal under the global lock.
ALTER TABLE plenora_chain_delivery ADD COLUMN IF NOT EXISTS created_ms bigint;
ALTER TABLE plenora_chain_delivery ADD COLUMN IF NOT EXISTS expired_report boolean;
UPDATE plenora_chain_delivery
SET created_ms=(convert_from(journal,'UTF8')::jsonb->>'created')::bigint,
    expired_report=(convert_from(journal,'UTF8')::jsonb->>'expired_report')::boolean
WHERE created_ms IS NULL OR expired_report IS NULL;
ALTER TABLE plenora_chain_delivery ALTER COLUMN created_ms SET NOT NULL;
ALTER TABLE plenora_chain_delivery ALTER COLUMN expired_report SET NOT NULL;
CREATE INDEX IF NOT EXISTS plenora_chain_delivery_report_due
    ON plenora_chain_delivery (created_ms,key) WHERE NOT expired_report;

-- Finalization performed by PostgresStore::migrate at the baseline commit.
INSERT INTO plenora_chain_schema VALUES (true,3,604800,7776000);
ALTER TABLE plenora_chain_schema ADD CONSTRAINT plenora_chain_schema_version_check CHECK (version IN (1,2,3));
