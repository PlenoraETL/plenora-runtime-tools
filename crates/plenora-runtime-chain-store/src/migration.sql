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
