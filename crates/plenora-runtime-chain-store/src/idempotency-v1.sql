-- Permanent scoped host identities, separate from exact per-message receipts.
CREATE TABLE IF NOT EXISTS plenora_chain_idempotency (
    key text PRIMARY KEY,
    binding bytea NOT NULL CHECK (octet_length(binding) = 32),
    created_ms bigint NOT NULL,
    carrier bytea
);
