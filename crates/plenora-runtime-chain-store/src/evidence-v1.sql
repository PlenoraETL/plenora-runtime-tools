-- Versioned supplemental ledger; existing P2.2 rows and authorities are unchanged.
CREATE TABLE IF NOT EXISTS plenora_chain_evidence (
    id text PRIMARY KEY REFERENCES plenora_chain_steps(id),
    version integer NOT NULL CHECK (version = 1),
    report bytea,
    digest bytea NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
