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
