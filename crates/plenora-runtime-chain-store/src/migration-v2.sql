-- Older binaries require version=1 and therefore fail closed after this atomic upgrade.
ALTER TABLE plenora_chain_schema DROP CONSTRAINT IF EXISTS plenora_chain_schema_version_check;
ALTER TABLE plenora_chain_schema ADD CONSTRAINT plenora_chain_schema_version_check CHECK (version IN (1,2));
