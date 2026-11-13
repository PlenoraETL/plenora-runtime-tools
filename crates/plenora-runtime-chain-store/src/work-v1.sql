-- Coordination fences do not grant permission to restart a Running domain receipt.
ALTER TABLE plenora_chain_runs ADD COLUMN IF NOT EXISTS work_owner text;
ALTER TABLE plenora_chain_runs ADD COLUMN IF NOT EXISTS work_fence bigint NOT NULL DEFAULT 0 CHECK (work_fence >= 0);
ALTER TABLE plenora_chain_runs ADD COLUMN IF NOT EXISTS work_until bigint NOT NULL DEFAULT 0 CHECK (work_until >= 0);
