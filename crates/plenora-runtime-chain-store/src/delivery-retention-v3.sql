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
