ALTER TABLE objects ADD COLUMN IF NOT EXISTS clear_metadata JSONB;
CREATE INDEX IF NOT EXISTS objects_clear_metadata_idx ON objects USING GIN (clear_metadata);
COMMENT ON COLUMN objects.clear_metadata is 'clear_metadata contains unencrypted metadata that indexed for efficient metadata search.';
