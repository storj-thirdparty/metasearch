-- Copyright (C) 2025 Storj Labs, Inc.
-- See LICENSE for copying information.

ALTER TABLE objects
    ADD COLUMN IF NOT EXISTS metasearch_queued_at TIMESTAMP
    DEFAULT current_timestamp()
    ON UPDATE current_timestamp();

CREATE INDEX IF NOT EXISTS objects_metasearch_queued_at_idx ON objects (
    project_id,
    metasearch_queued_at
) WHERE metasearch_queued_at IS NOT NULL;

