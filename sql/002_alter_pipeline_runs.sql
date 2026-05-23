ALTER TABLE metadata.pipeline_runs
ADD COLUMN IF NOT EXISTS source TEXT;

ALTER TABLE metadata.pipeline_runs
ADD COLUMN IF NOT EXISTS dataset TEXT;

ALTER TABLE metadata.pipeline_runs
ADD COLUMN IF NOT EXISTS mode TEXT;

ALTER TABLE metadata.pipeline_runs
ADD COLUMN IF NOT EXISTS data_start_date DATE;

ALTER TABLE metadata.pipeline_runs
ADD COLUMN IF NOT EXISTS data_end_date DATE;

ALTER TABLE metadata.pipeline_runs
ADD COLUMN IF NOT EXISTS symbols_count INTEGER;

ALTER TABLE metadata.pipeline_runs
ADD COLUMN IF NOT EXISTS ods_records BIGINT;

ALTER TABLE metadata.pipeline_runs
ADD COLUMN IF NOT EXISTS dwd_records BIGINT;

ALTER TABLE metadata.pipeline_runs
ADD COLUMN IF NOT EXISTS error_message TEXT;