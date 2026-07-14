BEGIN;

-- Allow valid zero-row API responses to be represented distinctly.
-- Drop any previous status check constraint that references status.
DO $$
DECLARE
    constraint_record RECORD;
BEGIN
    FOR constraint_record IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'metadata.symbol_ingestion_status'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) ILIKE '%status%'
    LOOP
        EXECUTE format(
            'ALTER TABLE metadata.symbol_ingestion_status DROP CONSTRAINT %I',
            constraint_record.conname
        );
    END LOOP;
END $$;

ALTER TABLE metadata.symbol_ingestion_status
ADD CONSTRAINT symbol_ingestion_status_status_check
CHECK (
    status IN (
        'pending',
        'running',
        'success',
        'failed',
        'skipped',
        'empty'
    )
);

-- Mutable current state for a ticker/window.
ALTER TABLE metadata.symbol_ingestion_status
ADD COLUMN IF NOT EXISTS last_run_id TEXT;

ALTER TABLE metadata.symbol_ingestion_status
ADD COLUMN IF NOT EXISTS last_action TEXT;

ALTER TABLE metadata.symbol_ingestion_status
ADD COLUMN IF NOT EXISTS row_count BIGINT;

ALTER TABLE metadata.symbol_ingestion_status
ADD COLUMN IF NOT EXISTS first_price_date DATE;

ALTER TABLE metadata.symbol_ingestion_status
ADD COLUMN IF NOT EXISTS last_price_date DATE;

ALTER TABLE metadata.symbol_ingestion_status
ADD COLUMN IF NOT EXISTS api_called BOOLEAN;

ALTER TABLE metadata.symbol_ingestion_status
ADD COLUMN IF NOT EXISTS uploaded_to_gcs BOOLEAN;

ALTER TABLE metadata.symbol_ingestion_status
ADD COLUMN IF NOT EXISTS local_path TEXT;

ALTER TABLE metadata.symbol_ingestion_status
ADD COLUMN IF NOT EXISTS gcs_uri TEXT;

ALTER TABLE metadata.symbol_ingestion_status
ADD COLUMN IF NOT EXISTS last_completed_at TIMESTAMPTZ;

-- Run-level metadata.
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
ADD COLUMN IF NOT EXISTS symbols_count BIGINT;

ALTER TABLE metadata.pipeline_runs
ADD COLUMN IF NOT EXISTS ods_records BIGINT;

ALTER TABLE metadata.pipeline_runs
ADD COLUMN IF NOT EXISTS dwd_records BIGINT;

ALTER TABLE metadata.pipeline_runs
ADD COLUMN IF NOT EXISTS error_message TEXT;

ALTER TABLE metadata.pipeline_runs
ADD COLUMN IF NOT EXISTS audit_report_path TEXT;

ALTER TABLE metadata.pipeline_runs
ADD COLUMN IF NOT EXISTS audit_report_uri TEXT;

ALTER TABLE metadata.pipeline_runs
ADD COLUMN IF NOT EXISTS metrics JSONB
NOT NULL DEFAULT '{}'::jsonb;

-- Ensure ON CONFLICT (run_id) is valid even if the original table did not
-- explicitly define run_id as a primary key.
CREATE UNIQUE INDEX IF NOT EXISTS
idx_pipeline_runs_run_id_unique
ON metadata.pipeline_runs (run_id);

-- Immutable per-run, per-window result log.
CREATE TABLE IF NOT EXISTS metadata.price_update_window_results (
    run_id TEXT NOT NULL,

    source TEXT NOT NULL,
    dataset_name TEXT NOT NULL,
    ticker TEXT NOT NULL,
    security_id TEXT NOT NULL,

    requested_start_date DATE NOT NULL,
    requested_end_date DATE NOT NULL,

    status TEXT NOT NULL CHECK (
        status IN (
            'success',
            'empty',
            'failed',
            'skipped'
        )
    ),

    action TEXT NOT NULL CHECK (
        action IN (
            'downloaded',
            'existing',
            'empty',
            'existing_empty',
            'failed',
            'skipped'
        )
    ),

    row_count BIGINT,
    first_price_date DATE,
    last_price_date DATE,

    api_called BOOLEAN NOT NULL DEFAULT FALSE,
    uploaded_to_gcs BOOLEAN NOT NULL DEFAULT FALSE,

    local_path TEXT,
    gcs_uri TEXT,
    error_message TEXT,

    completed_at TIMESTAMPTZ NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (
        run_id,
        source,
        dataset_name,
        ticker,
        requested_start_date,
        requested_end_date
    )
);

CREATE INDEX IF NOT EXISTS
idx_price_update_window_results_run_id
ON metadata.price_update_window_results (run_id);

CREATE INDEX IF NOT EXISTS
idx_price_update_window_results_status
ON metadata.price_update_window_results (status);

CREATE INDEX IF NOT EXISTS
idx_price_update_window_results_ticker
ON metadata.price_update_window_results (ticker);

CREATE INDEX IF NOT EXISTS
idx_price_update_window_results_window
ON metadata.price_update_window_results (
    source,
    dataset_name,
    ticker,
    requested_start_date,
    requested_end_date
);

CREATE INDEX IF NOT EXISTS
idx_symbol_ingestion_status_last_run_id
ON metadata.symbol_ingestion_status (last_run_id);

CREATE INDEX IF NOT EXISTS
idx_symbol_ingestion_status_last_completed_at
ON metadata.symbol_ingestion_status (last_completed_at);

CREATE INDEX IF NOT EXISTS
idx_pipeline_runs_pipeline_data_end
ON metadata.pipeline_runs (
    pipeline_name,
    data_end_date
);

COMMIT;