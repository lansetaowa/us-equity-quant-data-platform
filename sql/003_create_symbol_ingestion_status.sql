CREATE SCHEMA IF NOT EXISTS metadata;

CREATE TABLE IF NOT EXISTS metadata.symbol_ingestion_status (
    source TEXT NOT NULL,
    dataset_name TEXT NOT NULL,
    ticker TEXT NOT NULL,
    security_id TEXT NOT NULL,

    requested_start_date DATE NOT NULL,
    requested_end_date DATE NOT NULL,
    last_successful_date DATE,

    status TEXT NOT NULL CHECK (
        status IN ('pending', 'running', 'success', 'failed', 'skipped')
    ),

    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error_message TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (
        source,
        dataset_name,
        ticker,
        requested_start_date,
        requested_end_date
    )
);

CREATE INDEX IF NOT EXISTS idx_symbol_ingestion_status_status
ON metadata.symbol_ingestion_status (status);

CREATE INDEX IF NOT EXISTS idx_symbol_ingestion_status_ticker
ON metadata.symbol_ingestion_status (ticker);

CREATE INDEX IF NOT EXISTS idx_symbol_ingestion_status_security_id
ON metadata.symbol_ingestion_status (security_id);


CREATE TABLE IF NOT EXISTS metadata.backfill_batches (
    batch_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    dataset_name TEXT NOT NULL,
    task_list_name TEXT NOT NULL,

    symbols_count INTEGER NOT NULL,
    data_start_date DATE NOT NULL,
    data_end_date DATE NOT NULL,

    status TEXT NOT NULL CHECK (
        status IN ('pending', 'running', 'success', 'failed', 'skipped')
    ),

    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    error_message TEXT,
    notes TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_backfill_batches_status
ON metadata.backfill_batches (status);

CREATE INDEX IF NOT EXISTS idx_backfill_batches_task_list
ON metadata.backfill_batches (task_list_name);