-- BigQuery metadata schema for the US equity quant data platform.
-- Replace YOUR_PROJECT_ID before running manually, or use a future Python runner.

CREATE TABLE IF NOT EXISTS `YOUR_PROJECT_ID.quant_metadata.pipeline_runs` (
    run_id STRING NOT NULL,
    pipeline_name STRING NOT NULL,
    status STRING NOT NULL,

    source STRING,
    dataset_name STRING,
    mode STRING,

    data_start_date DATE,
    data_end_date DATE,
    symbols_count INT64,

    ods_records INT64,
    dwd_records INT64,
    dws_records INT64,
    ads_records INT64,

    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP,
    error_message STRING,
    notes STRING,

    created_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(started_at)
CLUSTER BY pipeline_name, status;


CREATE TABLE IF NOT EXISTS `YOUR_PROJECT_ID.quant_metadata.symbol_ingestion_status` (
    source STRING NOT NULL,
    dataset_name STRING NOT NULL,
    ticker STRING NOT NULL,
    security_id STRING,

    requested_start_date DATE,
    requested_end_date DATE,
    last_successful_date DATE,

    status STRING NOT NULL,
    attempt_count INT64,
    last_error_message STRING,

    updated_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(updated_at)
CLUSTER BY source, dataset_name, status, ticker;


CREATE TABLE IF NOT EXISTS `YOUR_PROJECT_ID.quant_metadata.backfill_batches` (
    batch_id STRING NOT NULL,
    source STRING NOT NULL,
    dataset_name STRING NOT NULL,
    batch_name STRING,

    symbols_count INT64,
    data_start_date DATE,
    data_end_date DATE,

    status STRING NOT NULL,
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP,
    error_message STRING,
    notes STRING,

    created_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(started_at)
CLUSTER BY source, dataset_name, status;


CREATE TABLE IF NOT EXISTS `YOUR_PROJECT_ID.quant_metadata.data_quality_results` (
    check_id STRING NOT NULL,
    dataset_name STRING NOT NULL,
    table_name STRING NOT NULL,
    check_name STRING NOT NULL,

    status STRING NOT NULL,
    check_date DATE NOT NULL,

    rows_checked INT64,
    failed_rows INT64,
    metric_value FLOAT64,
    notes STRING,

    created_at TIMESTAMP NOT NULL
)
PARTITION BY check_date
CLUSTER BY dataset_name, table_name, check_name, status;


CREATE TABLE IF NOT EXISTS `YOUR_PROJECT_ID.quant_metadata.model_registry` (
    model_id STRING NOT NULL,
    model_name STRING NOT NULL,
    model_version STRING NOT NULL,

    target_name STRING,
    universe_name STRING,
    training_start_date DATE,
    training_end_date DATE,
    validation_start_date DATE,
    validation_end_date DATE,

    feature_set_name STRING,
    model_type STRING,
    model_params_json STRING,

    validation_metric_name STRING,
    validation_metric_value FLOAT64,

    artifact_uri STRING,
    promoted BOOL,

    created_at TIMESTAMP NOT NULL,
    notes STRING
)
PARTITION BY DATE(created_at)
CLUSTER BY model_name, model_version, promoted;