# Week 5: BigQuery Foundation

## Objective

Week 5 introduces BigQuery as the future analytical warehouse for the US equity quant data platform while preserving DuckDB as the local development target.

This week does not run large-scale backfill or migrate all dbt models. It defines the cloud warehouse foundation needed for later weeks.

## Current Architecture

```text
Tiingo EOD
  -> local ODS raw files
  -> local DWD Parquet
  -> GCS sync for ODS/DWD
  -> local DuckDB dbt models
  -> local PostgreSQL metadata
```

## Target Architecture

```text
Tiingo EOD
  -> GCS ODS raw JSON/CSV
  -> GCS DWD clean Parquet
  -> BigQuery DWD native tables
  -> BigQuery DWS/ADS/mart tables
  -> Looker Studio dashboards
```

## Storage Design

### GCS

GCS remains the file-based data lake and source of truth for raw and clean file assets.

```text
gs://<bucket>/ods/
gs://<bucket>/dwd/
gs://<bucket>/reports/
gs://<bucket>/models/
```

### BigQuery

BigQuery is the analytical warehouse and dashboard-facing query layer.

```text
quant_dwh
quant_metadata
```

## Dataset Design

### quant_dwh

Main analytical tables:

```text
dwd_equity_price_daily
dim_security
dim_universe
dim_universe_membership
dws_equity_returns_daily
dws_equity_features_daily
ads_research_panel
ads_factor_eval_summary
ads_model_predictions
ads_daily_trade_candidates
```

### quant_metadata

Operational metadata tables:

```text
pipeline_runs
symbol_ingestion_status
backfill_batches
data_quality_results
model_registry
```

## DWD Loading Strategy

The DWD equity price data remains stored as Parquet in GCS:

```text
gs://<bucket>/dwd/equity_price_daily/year=*/month=*/part-*.parquet
```

Later, the same Parquet files will be loaded into a BigQuery native table:

```text
quant_dwh.dwd_equity_price_daily
```

Recommended BigQuery physical design:

```text
PARTITION BY date
CLUSTER BY ticker, security_id
```

The BigQuery DWD table is a warehouse copy of the GCS DWD Parquet data, not a replacement for the GCS data lake.

## dbt Targets

The dbt project supports two targets:

```text
dev   -> DuckDB, local development
cloud -> BigQuery, future cloud warehouse
```

CI should continue to use the `dev` target because GitHub Actions does not have GCP credentials by default.

## Week 5 Scope

### Included

- BigQuery dependency setup
- Cloud config file
- BigQuery dataset creation script
- dbt BigQuery target
- BigQuery metadata schema design
- Architecture documentation
- CI validation using DuckDB target

### Excluded

- Large Tiingo backfill
- Dynamic liquid universe construction
- Production DWD-to-BigQuery load
- Factor evaluation
- ML model training
- Looker Studio dashboard
