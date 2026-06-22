# US Equity Quant Data Platform

A data engineering and quantitative research platform for US equities.

The project supports:

```text
security master construction
historical EOD price ingestion
resumable backfills
incremental daily price updates
local Parquet research
BigQuery analytical workloads
dbt feature and research models
future factor and ML research
```

## Architecture
```
Tiingo
  -> ODS raw JSON
  -> DWD normalized Parquet
  -> GCS durable data lake
  -> BigQuery DWD
  -> dbt DWS / ADS
  -> factor research / ML / dashboards
```
Supporting systems:
```
DuckDB    = local inspection and dbt development
Postgres  = metadata and pipeline control
GCS       = durable file-based source of truth
BigQuery  = analytical warehouse
```
See docs/architecture.md for the full design.

## Current state

The formal historical bootstrap covers:
```
2019-01-01 -> 2026-06-11
```
Formal bootstrap results:
```
task count: 8679
success:    8661
skipped:    18
failed:     0
```
The formal baseline is generated from bootstrap_candidates. The old
pilot_500 run remains an engineering artifact only.

Week 8.5 is adding safe request-windowed daily EOD updates after the frozen
bootstrap anchor.

## Repository organization
```
quant_platform/
  reusable Python implementation

scripts/
  current command entrypoints

scripts/dev/
  read-only inspection utilities

scripts/legacy/
  archived Weeks 1–3 demo pipeline

configs/
  current pipeline configuration

dbt_quant/
  dbt DuckDB and BigQuery models

tests/
  unit and regression tests

docs/
  architecture, data layout, implementation history, and runbooks
```
## Setup
### Create environment
```
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```
### Configure environment

Create .env from .env.example.

Typical variables:
```
POSTGRES_DSN
TIINGO_API_TOKEN
GCP_PROJECT_ID
GCS_BUCKET
BIGQUERY_DWH_DATASET
GCP_LOCATION
```
Do not commit .env, credentials, local data, reports, or generated
databases.

### Current workflows
Security master
```
python -m scripts.ingest_tiingo_supported_tickers
python -m scripts.build_security_master
python -m scripts.build_candidate_pool
```
Historical bootstrap

The formal bootstrap has already completed. These commands are retained for
reproducibility and should not be rerun casually:
```
python -m scripts.generate_backfill_task_list
python -m scripts.validate_backfill_config
python -m scripts.init_backfill_metadata
python -m scripts.run_tiingo_backfill
python -m scripts.transform_backfill_prices_to_dwd
python -m scripts.audit_backfill_coverage
```
Daily price gap planning

Read-only dry run:
```
python -m scripts.generate_price_gap_tasks --dry-run
```
This uses:
```
XNYS trading calendar
latest completed EOD session
formal bootstrap candidates
dim_security active-status filtering
latest local DWD date per ticker
```
GCS sync preview
```
python -m scripts.sync_data_to_gcs --dry-run
Local inspection
python -m scripts.dev.query_security_master --limit 20
python -m scripts.dev.inspect_data `
  --parquet "data/dwd/equity_price_daily/**/*.parquet" `
  --limit 20
```
dbt
```
cd dbt_quant

dbt parse --profiles-dir . --target dev
dbt run --profiles-dir . --target dev
dbt test --profiles-dir . --target dev

cd ..
```
Validation
```
python -m ruff check .
python -m pytest -q
```
CI also validates dbt parsing with the local DuckDB target.

## Data policies
```
The Week 8 bootstrap anchor remains frozen at 2026-06-11.

Do not regenerate the formal bootstrap task list casually.

New incremental raw files must use request-windowed paths.

Legacy bootstrap raw files remain readable but are not overwritten.

Formal DWD excludes pilot-only tickers.

Historical delisted securities remain in DWD but are excluded from daily API updates.
```
Documentation
```
docs/architecture.md
docs/data_layout.md
docs/refactor_plan.md
docs/script_inventory.md
docs/week7_pilot_backfill.md
docs/week8_full_bootstrap_bigquery.md
docs/week8_5_windowed_gap_fill.md
```