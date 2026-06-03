# US Equity Quant Data Platform

A cloud-oriented data engineering platform for US equity quantitative research.

This project is designed to support long-term alpha research, feature generation, model training, and reproducible backtesting using US equity market data. It also serves as a practical data engineering project using a modern data lake and analytics stack.

## Cloud Warehouse Roadmap

The project is moving from a local DuckDB-centered research workflow toward a cloud-backed analytical warehouse architecture.

## Current Local Development Layer

- Python ingests Tiingo EOD data.
- ODS raw files and DWD clean Parquet files are written locally and synced to GCS.
- dbt models currently run on DuckDB for local development.
- PostgreSQL stores local pipeline metadata.

## Target Cloud Architecture

```text
Tiingo EOD
  -> GCS ODS raw files
  -> GCS DWD clean Parquet
  -> BigQuery DWD native tables
  -> BigQuery DWS / ADS / mart tables
  -> Looker Studio dashboards
```

## Why Keep GCS DWD Parquet?

GCS stores the file-based data lake and source-of-truth data assets. The DWD Parquet layer can be used to rebuild BigQuery tables if table schemas, partitioning, clustering, or warehouse design need to change.

This means DWD data is intentionally stored in two forms:

```text
GCS DWD Parquet
  = durable file-based source of truth

BigQuery DWD native table
  = analytical warehouse copy for SQL, dbt, factor research, and dashboard queries
```

## Why Add BigQuery?

BigQuery becomes the analytical warehouse for larger-scale research workloads, including:

- Dynamic liquid universe construction
- Factor evaluation
- ML prediction panels
- Dashboard-facing marts
- Daily trade candidate generation

## dbt Targets

The dbt project supports two targets:

```text
dev   = DuckDB local development
cloud = BigQuery analytical warehouse
```

CI currently validates the `dev` target only, so that tests can run without GCP credentials.

## Near-Term Migration Path

```text
Week 5:
  BigQuery foundation and configuration

Week 6:
  Security master and Tiingo candidate pool

Week 7:
  Pilot backfill engine

Week 8:
  Full candidate backfill and DWD-to-BigQuery load

Week 9:
  Dynamic liquid universe construction

Week 10:
  dbt DWS/ADS migration to BigQuery
```

## Final Target Shape

```text
GCS:
  ods/
  dwd/
  reports/
  models/

BigQuery:
  quant_dwh.dwd_equity_price_daily
  quant_dwh.dim_security
  quant_dwh.dim_universe_membership
  quant_dwh.dws_equity_returns_daily
  quant_dwh.dws_equity_features_daily
  quant_dwh.ads_research_panel
  quant_dwh.mart_factor_summary

BigQuery Metadata:
  quant_metadata.pipeline_runs
  quant_metadata.symbol_ingestion_status
  quant_metadata.backfill_batches
  quant_metadata.data_quality_results
  quant_metadata.model_registry
```


## Current Scope

### Week 6: Security Master and Candidate Pool

Week 6 adds a Tiingo-based security master and candidate pool layer. It downloads Tiingo supported tickers, standardizes them into `dim_security`, filters US common-stock candidates, and generates pilot/full bootstrap backfill task lists for the paid Tiingo bootstrap month.

New capabilities added:

- Tiingo supported tickers ingestion to local ODS and GCS
- Standardized `dim_security` with internal `security_id`
- Active security logic using a configurable end-date grace window
- US common-stock candidate pool construction
- `pilot_500` and `bootstrap_candidates` backfill task lists
- Local Postgres metadata tables for symbol-level ingestion status and backfill batch tracking
- Tests for security master filters and backfill task-list generation

Important distinction:

```text
candidate_security_pool
  = broad list of tickers eligible for future price/volume backfill

us_liquid_100 / us_liquid_500
  = future dynamic universes generated after price and volume data are available
```

The project avoids filtering candidates based on `start_date <= research_start_date`, because that would exclude post-2020 IPOs. Instead, the candidate pool checks whether each ticker overlaps with the requested backfill window.


### Week 4: dbt Modeling Layer

Week 4 adds a dbt + DuckDB modeling layer on top of the Tiingo DWD Parquet data.

New capabilities added:

- dbt project initialized under `dbt_quant/`
- DuckDB-backed dbt profile for local analytics modeling
- Staging model over Tiingo DWD Parquet files
- DWS daily returns model with multi-horizon adjusted returns
- DWS feature model with volume, momentum, lagged return, volatility, price-position, and time-based features
- ADS ML/factor research panel with forward return labels and SPY-relative forward return labels
- dbt tests for key non-null fields
- dbt docs generation for model documentation and lineage
- CI validation for dbt project parsing

### Week 3: Metadata-Driven Incremental Batch Pipeline

Week 3 converted the manual Tiingo ingestion workflow into a single-command, metadata-driven batch pipeline.

New capabilities added:

- Unified daily price pipeline entrypoint: `python -m scripts.run_daily_price_pipeline`
- Backfill and incremental refresh modes configured through `configs/pipeline.yml`
- Refresh-window calculation based on the last successful pipeline run
- Expanded PostgreSQL metadata schema for pipeline state, data date ranges, source, dataset, mode, record counts, and error messages
- Structured pipeline status logging with `started`, `success`, and `failed` states
- DWD data quality checks for duplicate keys, invalid price rows, missing symbols, and row-count summaries
- Integrated GCS sync as part of the daily batch pipeline
- Package-style script execution using `python -m scripts.<module>` for consistent local and CI behavior
- Additional CI-tested pipeline utilities

### Week 2: Tiingo EOD Market Data Ingestion

Week 2 added a real US equity daily price ingestion pipeline using Tiingo EOD as the initial market data source.

New capabilities added:

- Config-driven ticker universe
- Tiingo EOD API ingestion
- ODS raw JSON storage
- DWD standardized Parquet output
- Adjusted OHLCV, dividend cash, and split factor fields
- DuckDB queries over real price data
- PostgreSQL metadata logging for pipeline runs
- GCS sync for ODS and DWD data
- Unit tests for transformation logic

### Week 1: Platform Bootstrap

Week 1 bootstrap completed:

- Local PostgreSQL metadata database via Docker Compose
- Synthetic daily equity price dataset
- Local Parquet-based data lake layout
- DuckDB SQL query over Parquet files
- Google Cloud Storage bucket upload
- Basic GitHub Actions CI with Ruff and pytest

## Architecture

```text
Tiingo EOD API
    -> ODS raw JSON
    -> DWD standardized Parquet
    -> dbt + DuckDB staging models
    -> DWS returns and feature models
    -> ADS ML/factor research panel
    -> DuckDB / Python research queries
    -> GCS data lake sync
    -> PostgreSQL pipeline metadata
    -> GitHub Actions CI validation
```

## Tech Stack

- Python / pandas
- Tiingo EOD API
- Parquet / pyarrow
- DuckDB
- dbt / dbt-duckdb
- PostgreSQL
- Docker Compose
- Google Cloud Storage
- GitHub Actions
- pytest
- Ruff

## Repository Structure

```text
us-equity-quant-data-platform/
  .github/
    workflows/
      ci.yml

  configs/
    universe.yml
    pipeline.yml

  data/
    ods/
      source=tiingo/
        dataset=equity_price_daily/
    dwd/
      equity_price_daily/
    dbt/
      quant.duckdb

  dbt_quant/
    dbt_project.yml
    profiles.yml
    profiles.yml.example
    models/
      staging/
        stg_tiingo__equity_price_daily.sql
        staging.yml
      dws/
        dws_equity_returns_daily.sql
        dws_equity_features_daily.sql
        dws.yml
      ads/
        ads_ml_research_panel.sql
        ads.yml

  scripts/
    __init__.py
    ingest_tiingo_prices.py
    transform_tiingo_prices_to_dwd.py
    query_real_prices_duckdb.py
    query_dbt_models.py
    sync_data_to_gcs.py
    metadata_utils.py
    pipeline_state.py
    data_quality_checks.py
    run_migrations.py
    run_daily_price_pipeline.py

  sql/
    001_create_metadata_tables.sql
    002_alter_pipeline_runs.sql

  tests/
    test_sample_prices.py
    test_tiingo_price_transform.py
    test_pipeline_state.py

  docker-compose.yml
  requirements.txt
  .env.example
  .gitignore
  README.md
```

## Local Setup

### Option A: Conda Environment

```bash
conda create -n quant-platform python=3.11
conda activate quant-platform
pip install -r requirements.txt
```

### Option B: Python venv

On macOS/Linux:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

On Windows PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Environment Variables

Create a local `.env` file based on `.env.example`:

```env
POSTGRES_DSN=postgresql://quant:quant@localhost:5432/quant_metadata
GCP_PROJECT_ID=your-gcp-project-id
GCS_BUCKET=your-gcs-bucket-name
TIINGO_API_TOKEN=your-tiingo-api-token
```

Do not commit `.env`, credential files, API keys, local data files, or generated DuckDB databases.

## Run the Daily Price Pipeline

### 1. Start local PostgreSQL

```bash
docker compose up -d postgres
```

Check that the container is running:

```bash
docker compose ps
```

Optional: connect to the database manually.

```bash
docker compose exec postgres psql -U quant -d quant_metadata
```

Inside `psql`, test the connection:

```sql
SELECT now();
```

Exit with:

```sql
\q
```

### 2. Run metadata migrations

```bash
python -m scripts.run_migrations
```

This creates the metadata schema and applies pipeline metadata migrations.

### 3. Run the unified daily price pipeline

```bash
python -m scripts.run_daily_price_pipeline
```

This pipeline performs:

1. Config loading from `configs/universe.yml` and `configs/pipeline.yml`
2. Refresh-window calculation using PostgreSQL pipeline history
3. Tiingo EOD ingestion
4. ODS raw JSON write
5. DWD standardized Parquet write
6. DWD data quality checks
7. PostgreSQL pipeline status logging
8. GCS sync for ODS and DWD files

### 4. Query DWD price data with DuckDB

```bash
python -m scripts.query_real_prices_duckdb
```

This summarizes the DWD Parquet price data by ticker and prints recent sample rows.

## dbt Modeling

Run dbt locally from the `dbt_quant/` directory:

```bash
cd dbt_quant
dbt debug --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
dbt docs generate --profiles-dir .
```

Optional: serve dbt docs locally.

```bash
dbt docs serve --profiles-dir .
```

Query the dbt-generated ML/factor research panel from the project root:

```bash
python -m scripts.query_dbt_models
```

The dbt modeling layer currently includes:

```text
stg_tiingo__equity_price_daily
    -> dws_equity_returns_daily
    -> dws_equity_features_daily
    -> ads_ml_research_panel
```

### dbt Models

| Layer | Model | Purpose |
|---|---|---|
| staging | `stg_tiingo__equity_price_daily` | Reads DWD Tiingo Parquet files and standardizes field types |
| DWS | `dws_equity_returns_daily` | Generates multi-horizon adjusted returns |
| DWS | `dws_equity_features_daily` | Generates SQL-based volume, momentum, volatility, price-position, and time features |
| ADS | `ads_ml_research_panel` | Produces ML/factor research panel with forward return labels |

### ADS Labels

The ADS research panel includes:

- `fwd_ret_1d`
- `fwd_ret_5d`
- `fwd_ret_20d`
- `label_direction_5d`
- `label_direction_20d`
- `fwd_excess_ret_20d_vs_spy`

`fwd_excess_ret_20d_vs_spy` is calculated as each ticker's future 20-day return minus SPY's future 20-day return on the same date.

## Testing and CI

Run checks locally from the project root:

```bash
python -m ruff check .
python -m pytest -q
```

Validate dbt locally:

```bash
cd dbt_quant
dbt parse --profiles-dir .
```

GitHub Actions automatically runs linting and tests on every push and pull request.

The current CI workflow performs:

1. Repository checkout
2. Python setup
3. Dependency installation
4. Ruff linting
5. pytest unit tests
6. dbt project parsing

## Data Storage Design

This project uses a data lake-style storage design.

```text
GCS bucket
  ods/    raw source data
  dwd/    cleaned and standardized data
  dws/    reusable features, factors, and labels
  ads/    research marts for ML and backtesting
```

Local development uses the same conceptual layers under `data/`.

```text
data/
  ods/    raw Tiingo JSON payloads
  dwd/    standardized Tiingo EOD Parquet files
  dbt/    local DuckDB database generated by dbt
```

PostgreSQL is used for metadata and pipeline control, not for storing large market data tables.

## Data Inspection

Parquet and DuckDB files are not usually inspected by double-clicking them. Recommended inspection methods:

### Inspect DWD Parquet with DuckDB

```bash
python -m scripts.query_real_prices_duckdb
```

### Inspect dbt-generated ADS panel

```bash
python -m scripts.query_dbt_models
```

## Design Principles

- Store analytical datasets as Parquet files.
- Use GCS as the cloud data lake.
- Use DuckDB for local analytical SQL over Parquet.
- Use dbt to manage SQL-based transformation models, tests, docs, and lineage.
- Use PostgreSQL for metadata and pipeline control.
- Keep data pipelines reproducible and testable.
- Keep raw data immutable.
- Use metadata-driven incremental refresh instead of full reloads for daily updates.
- Avoid committing credentials, local data files, generated databases, or environment-specific configuration.
- Use CI to catch schema, import, linting, testing, and dbt parsing issues early.

## Operating Conventions

Use package-style Python execution from the project root:

```bash
python -m scripts.run_migrations
python -m scripts.run_daily_price_pipeline
python -m scripts.query_real_prices_duckdb
python -m scripts.query_dbt_models
```

Prefer imports like:

```python
from scripts.data_quality_checks import run_price_quality_checks
```

Avoid relying on script-relative imports such as:

```python
from data_quality_checks import run_price_quality_checks
```

This keeps local execution and GitHub Actions behavior consistent.

## Roadmap

Next steps:

1. Complete and stabilize the dbt modeling layer.
2. Add richer Python-based technical indicator features, including RSI, MACD, ATR, ADX, Bollinger Bands, and candlestick patterns.
3. Build reusable DWS/ADS feature and label datasets for factor research and ML modeling.
4. Build a reproducible factor backtest pipeline and reporting layer.
5. Add Airflow or cron-based orchestration for scheduled daily refresh.
6. Add SEC fundamentals, FRED macro data, and Fama-French factors.
7. Add architecture diagrams, data dictionaries, and project documentation.
8. Optionally add a dashboard for data freshness, pipeline status, and factor research outputs.

## Notes

This repository is evolving from a bootstrap data engineering project into a US equity quantitative research data platform.

The current implementation supports Tiingo EOD ingestion, metadata-driven incremental refresh, DWD Parquet storage, dbt-managed returns/features/ADS modeling, PostgreSQL run logging, GCS sync, and CI validation. Future work will extend the platform with richer technical features, factor research, ML modeling, scheduled orchestration, and broader market/fundamental datasets.
