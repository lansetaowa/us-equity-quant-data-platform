# US Equity Quant Data Platform

A cloud-oriented data engineering platform for US equity quantitative research.

This project is designed to support long-term alpha research, feature generation, model training, and reproducible backtesting using US equity market data. It also serves as a practical data engineering project using a modern data lake and analytics stack.

## Current Scope

### Week 2: Tiingo EOD Market Data Ingestion

Week 2 adds a real US equity daily price ingestion pipeline using Tiingo EOD as the initial market data source.

The pipeline supports:

- Config-driven ticker universe
- Tiingo EOD API ingestion
- ODS raw JSON storage
- DWD standardized Parquet output
- Adjusted OHLCV, dividend cash, and split factor fields
- DuckDB queries over real price data
- Postgres metadata logging for pipeline runs
- GCS sync for ODS and DWD data
- Unit tests for transformation logic

### Previous
Week 1 bootstrap completed:

- Local PostgreSQL metadata database via Docker Compose
- Synthetic daily equity price dataset
- Local Parquet-based data lake layout
- DuckDB SQL query over Parquet files
- Google Cloud Storage bucket upload
- Basic GitHub Actions CI with Ruff and pytest

## Architecture

```text
sample price data
    -> local Parquet
    -> DuckDB SQL query
    -> PostgreSQL metadata database
    -> Google Cloud Storage bucket
    -> GitHub Actions CI validation
```

## Tech Stack

- Python / pandas
- Parquet / pyarrow
- DuckDB
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

  data/
    dwd/
      equity_price_daily/

  scripts/
    __init__.py
    create_sample_prices.py
    query_duckdb.py
    init_metadata.py
    upload_to_gcs.py

  sql/
    001_create_metadata_tables.sql

  tests/
    test_sample_prices.py

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
```

Do not commit `.env`, credential files, API keys, or local data files.

## Run the Bootstrap Pipeline

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

### 2. Initialize metadata tables

```bash
python scripts/init_metadata.py
```

This creates the metadata schema and registers the sample dataset.

### 3. Create sample Parquet data

```bash
python scripts/create_sample_prices.py
```

Expected output:

```text
Wrote 15 rows to data/dwd/equity_price_daily/year=2025/month=01/equity_price_daily.parquet
```

### 4. Query Parquet data with DuckDB

```bash
python scripts/query_duckdb.py
```

Expected output should summarize the sample tickers, row counts, average close prices, and date ranges.

### 5. Upload sample Parquet data to GCS

Before running this step, make sure:

1. A GCS bucket already exists.
2. `GCP_PROJECT_ID` and `GCS_BUCKET` are set in `.env`.
3. You have authenticated locally with Google Cloud.

```bash
gcloud init
gcloud auth application-default login
```

Then run:

```bash
python scripts/upload_to_gcs.py
```

Verify the upload:

```bash
gcloud storage ls gs://YOUR_BUCKET_NAME/dwd/equity_price_daily/year=2025/month=01/
```

You should see:

```text
gs://YOUR_BUCKET_NAME/dwd/equity_price_daily/year=2025/month=01/equity_price_daily.parquet
```

## Testing and CI

Run checks locally:

```bash
python -m ruff check .
python -m pytest -q
```

GitHub Actions automatically runs linting and tests on every push and pull request.

The current CI workflow performs:

1. Repository checkout
2. Python setup
3. Dependency installation
4. Ruff linting
5. pytest unit tests

## Data Storage Design

This project uses a data lake-style storage design.

```text
GCS bucket
  ods/    raw source data
  dwd/    cleaned and standardized data
  dws/    reusable features, factors, and labels
  ads/    research marts for ML and backtesting
```

In the current bootstrap phase, only a sample DWD dataset is generated.

PostgreSQL is used for metadata and pipeline control, not for storing large market data tables.

## Design Principles

- Store analytical datasets as Parquet files.
- Use GCS as the cloud data lake.
- Use DuckDB for local analytical SQL over Parquet.
- Use PostgreSQL for metadata and pipeline control.
- Keep data pipelines reproducible and testable.
- Keep raw data immutable.
- Avoid committing credentials, local data files, or environment-specific configuration.
- Use CI to catch schema, import, linting, and basic data quality issues early.

## Roadmap

Next steps:

1. Ingest real US equity daily price data.
2. Build ODS and DWD layers for market data.
3. Add dbt models and data quality tests.
4. Add Airflow orchestration.
5. Generate reusable feature and label datasets.
6. Build ML and backtest research marts.
7. Add SEC fundamentals, FRED macro data, and Fama-French factors.
8. Improve documentation with architecture diagrams and data dictionaries.

## Notes

This repository is currently in the bootstrap phase. The sample price dataset is synthetic and is only used to validate the project structure, local services, Parquet storage, DuckDB queries, GCS upload, and CI workflow.
