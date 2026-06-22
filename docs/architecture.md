# Platform Architecture

## Data plane

```text
Tiingo API
  |
  +-- supported tickers
  |     -> ODS supported_tickers snapshot
  |     -> canonical dim_security
  |     -> candidate pool
  |     -> historical bootstrap tasks
  |     -> active daily update candidates
  |
  +-- EOD prices
        -> raw ODS JSON
        -> normalized DWD Parquet
        -> GCS durable data lake
        -> BigQuery native DWD table
        -> dbt DWS / ADS models
        -> factor research / ML / dashboards
```

## Control plane
```
PostgreSQL metadata
  -> pipeline runs
  -> backfill batches
  -> per-symbol ingestion status

Reports
  -> bootstrap coverage
  -> duplicate-key checks
  -> stale-symbol reports
  -> future daily gap audits
```

## Storage roles
```Local ODS/DWD:
  local development, transformation, and validation

GCS:
  durable file-based data lake and rebuild source

BigQuery:
  analytical warehouse for dbt, SQL research, and dashboards

DuckDB:
  local analytical inspection and dbt development

PostgreSQL:
  pipeline metadata and control state
```

## Code organization
```quant_platform/
  reusable implementation

scripts/
  maintained command-line entrypoints

scripts/dev/
  read-only inspection utilities

scripts/legacy/
  archived demo-era workflows

dbt_quant/
  analytical SQL transformations

configs/
  current declarative configuration

tests/
  package and command behavior validation
```

## Universe semantics
```bootstrap_candidates:
  historical price-baseline universe

daily_update_candidates:
  active or plausibly active API update universe

us_liquid_100 / us_liquid_500:
  future research universes based on liquidity
```

Historical delisted symbols remain in DWD but are excluded from repeated daily API requests.

## Raw price layout

Legacy bootstrap files remain readable:
```
symbol=AAPL/aapl_prices.json
```
New incremental files use request windows:
```
symbol=AAPL/
  request_start=2026-06-12/
    request_end=2026-06-12/
      prices.json
```
Both layouts use the same package-level normalization and DWD schema.