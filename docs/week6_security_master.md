# Week 6: Security Master and Candidate Pool

## Objective

Week 6 adds the security master and candidate pool foundation needed for large-scale Tiingo backfill.

This week does not create the final dynamic liquid universe. Instead, it prepares the metadata layer required to decide which tickers should be considered for future price/volume backfill.

## Data Flow

```text
Tiingo supported_tickers.csv
  -> local ODS
  -> GCS ODS
  -> standardized dim_security
  -> candidate_security_pool
  -> backfill task lists
  -> metadata tracking tables
```

## Key Outputs

### Local Outputs

```text
data/ods/source=tiingo/dataset=supported_tickers/supported_tickers.csv

data/dwd/security_master/dim_security.parquet
data/dwd/security_master/candidate_security_pool.parquet
data/dwd/security_master/backfill_task_list_pilot_500.parquet
data/dwd/security_master/backfill_task_list_bootstrap_candidates.parquet
```

### GCS Outputs

```text
gs://<bucket>/ods/source=tiingo/dataset=supported_tickers/supported_tickers.csv

gs://<bucket>/dwd/security_master/dim_security.parquet
gs://<bucket>/dwd/security_master/candidate_security_pool.parquet
gs://<bucket>/dwd/security_master/backfill_task_list_pilot_500.parquet
gs://<bucket>/dwd/security_master/backfill_task_list_bootstrap_candidates.parquet
```

### Postgres Metadata Tables

```text
metadata.symbol_ingestion_status
metadata.backfill_batches
```

## Security Master

The standardized security master is stored as:

```text
data/dwd/security_master/dim_security.parquet
```

It standardizes Tiingo supported ticker metadata into project-level fields:

```text
security_id
source
source_ticker
ticker
exchange
asset_type
price_currency
start_date
end_date
is_active
company_name
source_raw_symbol
loaded_at
```

The first version of `security_id` uses:

```text
security_id = tiingo:<ticker>
```

Example:

```text
tiingo:AAPL
tiingo:MSFT
tiingo:NVDA
```

## Active Security Logic

Tiingo supported tickers may show a recent market date as `end_date`, not necessarily a true delisting date. To avoid incorrectly marking active stocks as inactive after weekends, holidays, or vendor delays, the project uses an active-date grace window.

Current setting:

```text
active_end_date_grace_days = 7
```

The active logic is based on the latest available `end_date` in the supported tickers file, minus the configured grace window.

## Candidate Pool

The candidate pool is stored as:

```text
data/dwd/security_master/candidate_security_pool.parquet
```

This pool is not the final research universe. It is the broad list of securities eligible for paid-month price backfill.

Candidate filters include:

```text
asset_type == Stock
price_currency == USD
exchange in configured major US exchanges
start_date <= requested_end_date
end_date >= requested_start_date
```

The project intentionally avoids requiring:

```text
start_date <= research_start_date
```

because that would exclude post-2020 IPOs and create an unnecessary selection bias.

The current implementation requires both `start_date` and `end_date` to be present, based on manual inspection that null-date tickers were likely unusable or unavailable for this project stage.

## Backfill Task Lists

Two backfill task lists are generated.

### pilot_500

```text
data/dwd/security_master/backfill_task_list_pilot_500.parquet
```

Purpose:

```text
Validate the Week 7 backfill engine on a representative 500-ticker sample.
```

The `pilot_500` list is not a trading universe or research universe. It is an engineering pilot list.

Selection logic:

```text
1. Include known sanity-check tickers if present.
2. Fill the remaining rows with a deterministic hash sample.
3. Keep the task count at 500.
```

### bootstrap_candidates

```text
data/dwd/security_master/backfill_task_list_bootstrap_candidates.parquet
```

Purpose:

```text
Backfill all eligible candidate tickers during the paid Tiingo bootstrap month.
```

This task list is the input for the full paid-month historical backfill.

## Requested Date Fields

In task lists:

```text
requested_start_date
requested_end_date
```

mean the requested Tiingo API download window.

They do not mean the actual available data range for the ticker.

For example:

```text
ticker = AAPL
requested_start_date = 2019-01-01
requested_end_date = 2026-06-02
```

means:

```text
Ask Tiingo for AAPL price data from 2019-01-01 to 2026-06-02.
```

If a ticker IPOed after 2019, Tiingo should return only the available rows.

## Metadata Tracking

The local Postgres metadata layer now includes:

```text
metadata.symbol_ingestion_status
metadata.backfill_batches
```

### symbol_ingestion_status

Tracks per-symbol ingestion status.

Example statuses:

```text
pending
running
success
failed
skipped
```

This table will let the Week 7/8 backfill engine resume after failure, retry failed tickers, and avoid reprocessing successful tickers.

### backfill_batches

Tracks batch-level backfill status.

This table answers questions such as:

```text
Which task list was run?
How many symbols were included?
Did the batch succeed or fail?
When did it start and end?
```

## Known Limitations

- `company_name` is currently nullable because Tiingo supported tickers does not include company names.
- Sector, industry, CIK, and company name enrichment will be added later through SEC or another metadata source.
- The current candidate pool is a broad backfill candidate list, not a point-in-time research universe.
- Dynamic liquid universes such as `us_liquid_100` and `us_liquid_500` will be generated later after price and volume data have been backfilled.

## Next Step

Week 7 will use `backfill_task_list_pilot_500.parquet` to build and test the pilot backfill engine with retry/resume and metadata tracking.
