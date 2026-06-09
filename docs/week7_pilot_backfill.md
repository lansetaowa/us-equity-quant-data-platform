# Week 7: Pilot Tiingo Backfill Engine

## Objective

Week 7 builds and validates the pilot Tiingo backfill engine using the `pilot_500` task list from Week 6.

The goal is not to complete the full candidate bootstrap. The goal is to prove that the platform can reliably:

```text
pilot_500 task list
  -> initialize metadata tracking
  -> resume from Postgres status
  -> call Tiingo per ticker
  -> save raw ODS JSON locally
  -> transform successful ODS files to DWD Parquet
  -> sync ODS/DWD to GCS
  -> generate coverage and quality audit reports
```

## Key Design Principles

### API backfill resumes from metadata

The Tiingo API backfill runner uses `metadata.symbol_ingestion_status` to decide what to process.

```text
success -> skip
pending -> process
failed  -> retry if attempt_count < max_attempts
running -> reset if stale
```

This prevents successful symbols from being downloaded repeatedly and allows the run to recover from interruptions.

### DWD transform rebuilds from successful ODS scope

The DWD transform reads all local ODS raw files for tickers whose metadata status is `success`.

For Week 7, the transform is intentionally designed as a safe rebuild:

```text
read successful ODS files
  -> build combined DWD dataframe
  -> validate dataframe
  -> write temp DWD output
  -> validate temp output
  -> replace final DWD root only after validation passes
```

This avoids accidental one-symbol-at-a-time overwrites of monthly Parquet partitions.

### GCS remains the long-term file lake

Local files are used for development and inspection. GCS is the durable file-based data lake.

```text
local data/ods/... -> gs://<bucket>/ods/...
local data/dwd/... -> gs://<bucket>/dwd/...
```

The local `data/` prefix should never appear in GCS object names.

## Main Files Added

```text
configs/backfill.yml
scripts/validate_backfill_config.py
scripts/init_backfill_metadata.py
scripts/run_tiingo_backfill.py
scripts/transform_backfill_prices_to_dwd.py
scripts/audit_backfill_coverage.py
```

## Main Outputs

### Local ODS

```text
data/ods/source=tiingo/dataset=equity_price_daily/symbol=<TICKER>/<ticker>_prices.json
```

### Local DWD

```text
data/dwd/equity_price_daily/year=<YYYY>/month=<MM>/part-000.parquet
```

### GCS ODS

```text
gs://<bucket>/ods/source=tiingo/dataset=equity_price_daily/symbol=<TICKER>/<ticker>_prices.json
```

### GCS DWD

```text
gs://<bucket>/dwd/equity_price_daily/year=<YYYY>/month=<MM>/part-000.parquet
```

### Audit Reports

```text
reports/backfill_audit/pilot_500/coverage_summary.csv
reports/backfill_audit/pilot_500/status_summary.csv
reports/backfill_audit/pilot_500/symbol_coverage.csv
reports/backfill_audit/pilot_500/failed_or_skipped_symbols.csv
reports/backfill_audit/pilot_500/low_coverage_symbols.csv
reports/backfill_audit/pilot_500/suspicious_ticker_patterns.csv
reports/backfill_audit/pilot_500/duplicate_keys.csv
```

## Important Findings from Pilot

The pilot discovered that Tiingo `assetType = Stock` is too broad for a common-stock-only research universe.

Examples of suspicious tickers include:

```text
rights
warrants
units
preferred shares
temporary when-issued tickers
legacy / renamed / delisted tickers
```

Some suspicious tickers can still have long price history. Long history alone does not imply the instrument is a normal common stock.

To improve the future bootstrap candidate pool, ticker suffix exclusions were added for patterns such as:

```text
-R$
-RT$
-WS$
-W$
-WT$
-U$
-UN$
-P-[A-Z]$
-P[A-Z]$
```

The current `pilot_500` result is kept as the Week 7 engineering pilot result. The future `bootstrap_candidates` list is regenerated after improving candidate-pool filtering.

## Metadata Tables Used

### metadata.symbol_ingestion_status

Tracks per-symbol processing status:

```text
pending
running
success
failed
skipped
```

This table supports retry/resume.

### metadata.backfill_batches

Tracks batch-level status for the overall pilot run.

## Completion Criteria

Week 7 is complete when:

```text
1. pilot_500 task list is initialized in Postgres metadata
2. Tiingo backfill runner can process selected tickers and resume from metadata
3. raw ODS JSON files are created locally
4. DWD Parquet is safely rebuilt from successful ODS files
5. ODS and DWD sync to correct GCS prefixes
6. coverage audit reports are generated
7. candidate-pool filters are improved for future bootstrap
8. updated bootstrap_candidates list is regenerated
9. tests pass
10. branch is pushed to GitHub
```

## Next Step

Week 8 will use the improved `bootstrap_candidates` task list to perform the full paid-month bootstrap backfill.
