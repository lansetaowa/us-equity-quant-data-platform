# Script Entry Points

Reusable implementation belongs in `quant_platform/`.

The `scripts/` package contains command-line entrypoints and a limited number
of compatibility wrappers.

## Current operational entrypoints

### Security master and candidate pool

```text
python -m scripts.ingest_tiingo_supported_tickers
python -m scripts.build_security_master
python -m scripts.build_candidate_pool
python -m scripts.generate_backfill_task_list
```

### Historical bootstrap
```
python -m scripts.validate_backfill_config
python -m scripts.init_backfill_metadata
python -m scripts.run_tiingo_backfill
python -m scripts.transform_backfill_prices_to_dwd
python -m scripts.audit_backfill_coverage
```

The formal bootstrap has already completed. These commands should not be
rerun casually.

### Cloud storage and warehouse
```
python -m scripts.sync_data_to_gcs --dry-run
python -m scripts.create_bigquery_datasets
python -m scripts.load_dwd_prices_to_bigquery
```

### Week 8.5 daily gap planning
```
python -m scripts.generate_price_gap_tasks --dry-run
```

The real windowed downloader is added after the Week 8.5R refactor.

### Development utilities

Read-only inspection commands live under scripts/dev/.

Examples:
```
python -m scripts.dev.query_security_master --limit 20

python -m scripts.dev.inspect_data \
  --parquet "data/dwd/equity_price_daily/**/*.parquet" \
  --limit 20

python -m scripts.dev.query_dbt_models
Legacy scripts
```

### Archived Week 1–3 demo scripts live under scripts/legacy/.

They are retained for project history and selected compatibility tests. They
are not the current formal bootstrap or daily-update workflow.

## Code ownership rule
```
quant_platform/ = reusable business logic
scripts/        = command entrypoints
scripts/dev/    = read-only development utilities
scripts/legacy/ = archived demo-era commands
```
New business logic should not be implemented directly inside command scripts.