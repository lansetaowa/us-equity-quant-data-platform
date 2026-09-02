# Metadata model

## Active operational tables

### `metadata.pipeline_runs`

Run-level metadata for operational pipelines.

Used for:

- run status
- mode
- date range
- symbol count
- ODS/DWD row counts
- reconciliation metrics

### `metadata.price_update_window_results`

Per-run, per-window result log for daily/windowed price updates.

Used for:

- same-run resume
- run audit
- reconciliation
- per-window status inspection

This table is active and should not be dropped.

### `metadata.symbol_ingestion_status`

Latest symbol/window status used by price gap generation.

Used for:

- avoiding repeated empty/skipped windows
- determining latest checked-through date
- retrying failed windows up to the configured retry limit

This table overlaps with `price_update_window_results`, but currently serves as
the latest-status cache used by future task generation.

## Reference/catalog tables

### `metadata.datasets`

Lightweight dataset registry.

Used for documenting current generated datasets, their layers, and their storage
locations.

## Historical/backfill tables

### `metadata.backfill_batches`

Historical backfill batch tracking.

This table is retained for historical reproducibility, but is not part of the
current daily price-update path.

## Important design notes

`price_update_window_results` and `symbol_ingestion_status` overlap but have
different roles:

- `price_update_window_results` is run-scoped history.
- `symbol_ingestion_status` is latest operational state.

A future refactor may replace `symbol_ingestion_status` with a view derived from
`price_update_window_results`, but the table remains active today.