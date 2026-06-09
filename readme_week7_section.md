### Week 7: Pilot Tiingo Backfill Engine

Week 7 adds the pilot Tiingo backfill engine using the `pilot_500` task list. The goal is to validate the end-to-end backfill workflow before running the full paid-month bootstrap.

New capabilities added:

- Backfill configuration in `configs/backfill.yml`
- Backfill config validation
- Postgres metadata initialization from task lists
- Tiingo API backfill runner with retry/resume behavior
- Per-symbol ingestion status tracking through `metadata.symbol_ingestion_status`
- Safe ODS-to-DWD transform using temp output before replacing final Parquet
- ODS and DWD sync to clean GCS prefixes
- Pilot backfill coverage and quality audit reports
- Candidate-pool filtering improvements for future bootstrap

Important design behavior:

```text
API backfill resumes from Postgres metadata.
DWD transform rebuilds from successful ODS files.
GCS stores the durable ODS/DWD file lake.
```

The pilot also revealed that Tiingo `assetType = Stock` includes non-common-stock instruments such as warrants, rights, units, preferreds, and temporary symbols. The candidate pool was therefore updated with ticker-pattern exclusions before regenerating the future `bootstrap_candidates` list.
