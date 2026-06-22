## Day 2 dry-run finding

The first price gap dry run showed that using the full `bootstrap_candidates` list for daily updates is too broad.

The bootstrap candidate list is a historical baseline universe. It may include symbols that existed during the bootstrap window but are no longer active. Daily windowed price updates should use an active or plausibly active update universe instead.

Examples observed during dry run:

```text
AABA
AAC
AAIN
```

These symbols had stale latest DWD dates long before the current update target and should not consume recent daily Tiingo API calls.

Refactor decision:
- bootstrap_candidates:
  historical DWD rebuild universe

- daily_update_candidates:
  active/plausibly active API update universe

This turns the dry-run discovery into a formal design decision.

## Daily update universe

The formal Week 8 bootstrap universe is historical and may contain delisted or inactive symbols. It should not be used directly as the daily API update universe.

Daily gap-fill tasks now use an active/plausibly active filter from `dim_security`:

```text
eligible if is_active is true
or end_date is null
or end_date >= bootstrap_anchor_date - active_end_date_grace_days
```

The default grace period is configured in configs/price_update.yml:
```
daily_update_universe:
  active_end_date_grace_days: 7
  ```