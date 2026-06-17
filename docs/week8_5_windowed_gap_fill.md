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