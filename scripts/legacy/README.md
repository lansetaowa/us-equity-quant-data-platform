# Legacy Demo Pipeline

This directory contains the early sample and small-universe pipeline built
during Weeks 1–3.

It includes:

```text
synthetic sample-data generation
small configured ticker ingestion
legacy one-file-per-ticker ODS writes
legacy DWD transformation
legacy metadata-driven daily pipeline
sample GCS upload
```

These scripts are retained for historical context and selected regression
tests.

They are not used for:
```
the Week 8 formal bootstrap
the formal bootstrap_candidates DWD baseline
the Week 8.5 windowed daily update
the BigQuery formal DWD load
```
Legacy configuration files live in:
```
configs/legacy/
```
Do not use these scripts against the formal production-style data baseline.