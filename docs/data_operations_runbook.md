# Quant Data Platform — Data Operations Runbook

Updated: 2026-08-17

This runbook covers two recurring data operations:

1. **Daily price catch-up**: generate new price gaps, download Tiingo prices, transform to local DWD, sync to GCS, update BigQuery, and finalize Postgres metadata.
2. **Monthly liquidity/universe update**: build the newest complete monthly liquidity metrics and next-month point-in-time liquid universe membership, then publish both to GCS and BigQuery.

The commands assume you are running from the repository root on Windows PowerShell.

---

## 0. Operating principles

The pipeline intentionally separates these layers:

```text
candidate_pool / coverage universe
  broad universe for price download

DWD prices
  canonical broad daily price table

monthly liquidity metrics
  DWS derived metrics from DWD prices

liquid universe membership
  point-in-time monthly research/trading universe
```

Daily prices should be maintained broadly for the latest candidate pool. Research, factor work, and strategies should use the point-in-time liquid universe, typically `us_liquid_500`.

Do not commit runtime/generated artifacts:

```text
data/
reports/
.env
.venv/
__pycache__/
.pytest_cache/
```

For GCS universe-output publishing, use the existing generic sync script:

```text
scripts.sync_data_to_gcs
```

For BigQuery universe-output publishing, use the generic warehouse publish adapter:

```text
scripts.update_universe_outputs_bigquery
```

---

## 1. Preflight before any data operation

```powershell
cd C:\Users\elisa\Desktop\git\codes\quant\us-equity-quant-data-platform
.\.venv\Scripts\Activate.ps1

git branch --show-current
git status --short
python -m pytest -q
python -m ruff check .
```

For operational data runs, normally use the latest merged production branch, usually `main`:

```powershell
git checkout main
git pull --ff-only origin main
```

If you are intentionally testing new code on a feature branch, stay on that branch but make sure tests and Ruff pass before running data operations.

Make sure local services and credentials are available:

```powershell
docker compose up -d postgres
docker compose exec postgres pg_isready -U quant -d quant_metadata
```

Required environment variables in `.env`:

```text
TIINGO_API_TOKEN
POSTGRES_DSN
GCP_PROJECT_ID
GCS_BUCKET
BIGQUERY_DWH_DATASET
```

Optional location variable:

```text
GCP_LOCATION=US
```

If a new SQL migration was added since your last run, apply migrations before data operations:

```powershell
python -m scripts.run_migrations
```

After the Week 9.6 metadata cleanup, this is needed once to apply:

```text
sql/006_seed_current_datasets.sql
```

---

# Part A — Optional monthly reference-data refresh

Run this when you want the broad daily price coverage universe to use a fresh Tiingo supported-ticker snapshot.

This is recommended at least monthly before the first price catch-up of the month, and also recommended when you recently changed stale-symbol eligibility logic.

Set the snapshot date:

```powershell
$SnapshotDate = "YYYY-MM-DD"
```

Example:

```powershell
$SnapshotDate = "2026-08-17"
```

## A1. Ingest Tiingo supported tickers snapshot

Dry-run GCS first:

```powershell
python -m scripts.ingest_tiingo_supported_tickers `
  --snapshot-date $SnapshotDate `
  --dry-run-gcs
```

If the output looks sane, run the actual snapshot and upload:

```powershell
python -m scripts.ingest_tiingo_supported_tickers `
  --snapshot-date $SnapshotDate
```

Expected local outputs:

```text
data/ods/source=tiingo/dataset=supported_tickers/snapshot_date=YYYY-MM-DD/supported_tickers.csv
data/ods/source=tiingo/dataset=supported_tickers/supported_tickers.csv
```

## A2. Build `dim_security` snapshot

```powershell
$SupportedSnapshotPath = `
  "data\ods\source=tiingo\dataset=supported_tickers\snapshot_date=$SnapshotDate\supported_tickers.csv"
```

Dry-run GCS:

```powershell
python -m scripts.build_security_master `
  --snapshot-date $SnapshotDate `
  --input-path $SupportedSnapshotPath `
  --dry-run-gcs
```

Actual build and upload:

```powershell
python -m scripts.build_security_master `
  --snapshot-date $SnapshotDate `
  --input-path $SupportedSnapshotPath
```

Expected local outputs:

```text
data/dwd/security_master_snapshots/snapshot_date=YYYY-MM-DD/dim_security.parquet
data/dwd/security_master/dim_security.parquet
```

## A3. Build candidate pool / coverage universe snapshot

```powershell
$DimSecuritySnapshotPath = `
  "data\dwd\security_master_snapshots\snapshot_date=$SnapshotDate\dim_security.parquet"
```

Dry-run GCS:

```powershell
python -m scripts.build_candidate_pool `
  --snapshot-date $SnapshotDate `
  --input $DimSecuritySnapshotPath `
  --dry-run-gcs
```

Actual build and upload:

```powershell
python -m scripts.build_candidate_pool `
  --snapshot-date $SnapshotDate `
  --input $DimSecuritySnapshotPath
```

Expected local outputs:

```text
data/dwd/candidate_pool_snapshots/snapshot_date=YYYY-MM-DD/candidate_security_pool.parquet
data/dwd/security_master/candidate_security_pool.parquet
```

## A4. Validate refreshed reference snapshots

Edit `snapshot_date = "YYYY-MM-DD"` inside the Python block before running.

```powershell
@'
from pathlib import Path
import pandas as pd

snapshot_date = "YYYY-MM-DD"

paths = {
    "supported_tickers_snapshot": (
        Path("data/ods/source=tiingo/dataset=supported_tickers")
        / f"snapshot_date={snapshot_date}"
        / "supported_tickers.csv"
    ),
    "dim_security_snapshot": (
        Path("data/dwd/security_master_snapshots")
        / f"snapshot_date={snapshot_date}"
        / "dim_security.parquet"
    ),
    "candidate_pool_snapshot": (
        Path("data/dwd/candidate_pool_snapshots")
        / f"snapshot_date={snapshot_date}"
        / "candidate_security_pool.parquet"
    ),
    "dim_security_latest": Path("data/dwd/security_master/dim_security.parquet"),
    "candidate_pool_latest": Path("data/dwd/security_master/candidate_security_pool.parquet"),
}

for name, path in paths.items():
    print("\n" + name)
    print(path)
    print("exists:", path.exists())

    if not path.exists():
        raise SystemExit(f"Missing {name}: {path}")

    if path.suffix == ".csv":
        df = pd.read_csv(path)
    else:
        df = pd.read_parquet(path)

    print("rows:", len(df))
    if "ticker" in df.columns:
        print("tickers:", df["ticker"].nunique())
    if "security_id" in df.columns:
        print("security_ids:", df["security_id"].nunique())
        print("duplicate security_id:", int(df["security_id"].duplicated().sum()))

print("\nReference snapshot validation passed.")
'@ | python -
```

---

# Part B — Daily price catch-up to latest available date

This is the main daily/periodic price operation.

Set a new run ID. Do not reuse an old run ID unless you are intentionally resuming an interrupted run.

```powershell
$RunId = "price_update_YYYYMMDD_catchup"
```

Example:

```powershell
$RunId = "price_update_20260817_catchup"
```

## B1. Generate price-gap tasks and complete exclusion list

Dry-run first:

```powershell
python -m scripts.generate_price_gap_tasks --dry-run
```

Confirm the output uses the latest candidate pool as the coverage universe:

```text
coverage_universe_path: data/dwd/security_master/candidate_security_pool.parquet
```

Then generate the actual task and exclusion artifacts:

```powershell
python -m scripts.generate_price_gap_tasks
```

After Week 9.6, `price_gap_excluded_symbols.parquet` is expected to explain all non-task decisions from the coverage universe, not only inactive/stale symbols.

Validate both task and exclusion outputs:

```powershell
@'
from pathlib import Path
import pandas as pd

candidate_path = Path("data/dwd/security_master/candidate_security_pool.parquet")
task_path = Path("data/dwd/security_master/price_gap_task_list.parquet")
excluded_path = Path("data/dwd/security_master/price_gap_excluded_symbols.parquet")

candidate = pd.read_parquet(candidate_path)
tasks = pd.read_parquet(task_path)
excluded = pd.read_parquet(excluded_path)

for df in [candidate, tasks, excluded]:
    if "ticker" in df.columns:
        df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    if "security_id" in df.columns:
        df["security_id"] = df["security_id"].astype(str).str.strip()

candidate_keys = candidate[["ticker", "security_id"]].drop_duplicates()
task_keys = (
    tasks[["ticker", "security_id"]].drop_duplicates()
    if not tasks.empty
    else pd.DataFrame(columns=["ticker", "security_id"])
)
excluded_keys = (
    excluded[["ticker", "security_id"]].drop_duplicates()
    if not excluded.empty
    else pd.DataFrame(columns=["ticker", "security_id"])
)

overlap = task_keys.merge(
    excluded_keys,
    on=["ticker", "security_id"],
    how="inner",
)

decided = pd.concat(
    [task_keys, excluded_keys],
    ignore_index=True,
).drop_duplicates()

missing = candidate_keys.merge(
    decided,
    on=["ticker", "security_id"],
    how="left",
    indicator=True,
)
missing = missing[missing["_merge"] == "left_only"]

print("candidate unique keys:", len(candidate_keys))
print("task unique keys:", len(task_keys))
print("excluded unique keys:", len(excluded_keys))
print("task + excluded unique keys:", len(decided))
print("task/excluded overlap:", len(overlap))
print("missing candidate decisions:", len(missing))

print("\nTask reason counts:")
print(tasks["reason"].value_counts(dropna=False).to_string() if not tasks.empty else "no tasks")

print("\nExcluded reason counts:")
print(
    excluded["daily_update_exclusion_reason"]
    .value_counts(dropna=False)
    .to_string()
    if not excluded.empty
    else "no exclusions"
)

if not tasks.empty:
    print("\nRequest window summary:")
    print("request_start min:", tasks["request_start_date"].min())
    print("request_start max:", tasks["request_start_date"].max())
    print("request_end min:", tasks["request_end_date"].min())
    print("request_end max:", tasks["request_end_date"].max())
    print("duplicate ticker/security_id:", int(tasks.duplicated(["ticker", "security_id"]).sum()))

print("\nKnown tickers:")
for ticker in ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOG", "GOOGL", "TSLA", "ATLN", "SLAI"]:
    print("\n" + "=" * 80)
    print(ticker)

    t = tasks[tasks["ticker"] == ticker] if "ticker" in tasks.columns else pd.DataFrame()
    e = excluded[excluded["ticker"] == ticker] if "ticker" in excluded.columns else pd.DataFrame()

    print("\nTasks:")
    if t.empty:
        print("none")
    else:
        task_cols = [
            c for c in [
                "ticker",
                "security_id",
                "latest_dwd_date",
                "metadata_status",
                "metadata_checked_through_date",
                "request_start_date",
                "request_end_date",
                "reason",
            ]
            if c in t.columns
        ]
        print(t[task_cols].to_string(index=False))

    print("\nExcluded:")
    if e.empty:
        print("none")
    else:
        excluded_cols = [
            c for c in [
                "ticker",
                "security_id",
                "latest_dwd_date",
                "metadata_status",
                "metadata_attempt_count",
                "metadata_checked_through_date",
                "end_date",
                "is_active",
                "daily_update_exclusion_reason",
            ]
            if c in e.columns
        ]
        print(e[excluded_cols].to_string(index=False))

if len(overlap) != 0:
    raise SystemExit("Task/excluded overlap found")

if len(missing) != 0:
    print(missing[["ticker", "security_id"]].head(50).to_string(index=False))
    raise SystemExit("Some candidate-pool rows have no task/exclusion decision")

if not tasks.empty and int(tasks.duplicated(["ticker", "security_id"]).sum()) != 0:
    raise SystemExit("Duplicate price gap tasks found")

if tasks.empty:
    print("\nNo price gaps found. Stop here; no downloader run is needed.")
else:
    print("\nPrice gap task/exclusion validation passed.")
'@ | python -
```

Expected invariant:

```text
candidate unique keys = task unique keys + excluded unique keys
task/excluded overlap = 0
missing candidate decisions = 0
```

If `tasks.empty`, stop here. Do not run the downloader, transform, GCS, BigQuery, or reconcile steps.

## B2. Confirm run ID is unused

```powershell
docker compose exec postgres `
  psql -U quant -d quant_metadata -c `
  "SELECT
       COUNT(*) AS pipeline_rows
   FROM metadata.pipeline_runs
   WHERE run_id = '$RunId';"
```

```powershell
docker compose exec postgres `
  psql -U quant -d quant_metadata -c `
  "SELECT
       COUNT(*) AS window_rows
   FROM metadata.price_update_window_results
   WHERE run_id = '$RunId';"
```

Expected:

```text
0
0
```

If either count is nonzero, either intentionally resume that run or choose a fresh run ID.

## B3. Downloader dry-run

```powershell
python -m scripts.run_tiingo_price_update `
  --run-id $RunId `
  --dry-run
```

Expected:

```text
already completed for run_id: 0
pending tasks: <task count>
planned API calls: <task count>
```

Confirm dry-run did not create a pipeline row:

```powershell
docker compose exec postgres `
  psql -U quant -d quant_metadata -c `
  "SELECT COUNT(*)
   FROM metadata.pipeline_runs
   WHERE run_id = '$RunId';"
```

Expected:

```text
0
```

## B4. Vendor freshness probe

This avoids running a large catch-up to an end date Tiingo has not published yet.

After the complete-exclusion cleanup, AAPL/MSFT/NVDA may be absent from the task list because they are already current. This probe therefore uses AAPL/MSFT/NVDA if present, otherwise it probes the first few generated tasks.

```powershell
@'
from pathlib import Path
import os

import pandas as pd
from dotenv import load_dotenv

from quant_platform.clients.tiingo import (
    TiingoClientConfig,
    fetch_daily_prices,
)
from quant_platform.prices.download import validate_price_rows_for_window

load_dotenv(dotenv_path=Path(".env").resolve())

api_token = os.environ["TIINGO_API_TOKEN"]

tasks = pd.read_parquet("data/dwd/security_master/price_gap_task_list.parquet")

if tasks.empty:
    print("No tasks in price_gap_task_list; skipping vendor freshness probe.")
    raise SystemExit(0)

tasks["ticker"] = tasks["ticker"].astype(str).str.upper().str.strip()
tasks["request_start_date"] = pd.to_datetime(tasks["request_start_date"]).dt.date
tasks["request_end_date"] = pd.to_datetime(tasks["request_end_date"]).dt.date

preferred = ["AAPL", "MSFT", "NVDA"]
available_preferred = [
    ticker for ticker in preferred if ticker in set(tasks["ticker"])
]

if available_preferred:
    probe_tickers = available_preferred[:3]
else:
    probe_tickers = tasks["ticker"].head(3).tolist()

print("Probe tickers:", probe_tickers)

config = TiingoClientConfig(
    api_token=api_token,
    timeout_seconds=60,
    max_attempts=3,
    retry_sleep_seconds=5,
)

errors = []

for ticker in probe_tickers:
    row = tasks[tasks["ticker"] == ticker]

    if row.empty:
        continue

    task = row.iloc[0]
    start_date = task["request_start_date"]
    end_date = task["request_end_date"]

    print("\n" + "=" * 80)
    print(ticker)
    print("request:", start_date, "->", end_date)

    rows = fetch_daily_prices(
        ticker=ticker,
        start_date=start_date,
        end_date=end_date,
        config=config,
    )

    first_date, last_date = validate_price_rows_for_window(
        rows,
        request_start_date=start_date,
        request_end_date=end_date,
    )

    print("rows:", len(rows))
    print("returned:", first_date, "->", last_date)

    if not rows:
        errors.append(f"{ticker} returned zero rows")
    elif last_date != end_date:
        errors.append(
            f"{ticker} latest returned date {last_date} "
            f"does not equal request_end_date {end_date}"
        )

if errors:
    print("\nERRORS")
    for error in errors:
        print("-", error)
    raise SystemExit("Vendor freshness probe failed")

print("\nVendor freshness probe passed.")
'@ | python -
```

If the vendor probe fails because Tiingo is not current through the requested end date, stop and regenerate tasks later.

## B5. Live Postgres-native price download

```powershell
python -m scripts.run_tiingo_price_update `
  --run-id $RunId `
  --upload-gcs
```

If interrupted, rerun the same command. Same-run resume should skip completed windows.

Validate Postgres results:

```powershell
docker compose exec postgres `
  psql -U quant -d quant_metadata -c `
  "SELECT
       status,
       action,
       COUNT(*) AS n,
       COUNT(*) FILTER (WHERE api_called IS TRUE) AS api_calls,
       COUNT(*) FILTER (WHERE uploaded_to_gcs IS TRUE) AS gcs_uploads,
       SUM(COALESCE(row_count, 0)) AS rows
   FROM metadata.price_update_window_results
   WHERE run_id = '$RunId'
   GROUP BY status, action
   ORDER BY status, action;"
```

```powershell
docker compose exec postgres `
  psql -U quant -d quant_metadata -c `
  "SELECT
       COUNT(*) AS total_windows,
       COUNT(*) FILTER (WHERE status = 'failed') AS failed_windows,
       COUNT(*) FILTER (WHERE status = 'skipped') AS skipped_windows,
       COUNT(*) FILTER (WHERE status = 'empty') AS empty_windows,
       SUM(COALESCE(row_count, 0)) AS returned_rows
   FROM metadata.price_update_window_results
   WHERE run_id = '$RunId';"
```

Required:

```text
failed_windows = 0
returned_rows > 0
```

Resolve failures before transforming.

## B6. Transform windows into local DWD

Dry-run:

```powershell
python -m scripts.transform_price_windows_to_dwd `
  --run-id $RunId `
  --dry-run
```

Prepare staging:

```powershell
python -m scripts.transform_price_windows_to_dwd `
  --run-id $RunId
```

If staging already exists:

```powershell
python -m scripts.transform_price_windows_to_dwd `
  --run-id $RunId `
  --overwrite-staging
```

Promote staging to final local DWD:

```powershell
python -m scripts.transform_price_windows_to_dwd `
  --run-id $RunId `
  --promote
```

Set transform report directory:

```powershell
$TransformReportDir = "reports\price_update_transform\$RunId"
```

## B7. Validate local DWD

```powershell
@'
from pathlib import Path
import pandas as pd

from quant_platform.paths.data_lake import DWD_PRICE_ROOT

root = Path(DWD_PRICE_ROOT)
df = pd.concat(
    [pd.read_parquet(path) for path in root.rglob("*.parquet")],
    ignore_index=True,
)

df["date"] = pd.to_datetime(df["date"]).dt.date

print("rows:", len(df))
print("tickers:", df["ticker"].nunique())
print("security_ids:", df["security_id"].nunique())
print("min date:", df["date"].min())
print("max date:", df["date"].max())
print("duplicate security_id/date:", int(df.duplicated(["security_id", "date"]).sum()))

if int(df.duplicated(["security_id", "date"]).sum()) != 0:
    raise SystemExit("Duplicate local DWD keys found")

print("\nLocal DWD validation passed.")
'@ | python -
```

## B8. Sync affected DWD partitions to GCS

Plan:

```powershell
python -m scripts.sync_price_dwd_partitions_to_gcs `
  --transform-report-dir $TransformReportDir
```

Apply:

```powershell
python -m scripts.sync_price_dwd_partitions_to_gcs `
  --transform-report-dir $TransformReportDir `
  --apply
```

Expected:

```text
GCS status: in_sync
```

## B9. Update BigQuery DWD

Plan:

```powershell
python -m scripts.update_price_dwd_bigquery `
  --mode plan `
  --transform-report-dir $TransformReportDir
```

Target must be your DWH price table, usually:

```text
<project_id>.<BIGQUERY_DWH_DATASET>.dwd_equity_price_daily
```

Stage:

```powershell
python -m scripts.update_price_dwd_bigquery `
  --mode stage `
  --transform-report-dir $TransformReportDir `
  --keep-staging
```

Apply:

```powershell
python -m scripts.update_price_dwd_bigquery `
  --mode apply `
  --transform-report-dir $TransformReportDir
```

## B10. Validate BigQuery parity and duplicates

```powershell
@'
from pathlib import Path
import os

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

from quant_platform.paths.data_lake import DWD_PRICE_ROOT

load_dotenv(dotenv_path=Path(".env").resolve())

project_id = os.environ["GCP_PROJECT_ID"]
dataset_id = os.environ["BIGQUERY_DWH_DATASET"]
location = os.getenv("GCP_LOCATION", "US")

local = pd.concat(
    [pd.read_parquet(path) for path in Path(DWD_PRICE_ROOT).rglob("*.parquet")],
    ignore_index=True,
)
local["date"] = pd.to_datetime(local["date"]).dt.date

client = bigquery.Client(project=project_id, location=location)
table = f"`{project_id}.{dataset_id}.dwd_equity_price_daily`"

sql = f"""
SELECT
  COUNT(*) AS row_count,
  COUNT(DISTINCT ticker) AS ticker_count,
  COUNT(DISTINCT security_id) AS security_id_count,
  MIN(date) AS min_date,
  MAX(date) AS max_date
FROM {table}
"""

bq = client.query(sql, location=location).to_dataframe().iloc[0]

print("LOCAL")
print("rows:", len(local))
print("tickers:", local["ticker"].nunique())
print("security_ids:", local["security_id"].nunique())
print("min date:", local["date"].min())
print("max date:", local["date"].max())

print("\nBIGQUERY")
print("rows:", int(bq["row_count"]))
print("tickers:", int(bq["ticker_count"]))
print("security_ids:", int(bq["security_id_count"]))
print("min date:", bq["min_date"])
print("max date:", bq["max_date"])

checks = [
    len(local) == int(bq["row_count"]),
    local["ticker"].nunique() == int(bq["ticker_count"]),
    local["security_id"].nunique() == int(bq["security_id_count"]),
    str(local["date"].min()) == str(bq["min_date"]),
    str(local["date"].max()) == str(bq["max_date"]),
]

if not all(checks):
    raise SystemExit("Local and BigQuery parity failed")

print("\nLocal/BQ parity passed.")
'@ | python -
```

Duplicate-key check:

```powershell
@'
from pathlib import Path
import os

from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv(dotenv_path=Path(".env").resolve())

project_id = os.environ["GCP_PROJECT_ID"]
dataset_id = os.environ["BIGQUERY_DWH_DATASET"]
location = os.getenv("GCP_LOCATION", "US")

client = bigquery.Client(project=project_id, location=location)
table = f"`{project_id}.{dataset_id}.dwd_equity_price_daily`"

sql = f"""
SELECT COUNT(*) AS duplicate_key_count
FROM (
  SELECT security_id, date, COUNT(*) AS n
  FROM {table}
  GROUP BY security_id, date
  HAVING n > 1
)
"""

row = client.query(sql, location=location).to_dataframe().iloc[0]
duplicate_count = int(row["duplicate_key_count"])

print("duplicate_key_count:", duplicate_count)

if duplicate_count != 0:
    raise SystemExit("BigQuery duplicate security_id/date keys found")

print("BigQuery duplicate-key validation passed.")
'@ | python -
```

## B11. Finalize price-update metadata

Load `.env` into PowerShell if needed:

```powershell
Get-Content .env | ForEach-Object {
    if ($_ -match '^\s*([^#][^=]+?)\s*=\s*(.*)\s*$') {
        $name = $matches[1].Trim()
        $value = $matches[2].Trim().Trim('"').Trim("'")
        Set-Item -Path "Env:$name" -Value $value
    }
}
```

Set audit URI:

```powershell
$AuditUri = "gs://$($env:GCS_BUCKET)/reports/price_update_audit/$RunId/"
```

Dry-run reconciliation:

```powershell
python -m scripts.reconcile_price_update_metadata `
  --run-id $RunId `
  --transform-report-dir $TransformReportDir `
  --audit-report-uri $AuditUri `
  --dry-run
```

Finalize:

```powershell
python -m scripts.reconcile_price_update_metadata `
  --run-id $RunId `
  --transform-report-dir $TransformReportDir `
  --audit-report-uri $AuditUri
```

Upload audit artifacts:

```powershell
gcloud storage cp `
  --recursive `
  "reports\price_update_audit\$RunId" `
  $AuditUri
```

Verify:

```powershell
docker compose exec postgres `
  psql -U quant -d quant_metadata -c `
  "SELECT
       run_id,
       status,
       data_start_date,
       data_end_date,
       symbols_count,
       ods_records,
       dwd_records,
       metrics #>> '{artifact_summary,bigquery_global_after,max_date}' AS bq_max_date,
       metrics #>> '{artifact_summary,bigquery_applied_transaction}' AS bq_applied
   FROM metadata.pipeline_runs
   WHERE run_id = '$RunId';"
```

Expected:

```text
status = success
dwd_records = ods_records
bq_applied = true
```

---

# Part C — Monthly liquidity metrics and universe membership update

Run this only after price catch-up completes and the newest full month is available in local DWD and BigQuery.

Set variables:

```powershell
$MetricMonth = "YYYY-MM"
$MembershipMonth = "YYYY-MM"
```

Example after completing July 2026 price data:

```powershell
$MetricMonth = "2026-07"
$MembershipMonth = "2026-08"
```

Interpretation:

```text
July 2026 liquidity metrics -> August 2026 liquid universe membership
```

## C1. Build monthly liquidity metrics incrementally

Normal monthly no-op-safe command:

```powershell
python -m scripts.build_equity_liquidity_monthly `
  --start-month $MetricMonth `
  --end-month $MetricMonth `
  --missing-only
```

Expected if partition already exists:

```text
No missing liquidity metric partitions; skipping source DWD read.
Partition count: 0
```

Expected if partition is missing:

```text
Partition count: 1
```

For targeted correction/recompute:

```powershell
python -m scripts.build_equity_liquidity_monthly `
  --start-month $MetricMonth `
  --end-month $MetricMonth `
  --replace-existing-partitions
```

Use `--overwrite` only for intentional full historical rebuilds.

## C2. Validate local liquidity metrics

Edit `metric_month = "YYYY-MM"` inside the Python block before running.

```powershell
@'
from pathlib import Path
import pandas as pd

metric_month = "YYYY-MM"
metric_date = pd.Timestamp(metric_month + "-01").date()

root = Path("data/dws/equity_liquidity_monthly")
df = pd.concat(
    [pd.read_parquet(path) for path in root.rglob("*.parquet")],
    ignore_index=True,
)

df["metric_month"] = pd.to_datetime(df["metric_month"]).dt.date

latest = df[df["metric_month"] == metric_date].copy()

print("rows:", len(latest))
print("passing filters:", int(latest["passes_liquidity_filters"].sum()))
print("duplicate metric_month/security_id:", int(latest.duplicated(["metric_month", "security_id"]).sum()))

if latest.empty:
    raise SystemExit("No rows for selected metric month")

if int(latest["passes_liquidity_filters"].sum()) < 500:
    raise SystemExit("Selected month has fewer than 500 passing liquidity rows")

if int(latest.duplicated(["metric_month", "security_id"]).sum()) != 0:
    raise SystemExit("Duplicate liquidity metric keys found")

print("\nTop 30 liquidity names:")
print(
    latest[latest["passes_liquidity_filters"]]
    .sort_values("liquidity_score", ascending=False)
    .head(30)[
        [
            "ticker",
            "security_id",
            "trading_day_count",
            "expected_trading_days",
            "trading_day_coverage",
            "median_close",
            "median_dollar_volume",
            "liquidity_score",
        ]
    ]
    .to_string(index=False)
)

print("\nLiquidity metrics validation passed.")
'@ | python -
```

## C3. Build liquid universe membership incrementally

Normal monthly no-op-safe command:

```powershell
python -m scripts.build_liquid_universe_membership `
  --start-membership-month $MembershipMonth `
  --end-membership-month $MembershipMonth `
  --missing-only
```

Expected if partitions already exist:

```text
No universe membership partitions were written.
Partition count: 0
```

Expected if partitions are missing:

```text
Partition count: 2
```

For targeted correction/recompute:

```powershell
python -m scripts.build_liquid_universe_membership `
  --start-membership-month $MembershipMonth `
  --end-membership-month $MembershipMonth `
  --replace-existing-partitions
```

## C4. Validate local liquid universe membership

Edit `membership_month = "YYYY-MM"` inside the Python block before running.

```powershell
@'
from pathlib import Path
import pandas as pd

membership_month = "YYYY-MM"
membership_date = pd.Timestamp(membership_month + "-01").date()

root = Path("data/dwd/universe_membership_monthly")
df = pd.concat(
    [pd.read_parquet(path) for path in root.rglob("*.parquet")],
    ignore_index=True,
)

for column in [
    "membership_month",
    "effective_start_date",
    "effective_end_date",
    "source_metric_month",
    "lookback_start_month",
    "lookback_end_month",
]:
    df[column] = pd.to_datetime(df[column]).dt.date

latest = df[df["membership_month"] == membership_date].copy()

print("rows:", len(latest))
print("universes:", sorted(latest["universe_name"].unique()))
print("duplicate universe/month/security_id:", int(latest.duplicated(["universe_name", "membership_month", "security_id"]).sum()))

expected_sizes = {
    "us_liquid_100": 100,
    "us_liquid_500": 500,
}

summary = (
    latest.groupby("universe_name")
    .agg(
        rows=("security_id", "size"),
        min_rank=("rank", "min"),
        max_rank=("rank", "max"),
        source_metric_month=("source_metric_month", "first"),
        lookback_start_month=("lookback_start_month", "first"),
        lookback_end_month=("lookback_end_month", "first"),
    )
    .reset_index()
)

print("\nMembership summary:")
print(summary.to_string(index=False))

for universe_name, expected_size in expected_sizes.items():
    rows = latest[latest["universe_name"] == universe_name]

    if len(rows) != expected_size:
        raise SystemExit(f"{universe_name} size mismatch: {len(rows)} != {expected_size}")

    ranks = sorted(rows["rank"].astype(int).tolist())

    if ranks != list(range(1, expected_size + 1)):
        raise SystemExit(f"{universe_name} ranks are not contiguous")

us100 = set(latest[latest["universe_name"] == "us_liquid_100"]["security_id"])
us500 = set(latest[latest["universe_name"] == "us_liquid_500"]["security_id"])

if not us100.issubset(us500):
    raise SystemExit("us_liquid_100 is not a subset of us_liquid_500")

bad = latest[
    pd.to_datetime(latest["membership_month"]).dt.to_period("M")
    != (pd.to_datetime(latest["source_metric_month"]).dt.to_period("M") + 1)
]

if not bad.empty:
    raise SystemExit("No-lookahead validation failed")

print("\nTop 30 us_liquid_100:")
print(
    latest[latest["universe_name"] == "us_liquid_100"]
    .sort_values("rank")
    .head(30)[
        [
            "rank",
            "ticker",
            "security_id",
            "liquidity_score",
            "score_observation_count",
            "source_metric_month",
            "lookback_start_month",
            "lookback_end_month",
            "source_median_dollar_volume",
        ]
    ]
    .to_string(index=False)
)

print("\nLiquid universe membership validation passed.")
'@ | python -
```

---

# Part D — Publish liquidity metrics and membership to GCS

Use existing generic GCS sync.

## D1. Sync selected liquidity month to GCS

Dry-run:

```powershell
python -m scripts.sync_data_to_gcs `
  --local-root data\dws\equity_liquidity_monthly\year=YYYY\month=MM `
  --dry-run
```

Apply:

```powershell
python -m scripts.sync_data_to_gcs `
  --local-root data\dws\equity_liquidity_monthly\year=YYYY\month=MM
```

Example for July 2026:

```powershell
python -m scripts.sync_data_to_gcs `
  --local-root data\dws\equity_liquidity_monthly\year=2026\month=07 `
  --dry-run

python -m scripts.sync_data_to_gcs `
  --local-root data\dws\equity_liquidity_monthly\year=2026\month=07
```

## D2. Sync selected membership month to GCS

Dry-run:

```powershell
python -m scripts.sync_data_to_gcs `
  --local-root data\dwd\universe_membership_monthly\universe_name=us_liquid_100\year=YYYY\month=MM `
  --local-root data\dwd\universe_membership_monthly\universe_name=us_liquid_500\year=YYYY\month=MM `
  --dry-run
```

Apply:

```powershell
python -m scripts.sync_data_to_gcs `
  --local-root data\dwd\universe_membership_monthly\universe_name=us_liquid_100\year=YYYY\month=MM `
  --local-root data\dwd\universe_membership_monthly\universe_name=us_liquid_500\year=YYYY\month=MM
```

Example for August 2026:

```powershell
python -m scripts.sync_data_to_gcs `
  --local-root data\dwd\universe_membership_monthly\universe_name=us_liquid_100\year=2026\month=08 `
  --local-root data\dwd\universe_membership_monthly\universe_name=us_liquid_500\year=2026\month=08 `
  --dry-run

python -m scripts.sync_data_to_gcs `
  --local-root data\dwd\universe_membership_monthly\universe_name=us_liquid_100\year=2026\month=08 `
  --local-root data\dwd\universe_membership_monthly\universe_name=us_liquid_500\year=2026\month=08
```

For the first full seed, sync all local universe outputs:

```powershell
python -m scripts.sync_data_to_gcs `
  --local-root data\dws\equity_liquidity_monthly `
  --local-root data\dwd\universe_membership_monthly `
  --dry-run

python -m scripts.sync_data_to_gcs `
  --local-root data\dws\equity_liquidity_monthly `
  --local-root data\dwd\universe_membership_monthly
```

---

# Part E — Publish liquidity metrics and membership to BigQuery

BigQuery tables:

```text
quant_dwh.dws_equity_liquidity_monthly
quant_dwh.dim_universe_membership_monthly
```

## E1. Check whether BigQuery tables exist

```powershell
@'
from pathlib import Path
import os

from dotenv import load_dotenv
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

load_dotenv(dotenv_path=Path(".env").resolve())

project_id = os.environ["GCP_PROJECT_ID"]
dataset_id = os.environ["BIGQUERY_DWH_DATASET"]
location = os.getenv("GCP_LOCATION", "US")

client = bigquery.Client(project=project_id, location=location)

for table_name in [
    "dws_equity_liquidity_monthly",
    "dim_universe_membership_monthly",
]:
    table_id = f"{project_id}.{dataset_id}.{table_name}"

    try:
        table = client.get_table(table_id)
        print(table_id, "exists", "rows=", table.num_rows)
    except NotFound:
        print(table_id, "MISSING")
'@ | python -
```

## E2. First-time BigQuery seed

Run only if the tables are missing or you intentionally want a full rebuild.

Make sure all local universe outputs have already been synced to GCS.

Liquidity full-replace:

```powershell
python -m scripts.update_universe_outputs_bigquery `
  --dataset liquidity_monthly `
  --mode full-replace `
  --start-month 2019-01 `
  --end-month YYYY-MM
```

Membership full-replace:

```powershell
python -m scripts.update_universe_outputs_bigquery `
  --dataset universe_membership `
  --mode full-replace `
  --start-month 2019-02 `
  --end-month YYYY-MM
```

Examples after July metrics and August membership:

```powershell
python -m scripts.update_universe_outputs_bigquery `
  --dataset liquidity_monthly `
  --mode full-replace `
  --start-month 2019-01 `
  --end-month 2026-07

python -m scripts.update_universe_outputs_bigquery `
  --dataset universe_membership `
  --mode full-replace `
  --start-month 2019-02 `
  --end-month 2026-08
```

## E3. Routine monthly BigQuery replace

Use this after the tables already exist.

Liquidity:

```powershell
python -m scripts.update_universe_outputs_bigquery `
  --dataset liquidity_monthly `
  --mode replace-months `
  --start-month $MetricMonth `
  --end-month $MetricMonth
```

Membership:

```powershell
python -m scripts.update_universe_outputs_bigquery `
  --dataset universe_membership `
  --mode replace-months `
  --start-month $MembershipMonth `
  --end-month $MembershipMonth
```

## E4. Validate BigQuery outputs

Edit the two date strings inside the Python block before running.

```powershell
@'
from pathlib import Path
import os

from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv(dotenv_path=Path(".env").resolve())

project_id = os.environ["GCP_PROJECT_ID"]
dataset_id = os.environ["BIGQUERY_DWH_DATASET"]
location = os.getenv("GCP_LOCATION", "US")

metric_month = "YYYY-MM-01"
membership_month = "YYYY-MM-01"

client = bigquery.Client(project=project_id, location=location)

queries = {
    "liquidity_month": f"""
        SELECT
          COUNT(*) AS row_count,
          COUNT(DISTINCT security_id) AS security_ids,
          SUM(CASE WHEN passes_liquidity_filters THEN 1 ELSE 0 END) AS passing_rows,
          MIN(metric_month) AS min_month,
          MAX(metric_month) AS max_month
        FROM `{project_id}.{dataset_id}.dws_equity_liquidity_monthly`
        WHERE metric_month = DATE '{metric_month}'
    """,
    "liquidity_duplicates": f"""
        SELECT COUNT(*) AS duplicate_key_count
        FROM (
          SELECT metric_month, security_id, COUNT(*) AS n
          FROM `{project_id}.{dataset_id}.dws_equity_liquidity_monthly`
          GROUP BY metric_month, security_id
          HAVING n > 1
        )
    """,
    "membership_month": f"""
        SELECT
          universe_name,
          COUNT(*) AS row_count,
          COUNT(DISTINCT security_id) AS security_ids,
          MIN(rank) AS min_rank,
          MAX(rank) AS max_rank,
          MIN(membership_month) AS min_month,
          MAX(membership_month) AS max_month,
          MIN(source_metric_month) AS source_metric_month
        FROM `{project_id}.{dataset_id}.dim_universe_membership_monthly`
        WHERE membership_month = DATE '{membership_month}'
        GROUP BY universe_name
        ORDER BY universe_name
    """,
    "membership_duplicates": f"""
        SELECT COUNT(*) AS duplicate_key_count
        FROM (
          SELECT universe_name, membership_month, security_id, COUNT(*) AS n
          FROM `{project_id}.{dataset_id}.dim_universe_membership_monthly`
          GROUP BY universe_name, membership_month, security_id
          HAVING n > 1
        )
    """,
}

for name, sql in queries.items():
    print("\n" + "=" * 100)
    print(name)
    df = client.query(sql, location=location).to_dataframe()
    print(df.to_string(index=False))
'@ | python -
```

Expected:

```text
liquidity_month:
  row_count > 0
  passing_rows > 500
  min/max month = selected metric month

liquidity_duplicates:
  duplicate_key_count = 0

membership_month:
  us_liquid_100 row_count = 100
  us_liquid_500 row_count = 500
  source_metric_month = prior metric month

membership_duplicates:
  duplicate_key_count = 0
```

---

# Part F — Complete monthly data-operation checklist

For a normal month after price catch-up:

```powershell
$MetricMonth = "YYYY-MM"
$MembershipMonth = "YYYY-MM"
```

Build local derived outputs:

```powershell
python -m scripts.build_equity_liquidity_monthly `
  --start-month $MetricMonth `
  --end-month $MetricMonth `
  --missing-only

python -m scripts.build_liquid_universe_membership `
  --start-membership-month $MembershipMonth `
  --end-membership-month $MembershipMonth `
  --missing-only
```

Sync selected outputs to GCS:

```powershell
python -m scripts.sync_data_to_gcs `
  --local-root data\dws\equity_liquidity_monthly\year=YYYY\month=MM

python -m scripts.sync_data_to_gcs `
  --local-root data\dwd\universe_membership_monthly\universe_name=us_liquid_100\year=YYYY\month=MM `
  --local-root data\dwd\universe_membership_monthly\universe_name=us_liquid_500\year=YYYY\month=MM
```

Update BigQuery:

```powershell
python -m scripts.update_universe_outputs_bigquery `
  --dataset liquidity_monthly `
  --mode replace-months `
  --start-month $MetricMonth `
  --end-month $MetricMonth

python -m scripts.update_universe_outputs_bigquery `
  --dataset universe_membership `
  --mode replace-months `
  --start-month $MembershipMonth `
  --end-month $MembershipMonth
```

Run final checks:

```powershell
python -m pytest -q
python -m ruff check .
git status --short
```

If this was only a data operation, no source-code commit is needed.

---

# Part G — Today-specific instructions for 2026-08-17

Today is 2026-08-17. August is not a complete month yet, so the normal action is:

```text
run Part A reference refresh
run Part B daily price catch-up
skip Parts C, D, and E unless intentionally recomputing July metrics / August membership
```

## G1. Use the correct branch

If the Week 9.6 cleanup has already been merged:

```powershell
git checkout main
git pull --ff-only origin main
```

If it is not merged, stay on the cleanup branch:

```powershell
git checkout week9-6-gap-metadata-cleanup
git pull
```

Then:

```powershell
python -m pytest -q
python -m ruff check .
docker compose up -d postgres
docker compose exec postgres pg_isready -U quant -d quant_metadata
python -m scripts.run_migrations
```

If `python -m ruff check .` fails because you are on a branch with unrelated lint-rule experiments, stop and clean that branch before running production data operations.

## G2. Set today's variables

```powershell
$SnapshotDate = "2026-08-17"
$RunId = "price_update_20260817_catchup"
```

## G3. Refresh today's Tiingo reference snapshot

Run Part A1 through A4 using:

```text
snapshot_date = 2026-08-17
```

This refresh is recommended today because the stale-symbol logic now depends more directly on current `dim_security.end_date`.

## G4. Generate tasks and exclusions

Run Part B1.

Expected after the Week 9.6 cleanup:

```text
candidate unique keys = task unique keys + excluded unique keys
task/excluded overlap = 0
missing candidate decisions = 0
```

If task count is zero, stop here.

## G5. Run the daily catch-up if tasks exist

Run Parts B2 through B11.

Do not run Parts C through E today unless you intentionally want to recompute and republish existing July/August universe outputs.

---

# Troubleshooting notes

## Price downloader interrupted

Rerun the exact same command with the same run ID:

```powershell
python -m scripts.run_tiingo_price_update `
  --run-id $RunId `
  --upload-gcs
```

Same-run resume should skip completed windows.

## Tiingo ticker 404

Permanent ticker-not-found responses should be recorded as terminal `skipped`, not `failed`. If failures remain in Postgres, inspect them before transforming.

## Stale symbols still appear in task list

Regenerate supported tickers, `dim_security`, and candidate pool snapshots first. Then regenerate price-gap tasks.

If stale symbols still appear, inspect:

```text
data/dwd/security_master/dim_security.parquet
metadata.symbol_ingestion_status
metadata.price_update_window_results
```

The daily eligibility cutoff should now use latest complete EOD minus `active_end_date_grace_days`, not the old bootstrap anchor date.

## `--missing-only` is slow for liquidity metrics

The script should early-exit if the selected partition already exists. If it reads all historical DWD despite `--missing-only`, check that the incremental month-pruning updates are present in:

```text
quant_platform/universe/liquidity.py
scripts/build_equity_liquidity_monthly.py
```

## BigQuery universe tables missing

Use `--mode full-replace` once to seed them. After that, use `--mode replace-months`.

## GCS objects missing for BigQuery load

Run `scripts.sync_data_to_gcs` for the selected local partitions before running BigQuery publish.

## Do not commit generated data

Generated local artifacts under `data/` and `reports/` are operational outputs. They should not be committed to Git.
