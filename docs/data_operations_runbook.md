# Quant Data Platform — Data Operations Runbook

Updated: 2026-09-01

This runbook covers four recurring data operations:

1. **Daily stock price catch-up**: generate new stock price gaps, download Tiingo prices, transform to local DWD, sync affected DWD partitions to GCS, update BigQuery, and finalize Postgres metadata.
2. **Daily market-context price catch-up**: download and transform ETF/index-proxy market-context prices into a separate DWD product.
3. **Monthly liquidity/universe update**: build the newest complete monthly liquidity metrics and next-month point-in-time liquid universe membership, then publish both to GCS and BigQuery.
4. **Research derived-layer refresh**: build equity features, market-context features, forward-return labels, and the point-in-time research panel, then publish stable research outputs to GCS and BigQuery.

The commands assume you are running from the repository root on Windows PowerShell.

---

## 0. Operating principles

The pipeline intentionally separates these layers:

```text
candidate_pool / coverage universe
  broad stock universe for daily stock price download

DWD stock prices
  canonical broad daily stock price table

DWD market-context prices
  separate ETF/index-proxy price product, not part of candidate_pool

monthly liquidity metrics
  DWS derived metrics from DWD stock prices

liquid universe membership
  point-in-time monthly research/trading universe

research derived layer
  features, technical factors, labels, and point-in-time research panel
```

Daily stock prices should be maintained broadly for the latest candidate pool. Research, factor work, and strategies should use the point-in-time liquid universe, typically `us_liquid_500`.

Market-context ETFs such as `SPY`, `QQQ`, `VOO`, `IWM`, `DIA`, `TLT`, and sector ETFs should **not** be added to `candidate_pool` and should **not** be written into `data/dwd/equity_price_daily/`. They are maintained as a separate product:

```text
data/dwd/market_context_price_daily/context_set=core_v1/...
data/dws/market_context_features_daily/context_set=core_v1/...
```

Do not commit runtime/generated artifacts:

```text
data/
reports/
.env
.venv/
__pycache__/
.pytest_cache/
```

For GCS publishing, use the existing generic sync script:

```text
scripts.sync_data_to_gcs
```

For BigQuery liquidity/universe publishing, use:

```text
scripts.update_universe_outputs_bigquery
```

For BigQuery research-output publishing, use:

```text
scripts.update_research_outputs_bigquery
```

Run broad `ruff --fix` only when intentionally doing lint cleanup. For normal feature/data operations, prefer targeted Ruff checks to avoid unrelated churn.

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

If intentionally testing new code on a feature branch, stay on that branch, but make sure tests and Ruff pass before running data operations.

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

## 2. Standard operation variables

For a normal daily/monthly run, set one shared operation timestamp:

```powershell
$RunStamp = (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssZ')

$OperationId = "daily_data_$RunStamp"
$PriceRunId = "price_update_$RunStamp"
$RunId = $PriceRunId
$MarketContextPriceRunId = "market_context_price_$OperationId"
```

For a month-end liquidity/universe refresh, set:

```powershell
$MetricMonth = "YYYY-MM"
$MembershipMonth = "YYYY-MM"

$MetricYear = $MetricMonth.Substring(0, 4)
$MetricMonthNum = $MetricMonth.Substring(5, 2)

$MembershipYear = $MembershipMonth.Substring(0, 4)
$MembershipMonthNum = $MembershipMonth.Substring(5, 2)
```

Example after August 2026 has complete stock DWD data:

```powershell
$MetricMonth = "2026-08"
$MembershipMonth = "2026-09"
```

Interpretation:

```text
August 2026 liquidity metrics -> September 2026 liquid universe membership
```

For a research refresh, set:

```powershell
$RefreshStamp = (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssZ')
$ResearchOperationId = "research_refresh_$RefreshStamp"

$StartMonth = "2019-01"
$EndMonth = "YYYY-MM"
$PanelStartMonth = "2019-02"
$PanelEndMonth = "YYYY-MM"

$EquityFeatureRunId = "equity_features_$ResearchOperationId"
$MarketContextFeatureRunId = "market_context_features_$ResearchOperationId"
$LabelRunId = "equity_labels_$ResearchOperationId"
$PanelRunId = "equity_research_panel_$ResearchOperationId"
```

Use the latest common available month across stock prices, market-context prices, and membership as the panel end month.

---

# Part A — Optional monthly reference-data refresh

Run this when you want the broad daily stock price coverage universe to use a fresh Tiingo supported-ticker snapshot.

This is recommended at least monthly before the first stock price catch-up of the month, and also recommended after stale-symbol eligibility logic changes.

Set the snapshot date:

```powershell
$SnapshotDate = "YYYY-MM-DD"
```

Example:

```powershell
$SnapshotDate = "2026-09-01"
```

## A1. Ingest Tiingo supported tickers snapshot

Dry-run GCS first:

```powershell
python -m scripts.ingest_tiingo_supported_tickers `
  --snapshot-date $SnapshotDate `
  --dry-run-gcs
```

Actual snapshot and upload:

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

```powershell
$env:SNAPSHOT_DATE = $SnapshotDate

@'
from pathlib import Path
import os

import pandas as pd

snapshot_date = os.environ["SNAPSHOT_DATE"]

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
    "candidate_pool_latest": Path(
        "data/dwd/security_master/candidate_security_pool.parquet"
    ),
}

for name, path in paths.items():
    print("\n" + "=" * 100)
    print(name)
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

# Part B — Daily stock price catch-up to latest available date

This is the main daily/periodic stock price operation.

Set a fresh run ID if not already set:

```powershell
$RunStamp = (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssZ')
$OperationId = "daily_data_$RunStamp"
$PriceRunId = "price_update_$RunStamp"
$RunId = $PriceRunId
```

Do not reuse an old run ID unless intentionally resuming an interrupted run.

## B1. Generate price-gap tasks and complete exclusion list

Dry-run first:

```powershell
python -m scripts.generate_price_gap_tasks --dry-run
```

Confirm the output uses the latest candidate pool as the coverage universe:

```text
coverage_universe_path: data/dwd/security_master/candidate_security_pool.parquet
```

Generate the actual task and exclusion artifacts:

```powershell
python -m scripts.generate_price_gap_tasks
```

After the Week 9.6 cleanup, `price_gap_excluded_symbols.parquet` is expected to explain all non-task decisions from the coverage universe, not only inactive/stale symbols.

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
    print(
        "duplicate ticker/security_id:",
        int(tasks.duplicated(["ticker", "security_id"]).sum()),
    )

print("\nKnown tickers:")
for ticker in ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOG", "GOOGL", "TSLA"]:
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
    print("\nNo price gaps found. Skip downloader/transform/GCS/BQ/reconcile for stock prices.")
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

If `tasks.empty`, skip B2 through B11 for stock prices. You may still run market-context catch-up, monthly liquidity/universe refresh, and research refresh if those are needed.

## B2. Confirm run ID is unused

```powershell
docker compose exec postgres `
  psql -U quant -d quant_metadata -c `
  "SELECT COUNT(*) AS pipeline_rows
   FROM metadata.pipeline_runs
   WHERE run_id = '$RunId';"
```

```powershell
docker compose exec postgres `
  psql -U quant -d quant_metadata -c `
  "SELECT COUNT(*) AS window_rows
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

This avoids running a large catch-up to an end date Tiingo has not published yet. AAPL/MSFT/NVDA may be absent from the task list because they are already current, so the probe uses preferred tickers if present and otherwise probes the first few generated tasks.

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
available_preferred = [ticker for ticker in preferred if ticker in set(tasks["ticker"])]

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

If interrupted, rerun the same command with the same run ID. Same-run resume should skip completed windows.

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

## B7. Validate local stock DWD

```powershell
@'
from pathlib import Path
import pandas as pd

from quant_platform.paths.data_lake import DWD_PRICE_ROOT

root = Path(DWD_PRICE_ROOT)
files = sorted(root.rglob("*.parquet"))

if not files:
    raise SystemExit(f"No DWD price files found under {root}")

df = pd.concat([pd.read_parquet(path) for path in files], ignore_index=True)
df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

print("rows:", len(df))
print("tickers:", df["ticker"].nunique())
print("security_ids:", df["security_id"].nunique())
print("min date:", df["date"].min())
print("max date:", df["date"].max())
print("duplicate security_id/date:", int(df.duplicated(["security_id", "date"]).sum()))

if int(df.duplicated(["security_id", "date"]).sum()) != 0:
    raise SystemExit("Duplicate local DWD keys found")

print("\nLocal stock DWD validation passed.")
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

## B10. Validate BigQuery parity and duplicate keys

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
local["date"] = pd.to_datetime(local["date"], errors="coerce").dt.date

client = bigquery.Client(project=project_id, location=location)
table = f"`{project_id}.{dataset_id}.dwd_equity_price_daily`"

summary_sql = f"""
SELECT
  COUNT(*) AS row_count,
  COUNT(DISTINCT ticker) AS ticker_count,
  COUNT(DISTINCT security_id) AS security_id_count,
  MIN(date) AS min_date,
  MAX(date) AS max_date
FROM {table}
"""

bq = client.query(summary_sql, location=location).to_dataframe().iloc[0]

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

duplicate_sql = f"""
SELECT COUNT(*) AS duplicate_key_count
FROM (
  SELECT security_id, date, COUNT(*) AS n
  FROM {table}
  GROUP BY security_id, date
  HAVING n > 1
)
"""

dupe = client.query(duplicate_sql, location=location).to_dataframe().iloc[0]
duplicate_count = int(dupe["duplicate_key_count"])
print("\nBQ duplicate security_id/date:", duplicate_count)

if duplicate_count != 0:
    raise SystemExit("BigQuery duplicate security_id/date keys found")

print("\nLocal/BQ parity and duplicate-key validation passed.")
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

# Part C — Daily market-context price catch-up

Run this after stock price catch-up, or independently when market-context ETF prices are stale.

Market-context price data is separate from stock `candidate_pool` and stock `equity_price_daily`.

Set a run ID if not already set:

```powershell
$MarketContextPriceRunId = "market_context_price_$OperationId"
```

## C1. Build market-context price data

Dry-run:

```powershell
python -m scripts.build_market_context_price_daily `
  --run-id $MarketContextPriceRunId `
  --operation-id $OperationId `
  --dry-run
```

Expected incremental behavior:

```text
full_refresh: False
existing max date: <latest local DWD market-context date>
planned tasks start from existing max date + 1
```

`price_start_date` in the output is the historical seed start date. It is used for first-time/full-refresh builds only; the actual incremental request windows are the `Planned tasks` paths and printed request dates.

Actual run:

```powershell
python -m scripts.build_market_context_price_daily `
  --run-id $MarketContextPriceRunId `
  --operation-id $OperationId
```

If you intentionally want to rebuild all market-context prices:

```powershell
python -m scripts.build_market_context_price_daily `
  --run-id $MarketContextPriceRunId `
  --operation-id $OperationId `
  --full-refresh
```

## C2. Validate local market-context DWD prices

```powershell
@'
from pathlib import Path

import pandas as pd

from quant_platform.research.config import load_market_context_config

config = load_market_context_config("configs/market_context.yml")

root = Path(config.price_dwd_root) / f"context_set={config.context_set}"
files = sorted(root.rglob("*.parquet"))

print("root:", root)
print("files:", len(files))

if not files:
    raise SystemExit("No market context price files found")

df = pd.concat([pd.read_parquet(path) for path in files], ignore_index=True)
df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

print("rows:", len(df))
print("tickers:", df["ticker"].nunique())
print("security_ids:", df["security_id"].nunique())
print("min date:", df["date"].min())
print("max date:", df["date"].max())
print(
    "duplicate context/security/date:",
    int(df.duplicated(["context_set", "security_id", "date"]).sum()),
)

if df["ticker"].nunique() != 17:
    raise SystemExit(f"Expected 17 market context tickers, got {df['ticker'].nunique()}")

if int(df.duplicated(["context_set", "security_id", "date"]).sum()) != 0:
    raise SystemExit("Duplicate market context price keys found")

print("\nMarket context price validation passed.")
'@ | python -
```

## C3. Check market-context price report

```powershell
Get-Content "reports\market_context_price_build\$MarketContextPriceRunId\summary.json"
Import-Csv "reports\market_context_price_build\$MarketContextPriceRunId\partition_manifest.csv" | Format-Table
Import-Csv "reports\market_context_price_build\$MarketContextPriceRunId\download_results.csv" | Format-Table
```

If the report status is `no_op`, no downstream market-context feature update is necessary unless you are doing a full research refresh.

---

# Part D — Monthly liquidity metrics and universe membership update

Run this only after stock price catch-up completes and the newest full month is available in local DWD and BigQuery.

Set variables:

```powershell
$MetricMonth = "YYYY-MM"
$MembershipMonth = "YYYY-MM"

$MetricYear = $MetricMonth.Substring(0, 4)
$MetricMonthNum = $MetricMonth.Substring(5, 2)

$MembershipYear = $MembershipMonth.Substring(0, 4)
$MembershipMonthNum = $MembershipMonth.Substring(5, 2)
```

Example after completing August 2026 stock price data:

```powershell
$MetricMonth = "2026-08"
$MembershipMonth = "2026-09"
```

Interpretation:

```text
August 2026 liquidity metrics -> September 2026 liquid universe membership
```

## D0. Confirm the metric month is complete

```powershell
@'
from pathlib import Path

import pandas as pd

from quant_platform.paths.data_lake import DWD_PRICE_ROOT
from quant_platform.research.config import load_market_context_config

market_config = load_market_context_config("configs/market_context.yml")


def read_max_date(root: Path) -> object:
    files = sorted(root.rglob("*.parquet"))

    if not files:
        raise SystemExit(f"No parquet files under {root}")

    pieces = [pd.read_parquet(path, columns=["date"]) for path in files]
    df = pd.concat(pieces, ignore_index=True)

    return pd.to_datetime(df["date"], errors="coerce").dt.date.max()


stock_max = read_max_date(Path(DWD_PRICE_ROOT))
market_context_max = read_max_date(
    Path(market_config.price_dwd_root) / f"context_set={market_config.context_set}"
)

research_max = min(stock_max, market_context_max)
research_end_month = research_max.strftime("%Y-%m")

print("STOCK_DWD_MAX_DATE=", stock_max)
print("MARKET_CONTEXT_DWD_MAX_DATE=", market_context_max)
print("RESEARCH_MAX_DATE=", research_max)
print("RESEARCH_END_MONTH=", research_end_month)
'@ | python -
```

For a month-end liquidity refresh, make sure `STOCK_DWD_MAX_DATE` covers the last trading session of `$MetricMonth`.

## D1. Build monthly liquidity metrics

Normal no-op-safe command:

```powershell
python -m scripts.build_equity_liquidity_monthly `
  --start-month $MetricMonth `
  --end-month $MetricMonth `
  --missing-only
```

For targeted correction/recompute, especially if you may have built a partial month earlier:

```powershell
python -m scripts.build_equity_liquidity_monthly `
  --start-month $MetricMonth `
  --end-month $MetricMonth `
  --replace-existing-partitions
```

Use `--overwrite` only for intentional full historical rebuilds.

## D2. Validate local liquidity metrics

```powershell
$env:METRIC_MONTH = $MetricMonth

@'
from pathlib import Path
import os

import pandas as pd

metric_month = os.environ["METRIC_MONTH"]
metric_date = pd.Timestamp(metric_month + "-01").date()

root = Path("data/dws/equity_liquidity_monthly")
files = sorted(root.rglob("*.parquet"))

if not files:
    raise SystemExit("No liquidity metric files found")

df = pd.concat([pd.read_parquet(path) for path in files], ignore_index=True)
df["metric_month"] = pd.to_datetime(df["metric_month"], errors="coerce").dt.date

latest = df[df["metric_month"] == metric_date].copy()

print("metric_month:", metric_month)
print("rows:", len(latest))
print("security_ids:", latest["security_id"].nunique() if not latest.empty else 0)
print(
    "passing filters:",
    int(latest["passes_liquidity_filters"].sum()) if not latest.empty else 0,
)
print(
    "duplicate metric_month/security_id:",
    int(latest.duplicated(["metric_month", "security_id"]).sum())
    if not latest.empty
    else 0,
)

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

## D3. Build liquid universe membership

Normal no-op-safe command:

```powershell
python -m scripts.build_liquid_universe_membership `
  --start-membership-month $MembershipMonth `
  --end-membership-month $MembershipMonth `
  --missing-only
```

For targeted correction/recompute:

```powershell
python -m scripts.build_liquid_universe_membership `
  --start-membership-month $MembershipMonth `
  --end-membership-month $MembershipMonth `
  --replace-existing-partitions
```

## D4. Validate local liquid universe membership

```powershell
$env:MEMBERSHIP_MONTH = $MembershipMonth
$env:METRIC_MONTH = $MetricMonth

@'
from pathlib import Path
import os

import pandas as pd

membership_month = os.environ["MEMBERSHIP_MONTH"]
metric_month = os.environ["METRIC_MONTH"]

membership_date = pd.Timestamp(membership_month + "-01").date()
metric_date = pd.Timestamp(metric_month + "-01").date()

root = Path("data/dwd/universe_membership_monthly")
files = sorted(root.rglob("*.parquet"))

if not files:
    raise SystemExit("No universe membership files found")

df = pd.concat([pd.read_parquet(path) for path in files], ignore_index=True)

for column in [
    "membership_month",
    "effective_start_date",
    "effective_end_date",
    "source_metric_month",
    "lookback_start_month",
    "lookback_end_month",
]:
    if column in df.columns:
        df[column] = pd.to_datetime(df[column], errors="coerce").dt.date

latest = df[df["membership_month"] == membership_date].copy()

print("membership_month:", membership_month)
print("source metric month expected:", metric_month)
print("rows:", len(latest))
print("universes:", sorted(latest["universe_name"].unique()) if not latest.empty else [])
print(
    "duplicate universe/month/security_id:",
    int(latest.duplicated(["universe_name", "membership_month", "security_id"]).sum())
    if not latest.empty
    else 0,
)

if latest.empty:
    raise SystemExit("No membership rows for selected month")

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

    source_months = set(rows["source_metric_month"])

    if source_months != {metric_date}:
        raise SystemExit(
            f"{universe_name} source_metric_month mismatch: "
            f"{source_months} != {metric_date}"
        )

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

# Part E — Publish liquidity metrics and membership to GCS / BigQuery

Use this after Part D local validation passes.

## E1. Sync selected liquidity month to GCS

Dry-run:

```powershell
python -m scripts.sync_data_to_gcs `
  --local-root data\dws\equity_liquidity_monthly\year=$MetricYear\month=$MetricMonthNum `
  --dry-run
```

Apply:

```powershell
python -m scripts.sync_data_to_gcs `
  --local-root data\dws\equity_liquidity_monthly\year=$MetricYear\month=$MetricMonthNum
```

## E2. Sync selected membership month to GCS

Dry-run:

```powershell
python -m scripts.sync_data_to_gcs `
  --local-root data\dwd\universe_membership_monthly\universe_name=us_liquid_100\year=$MembershipYear\month=$MembershipMonthNum `
  --local-root data\dwd\universe_membership_monthly\universe_name=us_liquid_500\year=$MembershipYear\month=$MembershipMonthNum `
  --dry-run
```

Apply:

```powershell
python -m scripts.sync_data_to_gcs `
  --local-root data\dwd\universe_membership_monthly\universe_name=us_liquid_100\year=$MembershipYear\month=$MembershipMonthNum `
  --local-root data\dwd\universe_membership_monthly\universe_name=us_liquid_500\year=$MembershipYear\month=$MembershipMonthNum
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

## E3. Publish liquidity and membership to BigQuery

BigQuery tables:

```text
<BIGQUERY_DWH_DATASET>.dws_equity_liquidity_monthly
<BIGQUERY_DWH_DATASET>.dim_universe_membership_monthly
```

First-time full seed, only if tables are missing or intentionally rebuilding:

```powershell
python -m scripts.update_universe_outputs_bigquery `
  --dataset liquidity_monthly `
  --mode full-replace `
  --start-month 2019-01 `
  --end-month $MetricMonth

python -m scripts.update_universe_outputs_bigquery `
  --dataset universe_membership `
  --mode full-replace `
  --start-month 2019-02 `
  --end-month $MembershipMonth
```

Routine monthly replace:

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

## E4. Validate BigQuery liquidity/universe outputs

```powershell
$env:METRIC_MONTH_DATE = "$MetricMonth-01"
$env:MEMBERSHIP_MONTH_DATE = "$MembershipMonth-01"

@'
from pathlib import Path
import os

from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv(dotenv_path=Path(".env").resolve())

project_id = os.environ["GCP_PROJECT_ID"]
dataset_id = os.environ["BIGQUERY_DWH_DATASET"]
location = os.getenv("GCP_LOCATION", "US")

metric_month = os.environ["METRIC_MONTH_DATE"]
membership_month = os.environ["MEMBERSHIP_MONTH_DATE"]

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

print("\nUniverse BigQuery validation complete.")
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

# Part R — Research derived-layer refresh

Run this after:

```text
1. stock DWD daily price catch-up completes
2. market-context daily price catch-up completes
3. monthly liquidity metrics and next-month universe membership are refreshed, when a new full month is available
```

Research outputs:

```text
data/dws/equity_features_daily
data/dws/market_context_features_daily
data/dws/equity_forward_returns_daily
data/ads/equity_research_panel_daily
```

## R0. Research config assumptions

`configs/research_panel.yml` is the source of truth for:

```text
factor_set
label_set
rolling_windows
technical windows
label horizons
feature / label / panel output roots
```

For daily stock data, use trading-session horizons. Current recommended `core_v1` values:

```yaml
rolling_windows:
  returns: [1, 2, 5, 10, 21, 63, 126, 252]
  return_lag_multiples: [1, 2, 3]

  momentum: [21, 63, 126]
  reversal: [1, 2, 5]

  volatility: [21, 63, 126]
  dollar_volume: [5, 20, 60]
  price_position: [63, 252]

  sma: [20, 50, 200]

  skip_recent_momentum:
    - [252, 21]

  annualization_days: 252

labels:
  horizons: [1, 2, 5, 10, 21, 63]

technical:
  backend: "talib"
  compute_candlestick_patterns: true
  market_context:
    include_candlestick_patterns: false
  rsi:
    windows: [14, 21]
  mfi:
    windows: [14, 21]
  atr:
    windows: [14, 21]
  macd:
    fast_period: 12
    slow_period: 26
    signal_period: 9
  bollinger:
    window: 20
    num_std_up: 2.0
    num_std_down: 2.0
  tema:
    windows: [20, 50]
  adx:
    windows: [14, 21]
  cmo:
    windows: [14, 21]
  ultimate_oscillator:
    timeperiod1: 7
    timeperiod2: 14
    timeperiod3: 28
  bop:
    enabled: true
  candlestick_patterns:
    mode: "all_talib_patterns"
```

Avoid crypto/hourly windows such as `4, 12, 24, 48, 72` unless intentionally creating a new factor set.

## R1. Set research operation IDs

```powershell
$RefreshStamp = (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssZ')
$ResearchOperationId = "research_refresh_$RefreshStamp"

$StartMonth = "2019-01"
$EndMonth = "YYYY-MM"
$PanelStartMonth = "2019-02"
$PanelEndMonth = "YYYY-MM"

$EquityFeatureRunId = "equity_features_$ResearchOperationId"
$MarketContextFeatureRunId = "market_context_features_$ResearchOperationId"
$LabelRunId = "equity_labels_$ResearchOperationId"
$PanelRunId = "equity_research_panel_$ResearchOperationId"
```

For month-end full refresh after August 2026 data is complete:

```powershell
$EndMonth = "2026-08"
$PanelEndMonth = "2026-08"
```

If stock prices, market-context prices, and membership all contain September rows, you can set:

```powershell
$EndMonth = "2026-09"
$PanelEndMonth = "2026-09"
```

## R2. Build equity features

Dry-run:

```powershell
python -m scripts.build_equity_features_daily `
  --run-id $EquityFeatureRunId `
  --operation-id $ResearchOperationId `
  --source-price-run-id $PriceRunId `
  --start-month $StartMonth `
  --end-month $EndMonth `
  --dry-run
```

Full overwrite:

```powershell
python -m scripts.build_equity_features_daily `
  --run-id $EquityFeatureRunId `
  --operation-id $ResearchOperationId `
  --source-price-run-id $PriceRunId `
  --start-month $StartMonth `
  --end-month $EndMonth `
  --overwrite
```

## R3. Build market-context features

Dry-run:

```powershell
python -m scripts.build_market_context_features_daily `
  --run-id $MarketContextFeatureRunId `
  --operation-id $ResearchOperationId `
  --source-market-context-price-run-id $MarketContextPriceRunId `
  --start-month $StartMonth `
  --end-month $EndMonth `
  --dry-run
```

Full overwrite:

```powershell
python -m scripts.build_market_context_features_daily `
  --run-id $MarketContextFeatureRunId `
  --operation-id $ResearchOperationId `
  --source-market-context-price-run-id $MarketContextPriceRunId `
  --start-month $StartMonth `
  --end-month $EndMonth `
  --overwrite
```

## R4. Build forward-return labels

Labels are computed by per-security trading-row shift. The configured horizon `21` means 21 future trading rows, not 21 calendar days.

Dry-run:

```powershell
python -m scripts.build_equity_forward_returns_daily `
  --run-id $LabelRunId `
  --operation-id $ResearchOperationId `
  --source-price-run-id $PriceRunId `
  --start-month $StartMonth `
  --end-month $EndMonth `
  --dry-run
```

Full overwrite:

```powershell
python -m scripts.build_equity_forward_returns_daily `
  --run-id $LabelRunId `
  --operation-id $ResearchOperationId `
  --source-price-run-id $PriceRunId `
  --start-month $StartMonth `
  --end-month $EndMonth `
  --overwrite
```

## R5. Build point-in-time research panel

The panel applies the final point-in-time membership filter. Features and labels are generated for `us_liquid_500` ever-members, but the panel keeps only rows whose dates fall inside the relevant monthly membership effective window.

Dry-run:

```powershell
python -m scripts.build_equity_research_panel_daily `
  --run-id $PanelRunId `
  --operation-id $ResearchOperationId `
  --source-feature-run-id $EquityFeatureRunId `
  --source-label-run-id $LabelRunId `
  --source-market-context-feature-run-id $MarketContextFeatureRunId `
  --start-month $PanelStartMonth `
  --end-month $PanelEndMonth `
  --dry-run
```

Full overwrite:

```powershell
python -m scripts.build_equity_research_panel_daily `
  --run-id $PanelRunId `
  --operation-id $ResearchOperationId `
  --source-feature-run-id $EquityFeatureRunId `
  --source-label-run-id $LabelRunId `
  --source-market-context-feature-run-id $MarketContextFeatureRunId `
  --start-month $PanelStartMonth `
  --end-month $PanelEndMonth `
  --overwrite
```

## R6. Validate regenerated research outputs

This validation reads `configs/research_panel.yml` dynamically and should not require edits when feature windows change.

```powershell
@'
from pathlib import Path

import pandas as pd

from quant_platform.research.config import (
    load_market_context_config,
    load_research_panel_config,
)

research_config = load_research_panel_config("configs/research_panel.yml")
market_config = load_market_context_config("configs/market_context.yml")

universe_name = str(
    research_config.feature_scope.get(
        "universe_name",
        research_config.default_universe_name,
    )
)

paths = {
    "equity_features": (
        Path(research_config.feature_output_root)
        / f"factor_set={research_config.factor_set}"
    ),
    "market_context_features": (
        Path(research_config.market_context_feature_root)
        / f"context_set={market_config.context_set}"
    ),
    "labels": (
        Path(research_config.label_output_root)
        / f"label_set={research_config.label_set}"
    ),
    "panel": (
        Path(research_config.panel_output_root)
        / f"universe_name={universe_name}"
        / f"factor_set={research_config.factor_set}"
    ),
}

frames = {}

for name, root in paths.items():
    print("\n" + "=" * 100)
    print(name)
    print("root:", root)

    files = sorted(root.rglob("*.parquet"))
    print("files:", len(files))

    if not files:
        raise SystemExit(f"No files found for {name}: {root}")

    df = pd.concat([pd.read_parquet(path) for path in files], ignore_index=True)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    print("rows:", len(df))
    print("min date:", df["date"].min())
    print("max date:", df["date"].max())

    if "ticker" in df.columns:
        print("tickers:", df["ticker"].nunique())

    if "security_id" in df.columns:
        print("security_ids:", df["security_id"].nunique())

    frames[name] = df

equity = frames["equity_features"]
mkt = frames["market_context_features"]
labels = frames["labels"]
panel = frames["panel"]

feature_required = []

for horizon in research_config.rolling_windows["returns"]:
    feature_required.append(f"ret_{horizon}d")

for horizon in research_config.rolling_windows["momentum"]:
    feature_required.append(f"mom_{horizon}d")

for long_window, skip_window in research_config.rolling_windows["skip_recent_momentum"]:
    feature_required.append(f"mom_{long_window}_{skip_window}d")

for horizon in research_config.rolling_windows["reversal"]:
    feature_required.append(f"rev_{horizon}d")

for horizon in research_config.rolling_windows["volatility"]:
    feature_required.append(f"realized_vol_{horizon}d")

for horizon in research_config.rolling_windows["dollar_volume"]:
    feature_required.append(f"avg_dollar_volume_{horizon}d")

for horizon in research_config.rolling_windows["price_position"]:
    feature_required.append(f"price_position_{horizon}d")

for horizon in research_config.rolling_windows["sma"]:
    feature_required.append(f"sma_{horizon}_ratio")

technical_raw = research_config.technical.raw

for horizon in technical_raw["rsi"]["windows"]:
    feature_required.append(f"tech_rsi_{horizon}")

for horizon in technical_raw["mfi"]["windows"]:
    feature_required.append(f"tech_mfi_{horizon}")

for horizon in technical_raw["atr"]["windows"]:
    feature_required.append(f"tech_atr_{horizon}_norm")

for horizon in technical_raw["adx"]["windows"]:
    feature_required.extend(
        [
            f"tech_adx_{horizon}",
            f"tech_plus_di_{horizon}",
            f"tech_minus_di_{horizon}",
            f"tech_di_spread_{horizon}",
        ]
    )

for horizon in technical_raw["cmo"]["windows"]:
    feature_required.append(f"tech_cmo_{horizon}")

macd = technical_raw["macd"]
macd_suffix = f"{macd['fast_period']}_{macd['slow_period']}_{macd['signal_period']}"

bollinger = technical_raw["bollinger"]
bb_window = bollinger["window"]

for horizon in technical_raw["tema"]["windows"]:
    feature_required.append(f"tech_tema_{horizon}_ratio")

ult = technical_raw["ultimate_oscillator"]
ult_suffix = f"{ult['timeperiod1']}_{ult['timeperiod2']}_{ult['timeperiod3']}"

feature_required.extend(
    [
        f"tech_macd_hist_{macd_suffix}",
        f"tech_macd_hist_{macd_suffix}_norm",
        f"tech_bb_position_{bb_window}",
        f"tech_ultosc_{ult_suffix}",
        "tech_bop",
    ]
)

label_required = []

for horizon in research_config.label_horizons:
    label_required.extend(
        [
            f"label_fwd_ret_{horizon}d",
            f"has_label_fwd_ret_{horizon}d",
            f"label_fwd_date_{horizon}d",
        ]
    )

market_feature_required = [
    column for column in feature_required if not column.startswith("cdl_")
]

panel_required = [
    "date",
    "universe_name",
    "membership_month",
    "effective_start_date",
    "effective_end_date",
    "rank",
    "security_id",
    "ticker",
    "factor_set",
    "label_set",
    "mkt_spy_ret_1d",
    "mkt_spy_ret_21d",
    "excess_ret_1d_vs_spy",
    *feature_required,
    *[f"label_fwd_ret_{horizon}d" for horizon in research_config.label_horizons],
]

checks = {
    "equity_features": (equity, feature_required),
    "market_context_features": (mkt, market_feature_required),
    "labels": (labels, label_required),
    "panel": (panel, panel_required),
}

for name, (df, required_columns) in checks.items():
    missing = [column for column in required_columns if column not in df.columns]

    print("\n" + "=" * 100)
    print(name)
    print("missing columns:", len(missing))

    if missing:
        print(missing[:100])
        raise SystemExit(f"{name} missing required columns")

duplicate_checks = {
    "equity_features": (equity, ["date", "security_id", "factor_set"]),
    "market_context_features": (mkt, ["date", "context_set", "security_id"]),
    "labels": (labels, ["date", "security_id", "label_set"]),
    "panel": (panel, ["date", "universe_name", "security_id", "factor_set"]),
}

for name, (df, keys) in duplicate_checks.items():
    duplicate_count = int(df.duplicated(keys).sum())
    print(name, "duplicate keys:", duplicate_count, keys)

    if duplicate_count != 0:
        raise SystemExit(f"{name} duplicate keys found")

for column in ["effective_start_date", "effective_end_date"]:
    panel[column] = pd.to_datetime(panel[column], errors="coerce").dt.date

bad_effective = panel[
    (panel["date"] < panel["effective_start_date"])
    | (panel["date"] >= panel["effective_end_date"])
]

print("\nbad effective-window rows:", len(bad_effective))

if not bad_effective.empty:
    raise SystemExit("Panel contains rows outside membership effective windows")

cdl_cols = [c for c in equity.columns if c.startswith("cdl_")]
mkt_cdl_cols = [c for c in mkt.columns if c.startswith("cdl_")]

print("\nequity candlestick columns:", len(cdl_cols))
print("market context candlestick columns:", len(mkt_cdl_cols))

if len(cdl_cols) < 20:
    raise SystemExit("Expected many equity candlestick columns")

if mkt_cdl_cols:
    raise SystemExit("Market context features should not have candlestick columns")

print("\nDaily-stock research refresh validation passed.")
'@ | python -
```

---

# Part S — Publish research outputs to GCS / BigQuery

Use this after Part R validation passes.

Research outputs to publish:

```text
data/dwd/security_master/dim_market_context_symbol
data/dwd/market_context_price_daily
data/dws/market_context_features_daily
data/dws/equity_features_daily
data/dws/equity_forward_returns_daily
data/ads/equity_research_panel_daily
```

Do not publish `data/ods/` or `reports/` as research tables.

## S1. Sync research outputs to GCS

Dry-run:

```powershell
python -m scripts.sync_data_to_gcs `
  --local-root data\dwd\security_master\dim_market_context_symbol `
  --local-root data\dwd\market_context_price_daily `
  --local-root data\dws\market_context_features_daily `
  --local-root data\dws\equity_features_daily `
  --local-root data\dws\equity_forward_returns_daily `
  --local-root data\ads\equity_research_panel_daily `
  --dry-run
```

Apply:

```powershell
python -m scripts.sync_data_to_gcs `
  --local-root data\dwd\security_master\dim_market_context_symbol `
  --local-root data\dwd\market_context_price_daily `
  --local-root data\dws\market_context_features_daily `
  --local-root data\dws\equity_features_daily `
  --local-root data\dws\equity_forward_returns_daily `
  --local-root data\ads\equity_research_panel_daily
```

## S2. Validate GCS research outputs

```powershell
@'
from pathlib import Path
import os

from dotenv import load_dotenv
from google.cloud import storage

load_dotenv(dotenv_path=Path(".env").resolve())

bucket_name = os.environ["GCS_BUCKET"]
project_id = os.environ["GCP_PROJECT_ID"]

prefixes = [
    "dwd/security_master/dim_market_context_symbol/",
    "dwd/market_context_price_daily/",
    "dws/market_context_features_daily/",
    "dws/equity_features_daily/",
    "dws/equity_forward_returns_daily/",
    "ads/equity_research_panel_daily/",
]

client = storage.Client(project=project_id)
bucket = client.bucket(bucket_name)

for prefix in prefixes:
    blobs = list(bucket.list_blobs(prefix=prefix, max_results=20))

    print("\n" + "=" * 100)
    print("prefix:", f"gs://{bucket_name}/{prefix}")
    print("sample object count:", len(blobs))

    if not blobs:
        raise SystemExit(f"No GCS objects found under {prefix}")

    for blob in blobs[:10]:
        print(blob.name, blob.size)

print("\nGCS research output validation passed.")
'@ | python -
```

## S3. Publish research outputs to BigQuery

Confirm the publisher exists:

```powershell
Test-Path scripts\update_research_outputs_bigquery.py
```

Plan:

```powershell
python -m scripts.update_research_outputs_bigquery `
  --dataset all `
  --mode plan
```

Apply:

```powershell
python -m scripts.update_research_outputs_bigquery `
  --dataset all `
  --mode apply
```

The research publisher performs full table replacement for these research outputs. This is appropriate for first-time publish or after a feature schema change.

Target BigQuery tables:

```text
<BIGQUERY_DWH_DATASET>.dim_market_context_symbol
<BIGQUERY_DWH_DATASET>.dwd_market_context_price_daily
<BIGQUERY_DWH_DATASET>.dws_market_context_features_daily
<BIGQUERY_DWH_DATASET>.dws_equity_features_daily
<BIGQUERY_DWH_DATASET>.dws_equity_forward_returns_daily
<BIGQUERY_DWH_DATASET>.ads_equity_research_panel_daily
```

## S4. Validate BigQuery research outputs

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

tables = {
    "dim_market_context_symbol": {
        "keys": ["context_set", "security_id"],
        "date_field": None,
    },
    "dwd_market_context_price_daily": {
        "keys": ["date", "context_set", "security_id"],
        "date_field": "date",
    },
    "dws_market_context_features_daily": {
        "keys": ["date", "context_set", "security_id"],
        "date_field": "date",
    },
    "dws_equity_features_daily": {
        "keys": ["date", "security_id", "factor_set"],
        "date_field": "date",
    },
    "dws_equity_forward_returns_daily": {
        "keys": ["date", "security_id", "label_set"],
        "date_field": "date",
    },
    "ads_equity_research_panel_daily": {
        "keys": ["date", "universe_name", "security_id", "factor_set"],
        "date_field": "date",
    },
}

for table_name, spec in tables.items():
    full_table = f"`{project_id}.{dataset_id}.{table_name}`"

    print("\n" + "=" * 100)
    print(table_name)

    if spec["date_field"] is None:
        summary_sql = f"""
        SELECT
          COUNT(*) AS row_count,
          COUNT(DISTINCT ticker) AS ticker_count,
          COUNT(DISTINCT security_id) AS security_id_count
        FROM {full_table}
        """
    else:
        date_field = spec["date_field"]
        summary_sql = f"""
        SELECT
          COUNT(*) AS row_count,
          COUNT(DISTINCT ticker) AS ticker_count,
          COUNT(DISTINCT security_id) AS security_id_count,
          MIN({date_field}) AS min_date,
          MAX({date_field}) AS max_date
        FROM {full_table}
        """

    summary = client.query(summary_sql, location=location).to_dataframe()
    print(summary.to_string(index=False))

    key_columns = ", ".join(spec["keys"])

    duplicate_sql = f"""
    SELECT COUNT(*) AS duplicate_key_count
    FROM (
      SELECT {key_columns}, COUNT(*) AS n
      FROM {full_table}
      GROUP BY {key_columns}
      HAVING n > 1
    )
    """

    duplicates = client.query(duplicate_sql, location=location).to_dataframe()
    duplicate_count = int(duplicates.iloc[0]["duplicate_key_count"])

    print("duplicate_key_count:", duplicate_count)

    if duplicate_count != 0:
        raise SystemExit(f"Duplicate keys found in {table_name}")

print("\nBigQuery research output validation passed.")
'@ | python -
```

---

# Part N — Notebook inspection

Use the inspection notebook for local/BQ/GCS exploration after research outputs are generated and optionally published.

Recommended notebook path:

```text
notebook/quant_research_panel_inspection.ipynb
```

Recommended run order:

```text
1. Setup
2. Load configs
3. Local dataset inventory
4. Panel summary
5. Column-family summary
6. Missingness
7. Label coverage
8. Simple IC by horizon
9. Factor decile analysis
10. Market context inspection
11. Optional BigQuery checks
12. Optional GCS checks
```

If notebook output becomes large, clear it before committing:

```powershell
jupyter nbconvert `
  --ClearOutputPreprocessor.enabled=True `
  --inplace notebook\quant_research_panel_inspection.ipynb
```

If `jupyter` CLI is unavailable, clear outputs in VS Code before committing.

---

# Part F — Complete month-end data-operation checklist

For a normal month-end run after the newest full month has stock DWD data:

```powershell
$RunStamp = (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssZ')
$OperationId = "daily_data_$RunStamp"
$PriceRunId = "price_update_$RunStamp"
$RunId = $PriceRunId
$MarketContextPriceRunId = "market_context_price_$OperationId"

$SnapshotDate = "YYYY-MM-DD"
$MetricMonth = "YYYY-MM"
$MembershipMonth = "YYYY-MM"
```

Run:

```text
1. Part A — reference-data refresh, usually monthly
2. Part B — stock price catch-up
3. Part C — market-context price catch-up
4. Part D — liquidity metrics and next-month membership
5. Part E — publish liquidity/membership to GCS/BQ
6. Part R — refresh research derived layer
7. Part S — publish research outputs to GCS/BQ
8. Part N — inspect notebook
```

Final checks:

```powershell
python -m pytest -q
python -m ruff check .
git status --short
```

If this was only a data operation, no source-code commit is needed. If you changed configs, scripts, tests, docs, or notebooks, commit only those source artifacts.

---

# Part G — Today-specific instructions for 2026-09-01 / 2026-09-02

Use this section for the September 2026 run after August 2026 data is complete.

## G1. Use the correct branch and clean status

If the Week 10 branch is still being finalized:

```powershell
git checkout week10-research-features-panel
git pull --ff-only
```

If it has already merged:

```powershell
git checkout main
git pull --ff-only origin main
```

Then:

```powershell
python -m pytest -q
python -m ruff check .
docker compose up -d postgres
docker compose exec postgres pg_isready -U quant -d quant_metadata
python -m scripts.run_migrations
```

## G2. Set September run variables

For 2026-09-01:

```powershell
$SnapshotDate = "2026-09-01"
```

For 2026-09-02:

```powershell
$SnapshotDate = "2026-09-02"
```

Then:

```powershell
$RunStamp = (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssZ')
$OperationId = "daily_data_$RunStamp"
$PriceRunId = "price_update_$RunStamp"
$RunId = $PriceRunId
$MarketContextPriceRunId = "market_context_price_$OperationId"

$MetricMonth = "2026-08"
$MembershipMonth = "2026-09"

$MetricYear = $MetricMonth.Substring(0, 4)
$MetricMonthNum = $MetricMonth.Substring(5, 2)
$MembershipYear = $MembershipMonth.Substring(0, 4)
$MembershipMonthNum = $MembershipMonth.Substring(5, 2)
```

## G3. Run reference and price operations

Run:

```text
Part A
Part B
Part C
```

If stock price tasks are empty, skip B2-B11. Continue to Part C and then verify whether August is complete.

## G4. Build August liquidity and September membership

Run Part D using:

```powershell
$MetricMonth = "2026-08"
$MembershipMonth = "2026-09"
```

If August liquidity or September membership was already generated from incomplete August data, use the `--replace-existing-partitions` commands in Part D.

Then run Part E to publish the selected monthly outputs.

## G5. Refresh research outputs

Set:

```powershell
$RefreshStamp = (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssZ')
$ResearchOperationId = "research_refresh_$RefreshStamp"

$StartMonth = "2019-01"
$EndMonth = "2026-08"
$PanelStartMonth = "2019-02"
$PanelEndMonth = "2026-08"

$EquityFeatureRunId = "equity_features_$ResearchOperationId"
$MarketContextFeatureRunId = "market_context_features_$ResearchOperationId"
$LabelRunId = "equity_labels_$ResearchOperationId"
$PanelRunId = "equity_research_panel_$ResearchOperationId"
```

If stock prices, market-context prices, and membership all contain September rows, set:

```powershell
$EndMonth = "2026-09"
$PanelEndMonth = "2026-09"
```

Run Part R and Part S.

## G6. Source-code commit policy

If you changed `configs/research_panel.yml`, tests, runbook, BQ publish script, or notebook, commit only those files:

```powershell
git status --short
git diff --stat
```

Example:

```powershell
git add `
  configs\research_panel.yml `
  tests\test_research_config.py `
  tests\test_research_features.py `
  tests\test_research_technical.py `
  tests\test_research_panel.py `
  docs\data_operations_runbook.md `
  scripts\update_research_outputs_bigquery.py `
  notebook\quant_research_panel_inspection.ipynb
```

Do not add:

```text
data/
reports/
.env
.venv/
```

Commit and push:

```powershell
git commit -m "Update data operations runbook for research refresh"
git push
```

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

## Vendor freshness probe fails

If the probe says returned max date is earlier than request end date, Tiingo has not published the requested date yet. Regenerate price-gap tasks later instead of continuing with a stale end date.

## Market-context dry-run prints `price_start_date: 2019-01-01`

That is only the configured historical seed start. For incremental runs, the actual request windows are shown under `Planned tasks`, for example:

```text
SPY 2026-08-22 -> 2026-08-31
```

If `full_refresh: False`, the script should use existing DWD max date + 1.

## Stale symbols still appear in stock task list

Regenerate supported tickers, `dim_security`, and candidate pool snapshots first. Then regenerate price-gap tasks.

Inspect:

```text
data/dwd/security_master/dim_security.parquet
metadata.symbol_ingestion_status
metadata.price_update_window_results
```

The daily eligibility cutoff should use latest complete EOD minus `active_end_date_grace_days`, not the old bootstrap anchor date.

## `--missing-only` is slow for liquidity metrics

The script should early-exit if the selected partition already exists. If it reads all historical DWD despite `--missing-only`, check that the incremental month-pruning updates are present in:

```text
quant_platform/universe/liquidity.py
scripts/build_equity_liquidity_monthly.py
```

## BigQuery universe tables missing

Use `--mode full-replace` once to seed them. After that, use `--mode replace-months`.

## Research BigQuery tables missing

Create and run:

```text
scripts.update_research_outputs_bigquery
```

Then publish with:

```powershell
python -m scripts.update_research_outputs_bigquery `
  --dataset all `
  --mode apply
```

## GCS objects missing for BigQuery load

Run `scripts.sync_data_to_gcs` for the selected local partitions before running BigQuery publish.

## Full `ruff check .` suddenly reports many unrelated files

Do not run broad `ruff --fix` unless intentionally doing repo-wide lint cleanup. Prefer targeted checks around the files you changed. If accidental broad Ruff changes appear, inspect `git diff --stat` carefully and revert unrelated modifications before committing.

## Do not commit generated data

Generated local artifacts under `data/` and `reports/` are operational outputs. They should not be committed to Git.
