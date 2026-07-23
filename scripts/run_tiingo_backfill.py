from __future__ import annotations

import argparse
import json
import os
import time
from datetime import date
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

import pandas as pd
import psycopg
import yaml
from dotenv import load_dotenv
from google.cloud import storage

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"
CONFIG_PATH = PROJECT_ROOT / "configs" / "backfill.yml"


def load_config(config_path: Path = CONFIG_PATH) -> dict[str, Any]:
    """Load backfill configuration."""
    if not config_path.exists():
        raise FileNotFoundError(f"Backfill config not found: {config_path}")

    raw_text = config_path.read_text(encoding="utf-8")
    expanded_text = os.path.expandvars(raw_text)
    config = yaml.safe_load(expanded_text)

    if not isinstance(config, dict):
        raise ValueError("backfill.yml must contain a YAML mapping.")

    return config


def resolve_project_path(path_str: str) -> Path:
    """Resolve a project-relative path."""
    path = Path(path_str)

    if path.is_absolute():
        return path

    return PROJECT_ROOT / path


def to_date(value: Any) -> date:
    """Convert a value to Python date."""
    return pd.Timestamp(value).date()


def parse_tickers(raw_tickers: str | None) -> list[str] | None:
    """Parse comma-separated ticker input."""
    if not raw_tickers:
        return None

    tickers = [
        item.strip().upper()
        for item in raw_tickers.split(",")
        if item.strip()
    ]

    return tickers or None


def get_task_list_path(config: dict[str, Any], task_list_name: str) -> Path:
    """Return task list parquet path from config."""
    task_lists = config.get("task_lists", {})

    if task_list_name not in task_lists:
        raise KeyError(f"Task list not found in config: {task_list_name}")

    return resolve_project_path(task_lists[task_list_name]["local_path"])


def load_task_list(
    config: dict[str, Any],
    task_list_name: str,
    target_tickers: list[str] | None,
) -> pd.DataFrame:
    """Load and validate the task list."""
    task_list_path = get_task_list_path(config, task_list_name)

    if not task_list_path.exists():
        raise FileNotFoundError(f"Task list not found: {task_list_path}")

    df = pd.read_parquet(task_list_path)

    required_columns = [
        "task_id",
        "task_list_name",
        "source",
        "dataset_name",
        "security_id",
        "ticker",
        "requested_start_date",
        "requested_end_date",
        "status",
    ]

    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Task list missing columns: {missing}")

    null_columns = [col for col in required_columns if df[col].isna().any()]
    if null_columns:
        raise ValueError(f"Task list has nulls in columns: {null_columns}")

    df = df.copy()
    df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
    df["security_id"] = df["security_id"].astype(str).str.strip()
    df["source"] = df["source"].astype(str).str.strip()
    df["dataset_name"] = df["dataset_name"].astype(str).str.strip()
    df["requested_start_date"] = df["requested_start_date"].map(to_date)
    df["requested_end_date"] = df["requested_end_date"].map(to_date)

    if target_tickers is not None:
        df = df[df["ticker"].isin(target_tickers)].copy()

        if df.empty:
            raise ValueError(
                "None of the requested tickers were found in task list. "
                f"Requested: {target_tickers}"
            )

    df = df.reset_index(drop=True)
    df["_task_order"] = range(len(df))

    return df


def validate_single_request_window(df: pd.DataFrame) -> tuple[str, str, date, date]:
    """Validate one source/dataset/request window for this run."""
    sources = sorted(df["source"].unique())
    datasets = sorted(df["dataset_name"].unique())
    start_dates = sorted(df["requested_start_date"].unique())
    end_dates = sorted(df["requested_end_date"].unique())

    if len(sources) != 1:
        raise ValueError(f"Expected one source, got {sources}")

    if len(datasets) != 1:
        raise ValueError(f"Expected one dataset_name, got {datasets}")

    if len(start_dates) != 1:
        raise ValueError(f"Expected one requested_start_date, got {start_dates}")

    if len(end_dates) != 1:
        raise ValueError(f"Expected one requested_end_date, got {end_dates}")

    return sources[0], datasets[0], start_dates[0], end_dates[0]


def make_batch_id(
    task_list_name: str,
    source: str,
    dataset_name: str,
    start_date: date,
    end_date: date,
) -> str:
    """Create deterministic batch ID matching init_backfill_metadata.py."""
    return (
        f"{task_list_name}:{source}:{dataset_name}:"
        f"{start_date.isoformat()}:{end_date.isoformat()}"
    )


def mark_batch_running(conn: psycopg.Connection, batch_id: str) -> None:
    """Mark a backfill batch as running."""
    sql = """
        UPDATE metadata.backfill_batches
        SET status = 'running',
            started_at = COALESCE(started_at, now()),
            updated_at = now()
        WHERE batch_id = %s;
    """

    with conn.cursor() as cur:
        cur.execute(sql, (batch_id,))


def reset_stale_running_rows(
    conn: psycopg.Connection,
    source: str,
    dataset_name: str,
    requested_start_date: date,
    requested_end_date: date,
    stale_minutes: int,
) -> None:
    """Reset stale running rows to failed so they can be retried."""
    sql = """
        UPDATE metadata.symbol_ingestion_status
        SET status = 'failed',
            last_error_message = 'Reset stale running row before resume.',
            updated_at = now()
        WHERE source = %s
          AND dataset_name = %s
          AND requested_start_date = %s
          AND requested_end_date = %s
          AND status = 'running'
          AND updated_at < now() - (%s || ' minutes')::interval;
    """

    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                source,
                dataset_name,
                requested_start_date,
                requested_end_date,
                stale_minutes,
            ),
        )


def fetch_metadata_statuses(
    conn: psycopg.Connection,
    task_df: pd.DataFrame,
    source: str,
    dataset_name: str,
    requested_start_date: date,
    requested_end_date: date,
) -> pd.DataFrame:
    """Fetch metadata status rows for tickers in the task list."""
    tickers = sorted(task_df["ticker"].unique().tolist())

    sql = """
        SELECT
            source,
            dataset_name,
            ticker,
            security_id,
            requested_start_date,
            requested_end_date,
            last_successful_date,
            status,
            attempt_count,
            last_error_message,
            updated_at
        FROM metadata.symbol_ingestion_status
        WHERE source = %s
          AND dataset_name = %s
          AND requested_start_date = %s
          AND requested_end_date = %s
          AND ticker = ANY(%s);
    """

    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                source,
                dataset_name,
                requested_start_date,
                requested_end_date,
                tickers,
            ),
        )
        rows = cur.fetchall()

    columns = [
        "source",
        "dataset_name",
        "ticker",
        "security_id",
        "requested_start_date",
        "requested_end_date",
        "last_successful_date",
        "status",
        "attempt_count",
        "last_error_message",
        "updated_at",
    ]

    return pd.DataFrame(rows, columns=columns)

def select_tasks_to_process(
    task_df: pd.DataFrame,
    status_df: pd.DataFrame,
    max_attempts: int,
    limit: int | None,
) -> pd.DataFrame:
    """Select pending/failed tasks that are eligible for processing.

    task_df contains the static task-list fields.
    status_df contains the live metadata status from Postgres.

    Both dataframes have a 'status' column, so we rename metadata fields before
    merging to avoid pandas creating status_x/status_y.
    """
    if status_df.empty:
        raise ValueError(
            "No metadata rows found for task list. "
            "Run scripts.init_backfill_metadata first."
        )

    status_df = status_df.copy()
    status_df["ticker"] = status_df["ticker"].astype(str).str.upper()

    metadata_status = status_df[
        [
            "ticker",
            "status",
            "attempt_count",
            "last_error_message",
            "last_successful_date",
        ]
    ].rename(
        columns={
            "status": "metadata_status",
            "attempt_count": "metadata_attempt_count",
            "last_error_message": "metadata_last_error_message",
            "last_successful_date": "metadata_last_successful_date",
        }
    )

    merged = task_df.merge(
        metadata_status,
        on="ticker",
        how="left",
    )

    missing_status = merged["metadata_status"].isna()

    if missing_status.any():
        missing = merged.loc[missing_status, "ticker"].tolist()
        raise ValueError(
            "Some task-list tickers are missing metadata rows. "
            f"Examples: {missing[:20]}"
        )

    eligible = merged[
        merged["metadata_status"].isin(["pending", "failed"])
        & (merged["metadata_attempt_count"] < max_attempts)
    ].copy()

    eligible = eligible.sort_values("_task_order").reset_index(drop=True)

    if limit is not None:
        eligible = eligible.head(limit).copy()

    # Preserve the column names expected later in run_backfill().
    eligible["status"] = eligible["metadata_status"]
    eligible["attempt_count"] = eligible["metadata_attempt_count"]
    eligible["last_error_message"] = eligible["metadata_last_error_message"]
    eligible["last_successful_date"] = eligible["metadata_last_successful_date"]

    return eligible

def tiingo_prices_url(ticker: str, start_date: date, end_date: date) -> str:
    """Build Tiingo historical EOD prices URL."""
    encoded_ticker = quote(ticker, safe="")
    query = urlencode(
        {
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
            "format": "json",
        }
    )
    return f"https://api.tiingo.com/tiingo/daily/{encoded_ticker}/prices?{query}"


def fetch_tiingo_prices(
    ticker: str,
    start_date: date,
    end_date: date,
    api_token: str,
    timeout_seconds: int,
    max_retries: int,
    retry_sleep_seconds: int,
) -> list[dict[str, Any]]:
    """Fetch Tiingo historical EOD prices for one ticker."""
    url = tiingo_prices_url(ticker, start_date, end_date)

    last_error = ""

    for attempt in range(1, max_retries + 1):
        try:
            request = Request(
                url,
                headers={
                    "Authorization": f"Token {api_token}",
                    "User-Agent": "us-equity-quant-data-platform/1.0",
                    "Accept": "application/json",
                },
            )

            with urlopen(request, timeout=timeout_seconds) as response:
                body = response.read().decode("utf-8")

            data = json.loads(body)

            if not isinstance(data, list):
                raise RuntimeError(
                    f"Expected Tiingo response list, got {type(data)}"
                )

            return data

        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")[:500]
            last_error = f"HTTP {exc.code}: {body}"

        except (URLError, TimeoutError, json.JSONDecodeError, RuntimeError) as exc:
            last_error = repr(exc)

        if attempt < max_retries:
            print(
                f"Retrying {ticker} after error "
                f"({attempt}/{max_retries}): {last_error}"
            )
            time.sleep(retry_sleep_seconds)

    raise RuntimeError(f"Tiingo request failed for {ticker}: {last_error}")


def get_raw_output_path(ods_root: Path, ticker: str) -> Path:
    """Return local raw ODS file path for one ticker."""
    return ods_root / f"symbol={ticker}" / f"{ticker.lower()}_prices.json"


def write_raw_prices(local_path: Path, rows: list[dict[str, Any]]) -> None:
    """Write raw Tiingo price rows as JSON."""
    local_path.parent.mkdir(parents=True, exist_ok=True)

    with local_path.open("w", encoding="utf-8") as file:
        json.dump(rows, file, indent=2)


def upload_raw_file_to_gcs(
    local_path: Path,
    ticker: str,
    bucket_name: str,
    gcs_ods_prefix: str,
    dry_run: bool,
) -> None:
    """Upload one raw ODS file to GCS."""
    destination_blob_name = (
        f"{gcs_ods_prefix}/symbol={ticker}/{ticker.lower()}_prices.json"
    )

    if dry_run:
        print(
            "[DRY RUN] Would upload "
            f"{local_path} to gs://{bucket_name}/{destination_blob_name}"
        )
        return

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(str(local_path))

    print(f"Uploaded to gs://{bucket_name}/{destination_blob_name}")


def extract_last_successful_date(rows: list[dict[str, Any]]) -> date | None:
    """Extract max price date from Tiingo rows."""
    if not rows:
        return None

    dates = pd.to_datetime(
        [row.get("date") for row in rows],
        errors="coerce",
        utc=True,
    ).dropna()

    if len(dates) == 0:
        return None

    return dates.max().date()


def mark_symbol_running(
    conn: psycopg.Connection,
    row: pd.Series,
) -> None:
    """Mark one symbol as running and increment attempt_count."""
    sql = """
        UPDATE metadata.symbol_ingestion_status
        SET status = 'running',
            attempt_count = attempt_count + 1,
            last_error_message = NULL,
            updated_at = now()
        WHERE source = %s
          AND dataset_name = %s
          AND ticker = %s
          AND requested_start_date = %s
          AND requested_end_date = %s;
    """

    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                row["source"],
                row["dataset_name"],
                row["ticker"],
                row["requested_start_date"],
                row["requested_end_date"],
            ),
        )


def mark_symbol_success(
    conn: psycopg.Connection,
    row: pd.Series,
    last_successful_date: date,
) -> None:
    """Mark one symbol as success."""
    sql = """
        UPDATE metadata.symbol_ingestion_status
        SET status = 'success',
            last_successful_date = %s,
            last_error_message = NULL,
            updated_at = now()
        WHERE source = %s
          AND dataset_name = %s
          AND ticker = %s
          AND requested_start_date = %s
          AND requested_end_date = %s;
    """

    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                last_successful_date,
                row["source"],
                row["dataset_name"],
                row["ticker"],
                row["requested_start_date"],
                row["requested_end_date"],
            ),
        )


def mark_symbol_skipped(
    conn: psycopg.Connection,
    row: pd.Series,
    message: str,
) -> None:
    """Mark one symbol as skipped."""
    sql = """
        UPDATE metadata.symbol_ingestion_status
        SET status = 'skipped',
            last_error_message = %s,
            updated_at = now()
        WHERE source = %s
          AND dataset_name = %s
          AND ticker = %s
          AND requested_start_date = %s
          AND requested_end_date = %s;
    """

    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                message,
                row["source"],
                row["dataset_name"],
                row["ticker"],
                row["requested_start_date"],
                row["requested_end_date"],
            ),
        )


def mark_symbol_failed(
    conn: psycopg.Connection,
    row: pd.Series,
    error_message: str,
) -> None:
    """Mark one symbol as failed."""
    sql = """
        UPDATE metadata.symbol_ingestion_status
        SET status = 'failed',
            last_error_message = %s,
            updated_at = now()
        WHERE source = %s
          AND dataset_name = %s
          AND ticker = %s
          AND requested_start_date = %s
          AND requested_end_date = %s;
    """

    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                error_message[:2000],
                row["source"],
                row["dataset_name"],
                row["ticker"],
                row["requested_start_date"],
                row["requested_end_date"],
            ),
        )


def fetch_status_counts_for_task(
    conn: psycopg.Connection,
    task_df: pd.DataFrame,
    source: str,
    dataset_name: str,
    requested_start_date: date,
    requested_end_date: date,
) -> pd.DataFrame:
    """Fetch status counts for tickers in the current task list."""
    tickers = sorted(task_df["ticker"].unique().tolist())

    sql = """
        SELECT status, COUNT(*) AS n
        FROM metadata.symbol_ingestion_status
        WHERE source = %s
          AND dataset_name = %s
          AND requested_start_date = %s
          AND requested_end_date = %s
          AND ticker = ANY(%s)
        GROUP BY status
        ORDER BY status;
    """

    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                source,
                dataset_name,
                requested_start_date,
                requested_end_date,
                tickers,
            ),
        )
        rows = cur.fetchall()

    return pd.DataFrame(rows, columns=["status", "n"])


def maybe_mark_batch_success(
    conn: psycopg.Connection,
    batch_id: str,
    status_counts: pd.DataFrame,
) -> None:
    """Mark batch success only if no pending/running/failed rows remain."""
    incomplete_statuses = {"pending", "running", "failed"}

    counts = {
        row.status: int(row.n)
        for row in status_counts.itertuples(index=False)
    }

    incomplete_count = sum(
        counts.get(status, 0)
        for status in incomplete_statuses
    )

    if incomplete_count > 0:
        return

    sql = """
        UPDATE metadata.backfill_batches
        SET status = 'success',
            ended_at = now(),
            updated_at = now()
        WHERE batch_id = %s;
    """

    with conn.cursor() as cur:
        cur.execute(sql, (batch_id,))


def run_backfill(args: argparse.Namespace) -> None:
    """Run Tiingo backfill for selected task-list rows."""
    load_dotenv(ENV_PATH)

    api_token = os.getenv("TIINGO_API_TOKEN")
    dsn = os.getenv("POSTGRES_DSN")
    bucket_name = os.getenv("GCS_BUCKET", "")

    if not api_token:
        raise RuntimeError("TIINGO_API_TOKEN is missing. Check your .env file.")

    if not dsn:
        raise RuntimeError("POSTGRES_DSN is missing. Check your .env file.")

    config = load_config()

    timeout_seconds = int(config["tiingo"]["request_timeout_seconds"])
    max_retries = int(config["tiingo"]["max_retries"])
    retry_sleep_seconds = int(config["tiingo"]["retry_sleep_seconds"])
    sleep_between = float(config["tiingo"]["sleep_seconds_between_requests"])
    max_attempts = int(config["execution"]["max_attempts_per_symbol"])
    stale_minutes = int(config["execution"]["reset_stale_running_after_minutes"])

    ods_root = resolve_project_path(config["local_paths"]["ods_root"])
    gcs_ods_prefix = config["gcs"]["ods_prefix"]

    # target_tickers = parse_tickers(args.tickers)

    # task_df = load_task_list(
    #     config=config,
    #     task_list_name=args.task_list,
    #     target_tickers=target_tickers,
    # )

    # source, dataset_name, requested_start_date, requested_end_date = (
    #     validate_single_request_window(task_df)
    # )

    target_tickers = parse_tickers(args.tickers)

    # Full task list defines the batch scope.
    # A ticker filter only defines the processing subset for this run.
    full_task_df = load_task_list(
        config=config,
        task_list_name=args.task_list,
        target_tickers=None,
    )

    if target_tickers is not None:
        task_df = full_task_df[full_task_df["ticker"].isin(target_tickers)].copy()

        missing_tickers = sorted(set(target_tickers) - set(task_df["ticker"]))

        if task_df.empty:
            raise ValueError(
                "None of the requested tickers were found in task list. "
                f"Requested: {target_tickers}"
            )

        if missing_tickers:
            print(
                "Warning: some requested tickers were not found in task list: "
                f"{missing_tickers}"
            )

        task_df = task_df.reset_index(drop=True)
        task_df["_task_order"] = range(len(task_df))
    else:
        task_df = full_task_df.copy()

    source, dataset_name, requested_start_date, requested_end_date = (
        validate_single_request_window(full_task_df)
    )

    batch_id = make_batch_id(
        task_list_name=args.task_list,
        source=source,
        dataset_name=dataset_name,
        start_date=requested_start_date,
        end_date=requested_end_date,
    )

    print("\nTiingo backfill run")
    print("-------------------")
    print(f"Task list: {args.task_list}")
    print(f"Batch ID: {batch_id}")
    print(f"Task-list rows loaded: {len(task_df):,}")
    print(f"Requested tickers: {target_tickers or 'all eligible'}")
    print(f"Limit: {args.limit}")
    print(f"No GCS: {args.no_gcs}")
    print(f"Dry run: {args.dry_run}")

    with psycopg.connect(dsn) as conn:
        reset_stale_running_rows(
            conn=conn,
            source=source,
            dataset_name=dataset_name,
            requested_start_date=requested_start_date,
            requested_end_date=requested_end_date,
            stale_minutes=stale_minutes,
        )
        conn.commit()

        status_df = fetch_metadata_statuses(
            conn=conn,
            task_df=task_df,
            source=source,
            dataset_name=dataset_name,
            requested_start_date=requested_start_date,
            requested_end_date=requested_end_date,
        )

        tasks_to_process = select_tasks_to_process(
            task_df=task_df,
            status_df=status_df,
            max_attempts=max_attempts,
            limit=args.limit,
        )

        print(f"Eligible tasks to process: {len(tasks_to_process):,}")

        if tasks_to_process.empty:
            print("No eligible tasks to process.")
            return

        print("\nTasks selected:")
        print(
            tasks_to_process[
                [
                    "ticker",
                    "status",
                    "attempt_count",
                    "requested_start_date",
                    "requested_end_date",
                ]
            ]
            .head(30)
            .to_string(index=False)
        )

        if args.dry_run:
            print("\n[DRY RUN] No API calls or metadata updates were performed.")
            return

        mark_batch_running(conn, batch_id)
        conn.commit()

        for row in tasks_to_process.itertuples(index=False):
            row_series = pd.Series(row._asdict())
            ticker = row_series["ticker"]

            print(f"\nProcessing {ticker}...")

            try:
                mark_symbol_running(conn, row_series)
                conn.commit()

                rows = fetch_tiingo_prices(
                    ticker=ticker,
                    start_date=row_series["requested_start_date"],
                    end_date=row_series["requested_end_date"],
                    api_token=api_token,
                    timeout_seconds=timeout_seconds,
                    max_retries=max_retries,
                    retry_sleep_seconds=retry_sleep_seconds,
                )

                if not rows:
                    message = "Tiingo returned zero price rows."
                    mark_symbol_skipped(conn, row_series, message)
                    conn.commit()
                    print(f"Skipped {ticker}: {message}")
                    continue

                raw_output_path = get_raw_output_path(ods_root, ticker)
                write_raw_prices(raw_output_path, rows)

                if not args.no_gcs:
                    upload_raw_file_to_gcs(
                        local_path=raw_output_path,
                        ticker=ticker,
                        bucket_name=bucket_name,
                        gcs_ods_prefix=gcs_ods_prefix,
                        dry_run=False,
                    )

                last_successful_date = extract_last_successful_date(rows)

                if last_successful_date is None:
                    message = "Tiingo rows did not contain valid date values."
                    mark_symbol_skipped(conn, row_series, message)
                    conn.commit()
                    print(f"Skipped {ticker}: {message}")
                    continue

                mark_symbol_success(conn, row_series, last_successful_date)
                conn.commit()

                print(
                    f"Success {ticker}: rows={len(rows):,}, "
                    f"last_date={last_successful_date}"
                )

            except Exception as exc:
                mark_symbol_failed(conn, row_series, repr(exc))
                conn.commit()
                print(f"Failed {ticker}: {exc!r}")

            if sleep_between > 0:
                time.sleep(sleep_between)

        # final_counts = fetch_status_counts_for_task(
        #     conn=conn,
        #     task_df=task_df,
        #     source=source,
        #     dataset_name=dataset_name,
        #     requested_start_date=requested_start_date,
        #     requested_end_date=requested_end_date,
        # )

        # maybe_mark_batch_success(conn, batch_id, final_counts)

        # Use the full task list to decide whether the whole batch is complete.
        final_counts = fetch_status_counts_for_task(
            conn=conn,
            task_df=full_task_df,
            source=source,
            dataset_name=dataset_name,
            requested_start_date=requested_start_date,
            requested_end_date=requested_end_date,
        )

        maybe_mark_batch_success(conn, batch_id, final_counts)
        conn.commit()

    print("\nBackfill status after run:")
    print(final_counts.to_string(index=False))

    print("\nTiingo backfill run completed.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run Tiingo historical EOD backfill for task-list rows."
    )
    parser.add_argument(
        "--task-list",
        default="pilot_500",
        help="Task list name from configs/backfill.yml.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of eligible tasks to process.",
    )
    parser.add_argument(
        "--tickers",
        type=str,
        default=None,
        help="Optional comma-separated tickers to process, e.g. AAPL,MSFT,NVDA.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Select tasks but do not call Tiingo or update metadata.",
    )
    parser.add_argument(
        "--no-gcs",
        action="store_true",
        help="Do not upload raw ODS files to GCS.",
    )
    args = parser.parse_args()

    run_backfill(args)


if __name__ == "__main__":
    main()