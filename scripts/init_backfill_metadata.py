from __future__ import annotations

import argparse
import os
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg
import yaml
from dotenv import load_dotenv

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


def get_task_list_path(config: dict[str, Any], task_list_name: str) -> Path:
    """Get local task list path from config."""
    task_lists = config.get("task_lists", {})

    if task_list_name not in task_lists:
        raise KeyError(
            f"Task list '{task_list_name}' not found in configs/backfill.yml"
        )

    return resolve_project_path(task_lists[task_list_name]["local_path"])


def to_date(value: Any) -> date:
    """Convert value to Python date."""
    return pd.Timestamp(value).date()


def load_task_list(task_list_path: Path, task_list_name: str) -> pd.DataFrame:
    """Load and validate backfill task list."""
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
        "priority",
        "status",
        "created_at",
    ]

    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        raise ValueError(
            f"Task list {task_list_name} missing columns: {missing_columns}"
        )

    null_columns = [col for col in required_columns if df[col].isna().any()]

    if null_columns:
        raise ValueError(
            f"Task list {task_list_name} has nulls in required columns: "
            f"{null_columns}"
        )

    actual_task_names = set(df["task_list_name"].astype(str).unique())

    if actual_task_names != {task_list_name}:
        raise ValueError(
            f"Task list name mismatch. Expected {task_list_name}, "
            f"got {actual_task_names}"
        )

    if df["task_id"].astype(str).str.contains("nan", case=False, na=False).any():
        raise ValueError("Task list contains invalid task_id with 'nan'.")

    df = df.copy()
    df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
    df["security_id"] = df["security_id"].astype(str).str.strip()
    df["source"] = df["source"].astype(str).str.strip()
    df["dataset_name"] = df["dataset_name"].astype(str).str.strip()
    df["requested_start_date"] = df["requested_start_date"].map(to_date)
    df["requested_end_date"] = df["requested_end_date"].map(to_date)

    if df.empty:
        raise ValueError(f"Task list {task_list_name} is empty.")

    return df


def validate_single_request_window(df: pd.DataFrame) -> tuple[str, str, date, date]:
    """Validate that a task list uses one source/dataset/date window."""
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
    """Create deterministic batch ID."""
    return (
        f"{task_list_name}:{source}:{dataset_name}:"
        f"{start_date.isoformat()}:{end_date.isoformat()}"
    )


def upsert_batch(
    conn: psycopg.Connection,
    batch_id: str,
    task_list_name: str,
    source: str,
    dataset_name: str,
    symbols_count: int,
    data_start_date: date,
    data_end_date: date,
    reset_existing: bool,
) -> None:
    """Insert or update metadata.backfill_batches."""
    if reset_existing:
        sql = """
            INSERT INTO metadata.backfill_batches (
                batch_id,
                source,
                dataset_name,
                task_list_name,
                symbols_count,
                data_start_date,
                data_end_date,
                status,
                notes
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending', %s)
            ON CONFLICT (batch_id)
            DO UPDATE SET
                source = EXCLUDED.source,
                dataset_name = EXCLUDED.dataset_name,
                task_list_name = EXCLUDED.task_list_name,
                symbols_count = EXCLUDED.symbols_count,
                data_start_date = EXCLUDED.data_start_date,
                data_end_date = EXCLUDED.data_end_date,
                status = 'pending',
                started_at = NULL,
                ended_at = NULL,
                error_message = NULL,
                notes = EXCLUDED.notes,
                updated_at = now();
        """
    else:
        sql = """
            INSERT INTO metadata.backfill_batches (
                batch_id,
                source,
                dataset_name,
                task_list_name,
                symbols_count,
                data_start_date,
                data_end_date,
                status,
                notes
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending', %s)
            ON CONFLICT (batch_id)
            DO UPDATE SET
                source = EXCLUDED.source,
                dataset_name = EXCLUDED.dataset_name,
                task_list_name = EXCLUDED.task_list_name,
                symbols_count = EXCLUDED.symbols_count,
                data_start_date = EXCLUDED.data_start_date,
                data_end_date = EXCLUDED.data_end_date,
                notes = EXCLUDED.notes,
                updated_at = now();
        """

    note = "Initialized from parquet backfill task list."

    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                batch_id,
                source,
                dataset_name,
                task_list_name,
                symbols_count,
                data_start_date,
                data_end_date,
                note,
            ),
        )


def upsert_symbol_rows(
    conn: psycopg.Connection,
    task_df: pd.DataFrame,
    reset_existing: bool,
) -> None:
    """Insert or update metadata.symbol_ingestion_status rows."""
    if reset_existing:
        sql = """
            INSERT INTO metadata.symbol_ingestion_status (
                source,
                dataset_name,
                ticker,
                security_id,
                requested_start_date,
                requested_end_date,
                last_successful_date,
                status,
                attempt_count,
                last_error_message
            )
            VALUES (%s, %s, %s, %s, %s, %s, NULL, 'pending', 0, NULL)
            ON CONFLICT (
                source,
                dataset_name,
                ticker,
                requested_start_date,
                requested_end_date
            )
            DO UPDATE SET
                security_id = EXCLUDED.security_id,
                last_successful_date = NULL,
                status = 'pending',
                attempt_count = 0,
                last_error_message = NULL,
                updated_at = now();
        """
    else:
        sql = """
            INSERT INTO metadata.symbol_ingestion_status (
                source,
                dataset_name,
                ticker,
                security_id,
                requested_start_date,
                requested_end_date,
                last_successful_date,
                status,
                attempt_count,
                last_error_message
            )
            VALUES (%s, %s, %s, %s, %s, %s, NULL, 'pending', 0, NULL)
            ON CONFLICT (
                source,
                dataset_name,
                ticker,
                requested_start_date,
                requested_end_date
            )
            DO UPDATE SET
                security_id = EXCLUDED.security_id,
                updated_at = now();
        """

    rows = [
        (
            row.source,
            row.dataset_name,
            row.ticker,
            row.security_id,
            row.requested_start_date,
            row.requested_end_date,
        )
        for row in task_df.itertuples(index=False)
    ]

    with conn.cursor() as cur:
        cur.executemany(sql, rows)


def fetch_status_counts(
    conn: psycopg.Connection,
    source: str,
    dataset_name: str,
    requested_start_date: date,
    requested_end_date: date,
) -> pd.DataFrame:
    """Fetch status counts for a request window."""
    sql = """
        SELECT status, COUNT(*) AS n
        FROM metadata.symbol_ingestion_status
        WHERE source = %s
          AND dataset_name = %s
          AND requested_start_date = %s
          AND requested_end_date = %s
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
            ),
        )
        rows = cur.fetchall()

    return pd.DataFrame(rows, columns=["status", "n"])


def print_task_summary(
    task_df: pd.DataFrame,
    task_list_name: str,
    batch_id: str,
    source: str,
    dataset_name: str,
    requested_start_date: date,
    requested_end_date: date,
) -> None:
    """Print initialization summary."""
    print("\nBackfill metadata initialization")
    print("--------------------------------")
    print(f"Task list: {task_list_name}")
    print(f"Batch ID: {batch_id}")
    print(f"Source: {source}")
    print(f"Dataset: {dataset_name}")
    print(f"Requested start date: {requested_start_date}")
    print(f"Requested end date: {requested_end_date}")
    print(f"Symbols: {len(task_df):,}")

    print("\nSample tasks:")
    print(
        task_df[
            [
                "task_list_name",
                "source",
                "dataset_name",
                "ticker",
                "security_id",
                "requested_start_date",
                "requested_end_date",
                "status",
            ]
        ]
        .head(20)
        .to_string(index=False)
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Initialize Postgres metadata rows from a backfill task list."
    )
    parser.add_argument(
        "--task-list",
        default="pilot_500",
        help="Task list name from configs/backfill.yml.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and print planned metadata initialization without writing.",
    )
    parser.add_argument(
        "--reset-existing",
        action="store_true",
        help=(
            "Reset existing rows for this source/dataset/ticker/date window "
            "back to pending."
        ),
    )
    args = parser.parse_args()

    load_dotenv(ENV_PATH)

    dsn = os.getenv("POSTGRES_DSN")
    if not dsn:
        raise RuntimeError("POSTGRES_DSN is missing. Check your .env file.")

    config = load_config()
    task_list_path = get_task_list_path(config, args.task_list)
    task_df = load_task_list(task_list_path, args.task_list)

    source, dataset_name, requested_start_date, requested_end_date = (
        validate_single_request_window(task_df)
    )

    batch_id = make_batch_id(
        task_list_name=args.task_list,
        source=source,
        dataset_name=dataset_name,
        start_date=requested_start_date,
        end_date=requested_end_date,
    )

    print_task_summary(
        task_df=task_df,
        task_list_name=args.task_list,
        batch_id=batch_id,
        source=source,
        dataset_name=dataset_name,
        requested_start_date=requested_start_date,
        requested_end_date=requested_end_date,
    )

    if args.dry_run:
        print("\n[DRY RUN] No Postgres rows were inserted or updated.")
        return

    with psycopg.connect(dsn) as conn:
        upsert_batch(
            conn=conn,
            batch_id=batch_id,
            task_list_name=args.task_list,
            source=source,
            dataset_name=dataset_name,
            symbols_count=len(task_df),
            data_start_date=requested_start_date,
            data_end_date=requested_end_date,
            reset_existing=args.reset_existing,
        )

        upsert_symbol_rows(
            conn=conn,
            task_df=task_df,
            reset_existing=args.reset_existing,
        )

        conn.commit()

        status_counts = fetch_status_counts(
            conn=conn,
            source=source,
            dataset_name=dataset_name,
            requested_start_date=requested_start_date,
            requested_end_date=requested_end_date,
        )

    print("\nCurrent symbol ingestion status counts:")
    print(status_counts.to_string(index=False))

    print("\nMetadata initialization completed successfully.")


if __name__ == "__main__":
    main()