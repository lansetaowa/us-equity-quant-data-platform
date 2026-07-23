from __future__ import annotations

import argparse
import os
import re
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import psycopg
import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"
CONFIG_PATH = PROJECT_ROOT / "configs" / "backfill.yml"


SUSPICIOUS_TICKER_PATTERNS = [
    r"-R$",
    r"-RT$",
    r"-WS$",
    r"-W$",
    r"-WT$",
    r"-U$",
    r"-UN$",
    r"-P-[A-Z]$",
    r"-P[A-Z]$",
]


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
    """Resolve project-relative path."""
    path = Path(path_str)

    if path.is_absolute():
        return path

    return PROJECT_ROOT / path


def get_task_list_path(config: dict[str, Any], task_list_name: str) -> Path:
    """Return task list path from config."""
    task_lists = config.get("task_lists", {})

    if task_list_name not in task_lists:
        raise KeyError(f"Task list not found in config: {task_list_name}")

    return resolve_project_path(task_lists[task_list_name]["local_path"])


def load_task_list(config: dict[str, Any], task_list_name: str) -> pd.DataFrame:
    """Load task list parquet."""
    path = get_task_list_path(config, task_list_name)

    if not path.exists():
        raise FileNotFoundError(f"Task list not found: {path}")

    df = pd.read_parquet(path)

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

    df = df.copy()
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["security_id"] = df["security_id"].astype(str).str.strip()
    df["source"] = df["source"].astype(str).str.strip()
    df["dataset_name"] = df["dataset_name"].astype(str).str.strip()
    df["requested_start_date"] = pd.to_datetime(
        df["requested_start_date"],
        errors="coerce",
    ).dt.date
    df["requested_end_date"] = pd.to_datetime(
        df["requested_end_date"],
        errors="coerce",
    ).dt.date

    return df


def validate_single_request_window(df: pd.DataFrame) -> tuple[str, str, Any, Any]:
    """Validate task list has a single source/dataset/date window."""
    sources = sorted(df["source"].dropna().unique())
    datasets = sorted(df["dataset_name"].dropna().unique())
    start_dates = sorted(df["requested_start_date"].dropna().unique())
    end_dates = sorted(df["requested_end_date"].dropna().unique())

    if len(sources) != 1:
        raise ValueError(f"Expected one source, got {sources}")

    if len(datasets) != 1:
        raise ValueError(f"Expected one dataset_name, got {datasets}")

    if len(start_dates) != 1:
        raise ValueError(f"Expected one requested_start_date, got {start_dates}")

    if len(end_dates) != 1:
        raise ValueError(f"Expected one requested_end_date, got {end_dates}")

    return sources[0], datasets[0], start_dates[0], end_dates[0]


def fetch_symbol_status(
    dsn: str,
    source: str,
    dataset_name: str,
    requested_start_date: Any,
    requested_end_date: Any,
) -> pd.DataFrame:
    """Fetch symbol ingestion status rows from Postgres."""
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
        ORDER BY ticker;
    """

    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
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

    df = pd.DataFrame(rows, columns=columns)

    if not df.empty:
        df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()

    return df


def read_dwd_coverage(dwd_root: Path) -> pd.DataFrame:
    """Read ticker coverage from DWD parquet."""
    pattern = str(dwd_root / "**" / "part-*.parquet")

    con = duckdb.connect()

    try:
        df = con.execute(
            """
            SELECT
                ticker,
                security_id,
                COUNT(*) AS n_rows,
                MIN(date) AS min_date,
                MAX(date) AS max_date,
                SUM(CASE WHEN adj_close IS NULL OR adj_close <= 0 THEN 1 ELSE 0 END)
                    AS invalid_adj_close_rows,
                SUM(CASE WHEN volume IS NULL OR volume < 0 THEN 1 ELSE 0 END)
                    AS invalid_volume_rows
            FROM read_parquet(?)
            GROUP BY ticker, security_id
            ORDER BY ticker
            """,
            [pattern],
        ).fetchdf()
    except duckdb.IOException:
        df = pd.DataFrame(
            columns=[
                "ticker",
                "security_id",
                "n_rows",
                "min_date",
                "max_date",
                "invalid_adj_close_rows",
                "invalid_volume_rows",
            ]
        )

    if not df.empty:
        df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()

    return df


def read_dwd_duplicate_keys(dwd_root: Path) -> pd.DataFrame:
    """Find duplicate security_id/date rows in DWD."""
    pattern = str(dwd_root / "**" / "part-*.parquet")

    con = duckdb.connect()

    try:
        df = con.execute(
            """
            SELECT
                security_id,
                date,
                COUNT(*) AS n
            FROM read_parquet(?)
            GROUP BY security_id, date
            HAVING COUNT(*) > 1
            ORDER BY n DESC, security_id, date
            """,
            [pattern],
        ).fetchdf()
    except duckdb.IOException:
        df = pd.DataFrame(columns=["security_id", "date", "n"])

    return df


def build_suspicious_pattern_report(symbol_df: pd.DataFrame) -> pd.DataFrame:
    """Flag suspicious ticker suffixes, rights, warrants, units, etc."""
    if symbol_df.empty:
        return pd.DataFrame()

    regex = re.compile("|".join(SUSPICIOUS_TICKER_PATTERNS), flags=re.IGNORECASE)

    report = symbol_df[
        symbol_df["ticker"].astype(str).str.contains(regex, regex=True, na=False)
    ].copy()

    if not report.empty:
        report["suspicious_reason"] = "matches_special_security_ticker_pattern"

    return report


def write_report(df: pd.DataFrame, path: Path) -> None:
    """Write dataframe report to CSV."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    print(f"Wrote report: {path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Audit pilot Tiingo backfill coverage."
    )
    parser.add_argument(
        "--task-list",
        default="pilot_500",
        help="Task list name from configs/backfill.yml.",
    )
    parser.add_argument(
        "--low-row-threshold",
        type=int,
        default=252,
        help="Flag symbols with fewer than this many DWD rows.",
    )
    args = parser.parse_args()

    load_dotenv(ENV_PATH)

    dsn = os.getenv("POSTGRES_DSN")
    if not dsn:
        raise RuntimeError("POSTGRES_DSN is missing. Check your .env file.")

    config = load_config()
    task_df = load_task_list(config, args.task_list)

    source, dataset_name, requested_start_date, requested_end_date = (
        validate_single_request_window(task_df)
    )

    dwd_root = resolve_project_path(config["local_paths"]["dwd_final_root"])
    report_root = resolve_project_path(config["local_paths"]["audit_report_root"])
    task_report_root = report_root / args.task_list

    status_df = fetch_symbol_status(
        dsn=dsn,
        source=source,
        dataset_name=dataset_name,
        requested_start_date=requested_start_date,
        requested_end_date=requested_end_date,
    )

    coverage_df = read_dwd_coverage(dwd_root)
    duplicates_df = read_dwd_duplicate_keys(dwd_root)
    metadata_status_df = status_df[
        [
            "ticker",
            "last_successful_date",
            "status",
            "attempt_count",
            "last_error_message",
            "updated_at",
        ]
    ].rename(
        columns={
            "status": "metadata_status",
            "attempt_count": "metadata_attempt_count",
            "last_error_message": "metadata_last_error_message",
            "updated_at": "metadata_updated_at",
        }
    )

    audit_df = task_df.rename(columns={"status": "task_status"}).merge(
        metadata_status_df,
        on="ticker",
        how="left",
    ).merge(
        coverage_df[
            [
                "ticker",
                "n_rows",
                "min_date",
                "max_date",
                "invalid_adj_close_rows",
                "invalid_volume_rows",
            ]
        ],
        on="ticker",
        how="left",
    )

    audit_df["status"] = audit_df["metadata_status"]
    audit_df["attempt_count"] = audit_df["metadata_attempt_count"]
    audit_df["last_error_message"] = audit_df["metadata_last_error_message"]
    audit_df["updated_at"] = audit_df["metadata_updated_at"]

    if audit_df["status"].isna().any():
        missing = audit_df.loc[audit_df["status"].isna(), "ticker"].head(20).tolist()
        raise ValueError(
            "Some task-list tickers are missing metadata status rows. "
            f"Examples: {missing}"
        )

    audit_df["has_dwd_rows"] = audit_df["n_rows"].fillna(0) > 0
    audit_df["n_rows"] = audit_df["n_rows"].fillna(0).astype(int)

    status_summary = (
        audit_df.groupby("status", dropna=False)
        .size()
        .reset_index(name="n")
        .sort_values("status")
    )

    failed_or_skipped = audit_df[
        audit_df["status"].isin(["failed", "skipped"])
    ].copy()

    low_coverage = audit_df[
        (audit_df["status"] == "success")
        & (audit_df["n_rows"] < args.low_row_threshold)
    ].copy()

    suspicious = build_suspicious_pattern_report(audit_df)

    overall_summary = pd.DataFrame(
        [
            {
                "task_list_name": args.task_list,
                "source": source,
                "dataset_name": dataset_name,
                "requested_start_date": requested_start_date,
                "requested_end_date": requested_end_date,
                "task_count": len(task_df),
                "metadata_rows": len(status_df),
                "dwd_ticker_count": coverage_df["ticker"].nunique()
                if not coverage_df.empty
                else 0,
                "dwd_row_count": int(coverage_df["n_rows"].sum())
                if not coverage_df.empty
                else 0,
                "duplicate_key_count": int(duplicates_df["n"].sum())
                if not duplicates_df.empty
                else 0,
                "low_row_threshold": args.low_row_threshold,
                "low_coverage_symbol_count": len(low_coverage),
                "failed_or_skipped_symbol_count": len(failed_or_skipped),
                "suspicious_pattern_symbol_count": len(suspicious),
            }
        ]
    )

    print("\nBackfill audit summary")
    print("----------------------")
    print(overall_summary.to_string(index=False))

    print("\nStatus summary:")
    print(status_summary.to_string(index=False))

    print("\nFailed or skipped symbols:")
    if failed_or_skipped.empty:
        print("None")
    else:
        print(
            failed_or_skipped[
                ["ticker", "status", "attempt_count", "last_error_message"]
            ]
            .head(50)
            .to_string(index=False)
        )

    print("\nLow coverage symbols:")
    if low_coverage.empty:
        print("None")
    else:
        print(
            low_coverage[
                ["ticker", "status", "n_rows", "min_date", "max_date"]
            ]
            .head(50)
            .to_string(index=False)
        )

    print("\nSuspicious ticker-pattern symbols:")
    if suspicious.empty:
        print("None")
    else:
        print(
            suspicious[
                ["ticker", "status", "n_rows", "min_date", "max_date"]
            ]
            .head(50)
            .to_string(index=False)
        )

    write_report(overall_summary, task_report_root / "coverage_summary.csv")
    write_report(status_summary, task_report_root / "status_summary.csv")
    write_report(audit_df, task_report_root / "symbol_coverage.csv")
    write_report(
        failed_or_skipped,
        task_report_root / "failed_or_skipped_symbols.csv",
    )
    write_report(
        low_coverage,
        task_report_root / "low_coverage_symbols.csv",
    )
    write_report(
        suspicious,
        task_report_root / "suspicious_ticker_patterns.csv",
    )
    write_report(duplicates_df, task_report_root / "duplicate_keys.csv")

    print("\nBackfill audit completed.")


if __name__ == "__main__":
    main()