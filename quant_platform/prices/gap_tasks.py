from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd
import psycopg

from quant_platform.calendar.eod import (
    EodResolutionConfig,
    resolve_latest_complete_eod_date,
)
from quant_platform.config.loaders import (
    load_yaml,
    optional_mapping,
    parse_iso_date,
    require_mapping,
    require_value,
)
from quant_platform.paths.data_lake import (
    BOOTSTRAP_CANDIDATES_TASK_LIST_PATH,
    DIM_SECURITY_PATH,
    DWD_PRICE_ROOT,
    PRICE_GAP_EXCLUDED_SYMBOLS_PATH,
    PRICE_GAP_TASK_LIST_PATH,
    PRICE_UPDATE_CONFIG_PATH,
    ensure_parent_dir,
)

DEFAULT_CONFIG_PATH = PRICE_UPDATE_CONFIG_PATH
DEFAULT_BOOTSTRAP_TASK_LIST_PATH = BOOTSTRAP_CANDIDATES_TASK_LIST_PATH
DEFAULT_DIM_SECURITY_PATH = DIM_SECURITY_PATH
DEFAULT_DWD_PRICE_ROOT = DWD_PRICE_ROOT
DEFAULT_OUTPUT_PATH = PRICE_GAP_TASK_LIST_PATH
DEFAULT_EXCLUDED_OUTPUT_PATH = PRICE_GAP_EXCLUDED_SYMBOLS_PATH

@dataclass(frozen=True)
class PriceGapTaskConfig:
    source: str
    dataset_name: str
    bootstrap_anchor_date: date
    latest_complete_eod_date: date
    bootstrap_task_list_path: Path
    dim_security_path: Path
    dwd_price_root: Path
    output_path: Path
    excluded_output_path: Path
    active_end_date_grace_days: int
    use_postgres_metadata: bool
    metadata_dsn_env_var: str
    max_failed_attempts: int

def build_eod_resolution_config(config_data: dict) -> EodResolutionConfig:
    price_update = require_mapping(config_data, "price_update")
    eod_resolution = optional_mapping(
        price_update,
        "eod_resolution",
        context="price_update",
    )

    return EodResolutionConfig(
        manual_latest_complete_eod_date=price_update.get(
            "latest_complete_eod_date"
        ),
        market_calendar=eod_resolution.get("market_calendar", "XNYS"),
        market_timezone=eod_resolution.get(
            "market_timezone",
            "America/New_York",
        ),
        market_close_buffer_minutes=int(
            eod_resolution.get("market_close_buffer_minutes", 90)
        ),
    )

def load_price_gap_task_config(
    config_path: Path,
    bootstrap_task_list_path: Path,
    dim_security_path: Path,
    dwd_price_root: Path,
    output_path: Path,
    excluded_output_path: Path,
) -> PriceGapTaskConfig:
    config_data = load_yaml(config_path)

    price_update = require_mapping(config_data, "price_update")
    daily_update_universe = optional_mapping(
        price_update,
        "daily_update_universe",
        context="price_update",
    )

    metadata_config = optional_mapping(
        price_update,
        "metadata",
        context="price_update",
    )

    source = str(price_update.get("source", "tiingo")).strip()
    dataset_name = str(
        price_update.get("dataset_name", "equity_price_daily")
    ).strip()

    bootstrap_anchor_raw = require_value(
        price_update,
        "bootstrap_anchor_date",
        context="price_update",
    )

    active_end_date_grace_days = int(
        daily_update_universe.get("active_end_date_grace_days", 7)
    )

    if active_end_date_grace_days < 0:
        raise ValueError("active_end_date_grace_days must be >= 0")

    max_failed_attempts = int(
        daily_update_universe.get("max_failed_attempts", 3)
    )

    if max_failed_attempts < 0:
        raise ValueError("max_failed_attempts must be >= 0")

    use_postgres_metadata = bool(
        metadata_config.get("use_postgres", True)
    )
    metadata_dsn_env_var = str(
        metadata_config.get("dsn_env_var", "POSTGRES_DSN")
    ).strip()

    if not metadata_dsn_env_var:
        raise ValueError("metadata.dsn_env_var must not be empty")

    eod_config = build_eod_resolution_config(config_data)
    latest_complete_eod_date = resolve_latest_complete_eod_date(eod_config)

    return PriceGapTaskConfig(
        source=source,
        dataset_name=dataset_name,
        bootstrap_anchor_date=parse_iso_date(bootstrap_anchor_raw),
        latest_complete_eod_date=latest_complete_eod_date,
        bootstrap_task_list_path=bootstrap_task_list_path,
        dim_security_path=dim_security_path,
        dwd_price_root=dwd_price_root,
        output_path=output_path,
        excluded_output_path=excluded_output_path,
        active_end_date_grace_days=active_end_date_grace_days,
        use_postgres_metadata=use_postgres_metadata,
        metadata_dsn_env_var=metadata_dsn_env_var,
        max_failed_attempts=max_failed_attempts,
    )

def _standardize_symbol_keys(df: pd.DataFrame) -> pd.DataFrame:
    required_columns = {"ticker", "security_id"}
    missing = required_columns - set(df.columns)

    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    output = df.dropna(subset=["ticker", "security_id"]).copy()
    output["ticker"] = output["ticker"].astype(str).str.strip().str.upper()
    output["security_id"] = output["security_id"].astype(str).str.strip()

    output = output[
        (output["ticker"] != "")
        & (output["security_id"] != "")
        & (output["ticker"].str.lower() != "nan")
        & (output["security_id"].str.lower() != "nan")
    ]

    return output


def load_bootstrap_task_list(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Bootstrap task list not found: {path}")

    df = pd.read_parquet(path)
    output = _standardize_symbol_keys(df)

    return output.drop_duplicates(subset=["ticker", "security_id"])


def load_dim_security(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"dim_security not found: {path}")

    df = pd.read_parquet(path)

    if "end_date" not in df.columns:
        raise ValueError("dim_security must contain an 'end_date' column")

    output = _standardize_symbol_keys(df)

    output["end_date"] = pd.to_datetime(
        output["end_date"],
        errors="coerce",
    ).dt.date

    if "is_active" not in output.columns:
        output["is_active"] = False

    keep_columns = [
        col
        for col in [
            "ticker",
            "security_id",
            "end_date",
            "is_active",
            "asset_type",
            "exchange",
            "currency",
        ]
        if col in output.columns
    ]

    return output[keep_columns].drop_duplicates(subset=["ticker", "security_id"])


def _truthy_series(series: pd.Series) -> pd.Series:
    normalized = series.astype("string").str.strip().str.lower()

    return normalized.isin({"true", "1", "yes", "y"})

def attach_daily_update_eligibility(
    bootstrap_tasks: pd.DataFrame,
    dim_security: pd.DataFrame,
    bootstrap_anchor_date: date,
    active_end_date_grace_days: int,
) -> pd.DataFrame:
    """
    Add daily update eligibility to bootstrap tasks.

    The bootstrap universe is historical. Daily updates should target only
    active or plausibly active securities.

    Eligibility rule:
    - eligible if is_active is true, OR
    - end_date is null, OR
    - end_date >= bootstrap_anchor_date - grace_days

    We compare against bootstrap_anchor_date instead of latest_complete_eod_date
    because the security master snapshot may lag the live update date.
    """
    if active_end_date_grace_days < 0:
        raise ValueError("active_end_date_grace_days must be >= 0")

    bootstrap = _standardize_symbol_keys(bootstrap_tasks)[
        ["ticker", "security_id"]
    ].drop_duplicates()

    security = _standardize_symbol_keys(dim_security)

    if "end_date" not in security.columns:
        raise ValueError("dim_security must contain an 'end_date' column")

    if "is_active" not in security.columns:
        security["is_active"] = False

    cutoff_date = bootstrap_anchor_date - timedelta(
        days=active_end_date_grace_days
    )
    cutoff_ts = pd.Timestamp(cutoff_date)

    security_cols = [
        col
        for col in [
            "ticker",
            "security_id",
            "end_date",
            "is_active",
            "asset_type",
            "exchange",
            "currency",
        ]
        if col in security.columns
    ]

    merged = bootstrap.merge(
        security[security_cols].drop_duplicates(
            subset=["ticker", "security_id"]
        ),
        on=["ticker", "security_id"],
        how="left",
        indicator=True,
    )

    end_date_ts = pd.to_datetime(merged["end_date"], errors="coerce")
    is_active_bool = _truthy_series(merged["is_active"])

    missing_security = merged["_merge"] != "both"
    has_open_end_date = end_date_ts.isna()
    has_recent_end_date = end_date_ts >= cutoff_ts

    eligible_by_status = (
        is_active_bool | has_open_end_date | has_recent_end_date
    )

    merged["eligible_for_daily_update"] = (
        ~missing_security & eligible_by_status
    )
    merged["daily_update_cutoff_date"] = cutoff_date
    merged["daily_update_exclusion_reason"] = pd.NA

    merged.loc[
        missing_security,
        "daily_update_exclusion_reason",
    ] = "missing_dim_security"

    merged.loc[
        ~missing_security & ~eligible_by_status,
        "daily_update_exclusion_reason",
    ] = "inactive_or_stale_end_date"

    merged["end_date"] = end_date_ts.dt.date

    return merged.drop(columns=["_merge"])

def _empty_latest_dwd_dates() -> pd.DataFrame:
    return pd.DataFrame(columns=["ticker", "security_id", "latest_dwd_date"])


def load_latest_dwd_dates(dwd_price_root: Path) -> pd.DataFrame:
    """
    Read latest available DWD date by ticker/security_id from local Parquet.

    Returns an empty DataFrame if the DWD root or Parquet files are absent.
    """
    if not dwd_price_root.exists():
        return _empty_latest_dwd_dates()

    parquet_paths = list(dwd_price_root.rglob("*.parquet"))
    if not parquet_paths:
        return _empty_latest_dwd_dates()

    parquet_glob = (dwd_price_root / "**" / "*.parquet").as_posix()

    con = duckdb.connect()

    try:
        latest = con.execute(
            """
            SELECT
                UPPER(TRIM(CAST(ticker AS VARCHAR))) AS ticker,
                TRIM(CAST(security_id AS VARCHAR)) AS security_id,
                MAX(CAST(date AS DATE)) AS latest_dwd_date
            FROM read_parquet(?)
            GROUP BY 1, 2
            """,
            [parquet_glob],
        ).fetchdf()
    finally:
        con.close()

    if latest.empty:
        return _empty_latest_dwd_dates()

    latest["latest_dwd_date"] = pd.to_datetime(
        latest["latest_dwd_date"],
        errors="coerce",
    ).dt.date

    return latest


def _empty_latest_window_metadata() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "ticker",
            "security_id",
            "metadata_status",
            "metadata_requested_start_date",
            "metadata_requested_end_date",
            "metadata_last_successful_date",
            "metadata_last_price_date",
            "metadata_attempt_count",
            "metadata_last_run_id",
            "metadata_last_action",
            "metadata_checked_through_date",
        ]
    )


def _date_or_none(value) -> date | None:
    """Normalize scalar date-like values, including pd.NaT, into date or None."""
    if value is None:
        return None

    if pd.isna(value):
        return None

    timestamp = pd.Timestamp(value)

    if pd.isna(timestamp):
        return None

    return timestamp.date()


def _max_date_or_none(values: list[object]) -> date | None:
    """Return max valid date, ignoring None / NaT / NA values."""
    usable: list[date] = []

    for value in values:
        parsed = _date_or_none(value)

        if parsed is not None:
            usable.append(parsed)

    if not usable:
        return None

    return max(usable)


def load_latest_window_metadata(
    dsn: str,
    *,
    source: str,
    dataset_name: str,
) -> pd.DataFrame:
    """
    Load the latest operational metadata row per ticker/security_id.

    This table represents current per-window state. It prevents repeatedly
    generating the same empty or already-checked window.
    """
    sql = """
    WITH ranked AS (
        SELECT
            ticker,
            security_id,
            status,
            requested_start_date,
            requested_end_date,
            last_successful_date,
            last_price_date,
            attempt_count,
            last_run_id,
            last_action,
            ROW_NUMBER() OVER (
                PARTITION BY ticker, security_id
                ORDER BY
                    requested_end_date DESC,
                    COALESCE(
                        last_completed_at,
                        updated_at,
                        created_at
                    ) DESC,
                    requested_start_date DESC
            ) AS rn
        FROM metadata.symbol_ingestion_status
        WHERE source = %s
          AND dataset_name = %s
    )
    SELECT
        ticker,
        security_id,
        status AS metadata_status,
        requested_start_date AS metadata_requested_start_date,
        requested_end_date AS metadata_requested_end_date,
        last_successful_date AS metadata_last_successful_date,
        last_price_date AS metadata_last_price_date,
        attempt_count AS metadata_attempt_count,
        last_run_id AS metadata_last_run_id,
        last_action AS metadata_last_action
    FROM ranked
    WHERE rn = 1;
    """

    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(sql, (source, dataset_name))
        rows = cur.fetchall()
        columns = [description.name for description in cur.description]

    if not rows:
        return _empty_latest_window_metadata()

    output = pd.DataFrame(rows, columns=columns)

    output["ticker"] = (
        output["ticker"]
        .astype(str)
        .str.strip()
        .str.upper()
    )
    output["security_id"] = output["security_id"].astype(str).str.strip()
    output["metadata_status"] = (
        output["metadata_status"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    for column in [
        "metadata_requested_start_date",
        "metadata_requested_end_date",
        "metadata_last_successful_date",
        "metadata_last_price_date",
    ]:
        output[column] = (
            pd.to_datetime(output[column], errors="coerce")
            .map(_date_or_none)
        )

    output["metadata_attempt_count"] = (
        pd.to_numeric(
            output["metadata_attempt_count"],
            errors="coerce",
        )
        .fillna(0)
        .astype(int)
    )

    def checked_through(row) -> date | None:
        status = row["metadata_status"]

        if status == "success":
            return _max_date_or_none(
                [
                    row["metadata_last_successful_date"],
                    row["metadata_last_price_date"],
                    row["metadata_requested_end_date"],
                ]
            )

        if status in {"empty", "skipped"}:
            return _date_or_none(row["metadata_requested_end_date"])

        return None

    output["metadata_checked_through_date"] = output.apply(
        checked_through,
        axis=1,
    )

    return output.sort_values(["ticker", "security_id"]).reset_index(drop=True)

def _empty_gap_tasks() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "source",
            "dataset_name",
            "ticker",
            "security_id",
            "latest_dwd_date",
            "request_start_date",
            "request_end_date",
            "reason",
            "generated_at_utc",
        ]
    )

def build_price_gap_tasks(
    bootstrap_tasks: pd.DataFrame,
    latest_dwd_dates: pd.DataFrame,
    source: str,
    dataset_name: str,
    bootstrap_anchor_date: date,
    latest_complete_eod_date: date,
    latest_window_metadata: pd.DataFrame | None = None,
    max_failed_attempts: int = 3,
) -> pd.DataFrame:
    """
    Build gap tasks for eligible daily update candidates.

    Effective state combines:
    - local DWD latest date
    - Postgres operational window metadata

    Rules:
    - success: advance from latest successful/price date
    - empty: advance from requested_end_date, avoiding repeat empty windows
    - failed: retry from failed requested_start_date until retry limit
    - skipped: exclude from daily update
    """
    if max_failed_attempts < 0:
        raise ValueError("max_failed_attempts must be >= 0")

    output_columns = [
        "source",
        "dataset_name",
        "ticker",
        "security_id",
        "latest_dwd_date",
        "metadata_status",
        "metadata_requested_start_date",
        "metadata_requested_end_date",
        "metadata_checked_through_date",
        "request_start_date",
        "request_end_date",
        "reason",
        "generated_at_utc",
    ]

    if latest_complete_eod_date <= bootstrap_anchor_date:
        return pd.DataFrame(columns=output_columns)

    base = _standardize_symbol_keys(bootstrap_tasks)[
        ["ticker", "security_id"]
    ].drop_duplicates()

    if latest_dwd_dates.empty:
        latest = _empty_latest_dwd_dates()
    else:
        latest = _standardize_symbol_keys(latest_dwd_dates)
        latest = latest[["ticker", "security_id", "latest_dwd_date"]].copy()
        latest["latest_dwd_date"] = pd.to_datetime(
            latest["latest_dwd_date"],
            errors="coerce",
        ).dt.date

    if latest_window_metadata is None or latest_window_metadata.empty:
        metadata = _empty_latest_window_metadata()
    else:
        metadata = latest_window_metadata.copy()
        metadata["ticker"] = (
            metadata["ticker"]
            .astype(str)
            .str.strip()
            .str.upper()
        )
        metadata["security_id"] = metadata["security_id"].astype(str).str.strip()

    merged = base.merge(
        latest,
        on=["ticker", "security_id"],
        how="left",
    ).merge(
        metadata,
        on=["ticker", "security_id"],
        how="left",
    )

    def dwd_latest_or_none(row) -> date | None:
        return _date_or_none(row.get("latest_dwd_date"))

    def baseline_latest(row) -> date:
        """
        Use local DWD latest date when available.

        bootstrap_anchor_date is only the fallback for symbols with no DWD rows.
        It must not mask a real latest_dwd_date that is earlier than the
        bootstrap anchor.
        """
        return dwd_latest_or_none(row) or bootstrap_anchor_date

    def effective_latest(row) -> date:
        return _max_date_or_none(
            [
                baseline_latest(row),
                _date_or_none(row.get("metadata_checked_through_date")),
            ]
        ) or bootstrap_anchor_date

    def request_start(row) -> date | None:
        status = row.get("metadata_status")

        if status == "skipped":
            return None

        if status == "failed":
            attempts = int(row.get("metadata_attempt_count") or 0)

            if attempts >= max_failed_attempts:
                return None

            failed_start = _date_or_none(
                row.get("metadata_requested_start_date")
            )

            if failed_start is None:
                return baseline_latest(row) + timedelta(days=1)

            latest_dwd = dwd_latest_or_none(row)

            if latest_dwd is not None and latest_dwd >= failed_start:
                return latest_dwd + timedelta(days=1)

            return failed_start

        return effective_latest(row) + timedelta(days=1)

    def reason(row) -> str:
        status = row.get("metadata_status")

        if status == "failed":
            attempts = int(row.get("metadata_attempt_count") or 0)

            if attempts >= max_failed_attempts:
                return "failed_retry_limit_exceeded"

            return "retry_failed_window"

        if status == "skipped":
            return "metadata_skipped"

        metadata_checked = _date_or_none(
            row.get("metadata_checked_through_date")
        )
        latest_dwd = _date_or_none(row.get("latest_dwd_date"))

        if status == "empty" and metadata_checked is not None:
            return "metadata_empty_checked_through"

        if metadata_checked is not None and (
            latest_dwd is None or metadata_checked > latest_dwd
        ):
            return "metadata_checked_through"

        if latest_dwd is None:
            return "no_dwd_rows"

        return "dwd_lag"

    merged["request_start_date"] = merged.apply(request_start, axis=1)
    merged["request_end_date"] = latest_complete_eod_date
    merged["reason"] = merged.apply(reason, axis=1)

    tasks = merged[
        merged["request_start_date"].notna()
        & (merged["request_start_date"] <= merged["request_end_date"])
    ].copy()

    if tasks.empty:
        return pd.DataFrame(columns=output_columns)

    tasks["source"] = source
    tasks["dataset_name"] = dataset_name
    tasks["generated_at_utc"] = pd.Timestamp.now(tz="UTC")

    tasks = tasks[output_columns].sort_values(["ticker", "security_id"])

    return tasks.reset_index(drop=True)

def save_frame(df: pd.DataFrame, output_path: Path) -> None:
    ensure_parent_dir(output_path)
    df.to_parquet(output_path, index=False)

def print_summary(
    tasks: pd.DataFrame,
    excluded_tasks: pd.DataFrame,
    latest_complete_eod_date: date,
    output_path: Path,
    excluded_output_path: Path,
    total_bootstrap_count: int,
    eligible_count: int,
) -> None:
    print("Price gap task generation complete")
    print(f"latest_complete_eod_date: {latest_complete_eod_date}")
    print(f"output_path: {output_path}")
    print(f"excluded_output_path: {excluded_output_path}")
    print(f"total_bootstrap_count: {total_bootstrap_count}")
    print(f"eligible_daily_update_count: {eligible_count}")
    print(f"excluded_daily_update_count: {len(excluded_tasks)}")
    print(f"task_count: {len(tasks)}")

    if not excluded_tasks.empty:
        print("\nExcluded symbols sample:")
        sample_cols = [
            col
            for col in [
                "ticker",
                "security_id",
                "end_date",
                "is_active",
                "daily_update_exclusion_reason",
            ]
            if col in excluded_tasks.columns
        ]
        print(excluded_tasks[sample_cols].head(20).to_string(index=False))

    if tasks.empty:
        print("\nNo price gaps found.")
        return

    print("\nReason counts:")
    print(tasks["reason"].value_counts(dropna=False).to_string())

    print("\nRequest window summary:")
    print(f"request_start_date min: {tasks['request_start_date'].min()}")
    print(f"request_start_date max: {tasks['request_start_date'].max()}")
    print(f"request_end_date min:   {tasks['request_end_date'].min()}")
    print(f"request_end_date max:   {tasks['request_end_date'].max()}")

    print("\nSample tasks:")
    print(tasks.head(20).to_string(index=False))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate Tiingo price gap tasks for windowed incremental updates."
        )
    )

    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="Path to price update config YAML.",
    )
    parser.add_argument(
        "--bootstrap-task-list",
        type=Path,
        default=DEFAULT_BOOTSTRAP_TASK_LIST_PATH,
        help="Path to formal bootstrap candidate task list Parquet.",
    )
    parser.add_argument(
        "--dim-security",
        type=Path,
        default=DEFAULT_DIM_SECURITY_PATH,
        help="Path to dim_security Parquet.",
    )
    parser.add_argument(
        "--dwd-price-root",
        type=Path,
        default=DEFAULT_DWD_PRICE_ROOT,
        help="Path to local DWD price Parquet root.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="Output path for generated price gap task list.",
    )
    parser.add_argument(
        "--excluded-output",
        type=Path,
        default=DEFAULT_EXCLUDED_OUTPUT_PATH,
        help="Output path for daily-update exclusions.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print summary without writing output Parquet files.",
    )

    parser.add_argument(
        "--ignore-postgres-metadata",
        action="store_true",
        help=(
            "Ignore Postgres operational metadata and generate tasks "
            "from local DWD only. Intended for debugging only."
        ),
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    config = load_price_gap_task_config(
        config_path=args.config,
        bootstrap_task_list_path=args.bootstrap_task_list,
        dim_security_path=args.dim_security,
        dwd_price_root=args.dwd_price_root,
        output_path=args.output,
        excluded_output_path=args.excluded_output,
    )

    bootstrap_tasks = load_bootstrap_task_list(config.bootstrap_task_list_path)
    dim_security = load_dim_security(config.dim_security_path)

    bootstrap_with_eligibility = attach_daily_update_eligibility(
        bootstrap_tasks=bootstrap_tasks,
        dim_security=dim_security,
        bootstrap_anchor_date=config.bootstrap_anchor_date,
        active_end_date_grace_days=config.active_end_date_grace_days,
    )

    eligible_bootstrap_tasks = bootstrap_with_eligibility[
        bootstrap_with_eligibility["eligible_for_daily_update"]
    ].copy()

    excluded_bootstrap_tasks = bootstrap_with_eligibility[
        ~bootstrap_with_eligibility["eligible_for_daily_update"]
    ].copy()

    latest_dwd_dates = load_latest_dwd_dates(config.dwd_price_root)

    latest_window_metadata = _empty_latest_window_metadata()

    if config.use_postgres_metadata and not args.ignore_postgres_metadata:
        metadata_dsn = os.getenv(config.metadata_dsn_env_var)

        if not metadata_dsn:
            raise RuntimeError(
                f"{config.metadata_dsn_env_var} is required because "
                "price_update.metadata.use_postgres is true"
            )

        latest_window_metadata = load_latest_window_metadata(
            metadata_dsn,
            source=config.source,
            dataset_name=config.dataset_name,
        )

    tasks = build_price_gap_tasks(
        bootstrap_tasks=eligible_bootstrap_tasks,
        latest_dwd_dates=latest_dwd_dates,
        source=config.source,
        dataset_name=config.dataset_name,
        bootstrap_anchor_date=config.bootstrap_anchor_date,
        latest_complete_eod_date=config.latest_complete_eod_date,
        latest_window_metadata=latest_window_metadata,
        max_failed_attempts=config.max_failed_attempts,
    )

    print_summary(
        tasks=tasks,
        excluded_tasks=excluded_bootstrap_tasks,
        latest_complete_eod_date=config.latest_complete_eod_date,
        output_path=config.output_path,
        excluded_output_path=config.excluded_output_path,
        total_bootstrap_count=len(bootstrap_tasks),
        eligible_count=len(eligible_bootstrap_tasks),
    )

    if args.dry_run:
        print("\ndry_run: true, no files written")
        return

    save_frame(tasks, config.output_path)
    save_frame(excluded_bootstrap_tasks, config.excluded_output_path)

    print("\nSaved price gap task list and exclusion list.")