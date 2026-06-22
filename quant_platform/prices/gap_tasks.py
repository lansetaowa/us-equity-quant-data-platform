from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd

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
) -> pd.DataFrame:
    """
    Build gap tasks for eligible daily update candidates.

    If an eligible ticker has no DWD rows, start from bootstrap_anchor_date + 1.
    If request_start_date > latest_complete_eod_date, no task is emitted.
    """
    if latest_complete_eod_date <= bootstrap_anchor_date:
        return _empty_gap_tasks()

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

    merged = base.merge(
        latest,
        on=["ticker", "security_id"],
        how="left",
    )

    merged["effective_latest_date"] = merged["latest_dwd_date"].apply(
        lambda x: x if pd.notna(x) else bootstrap_anchor_date
    )

    merged["request_start_date"] = merged["effective_latest_date"].apply(
        lambda x: x + timedelta(days=1)
    )
    merged["request_end_date"] = latest_complete_eod_date

    tasks = merged[
        merged["request_start_date"] <= merged["request_end_date"]
    ].copy()

    if tasks.empty:
        return _empty_gap_tasks()

    tasks["source"] = source
    tasks["dataset_name"] = dataset_name
    tasks["reason"] = tasks["latest_dwd_date"].apply(
        lambda x: "no_dwd_rows" if pd.isna(x) else "dwd_lag"
    )
    tasks["generated_at_utc"] = pd.Timestamp.now(tz="UTC")

    tasks = tasks[
        [
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
    ].sort_values(["ticker", "security_id"])

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

    tasks = build_price_gap_tasks(
        bootstrap_tasks=eligible_bootstrap_tasks,
        latest_dwd_dates=latest_dwd_dates,
        source=config.source,
        dataset_name=config.dataset_name,
        bootstrap_anchor_date=config.bootstrap_anchor_date,
        latest_complete_eod_date=config.latest_complete_eod_date,
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