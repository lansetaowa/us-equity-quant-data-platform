from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import yaml

from scripts.eod_date_resolver import (
    EodResolutionConfig,
    resolve_latest_complete_eod_date,
)


DEFAULT_CONFIG_PATH = Path("configs/price_update.yml")
DEFAULT_BOOTSTRAP_TASK_LIST_PATH = Path(
    "data/dwd/security_master/backfill_task_list_bootstrap_candidates.parquet"
)
DEFAULT_DWD_PRICE_ROOT = Path("data/dwd/equity_price_daily")
DEFAULT_OUTPUT_PATH = Path("data/dwd/security_master/price_gap_task_list.parquet")


@dataclass(frozen=True)
class PriceGapTaskConfig:
    source: str
    dataset_name: str
    bootstrap_anchor_date: date
    latest_complete_eod_date: date
    bootstrap_task_list_path: Path
    dwd_price_root: Path
    output_path: Path


def parse_iso_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value).strip())


def load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if not isinstance(data, dict):
        raise ValueError(f"Config file must contain a YAML mapping: {path}")

    return data


def build_eod_resolution_config(config_data: dict[str, Any]) -> EodResolutionConfig:
    price_update = config_data.get("price_update", {})
    eod_resolution = price_update.get("eod_resolution", {})

    return EodResolutionConfig(
        manual_latest_complete_eod_date=price_update.get("latest_complete_eod_date"),
        market_calendar=eod_resolution.get("market_calendar", "XNYS"),
        market_timezone=eod_resolution.get("market_timezone", "America/New_York"),
        market_close_buffer_minutes=int(
            eod_resolution.get("market_close_buffer_minutes", 90)
        ),
    )


def load_price_gap_task_config(
    config_path: Path,
    bootstrap_task_list_path: Path,
    dwd_price_root: Path,
    output_path: Path,
) -> PriceGapTaskConfig:
    config_data = load_yaml(config_path)

    price_update = config_data.get("price_update")
    if not isinstance(price_update, dict):
        raise ValueError("Config must contain a 'price_update' mapping")

    source = str(price_update.get("source", "tiingo"))
    dataset_name = str(price_update.get("dataset_name", "equity_price_daily"))

    bootstrap_anchor_raw = price_update.get("bootstrap_anchor_date")
    if not bootstrap_anchor_raw:
        raise ValueError("price_update.bootstrap_anchor_date is required")

    eod_config = build_eod_resolution_config(config_data)
    latest_complete_eod_date = resolve_latest_complete_eod_date(eod_config)

    return PriceGapTaskConfig(
        source=source,
        dataset_name=dataset_name,
        bootstrap_anchor_date=parse_iso_date(bootstrap_anchor_raw),
        latest_complete_eod_date=latest_complete_eod_date,
        bootstrap_task_list_path=bootstrap_task_list_path,
        dwd_price_root=dwd_price_root,
        output_path=output_path,
    )


def load_bootstrap_task_list(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Bootstrap task list not found: {path}")

    df = pd.read_parquet(path)

    required_columns = {"ticker", "security_id"}
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(
            f"Bootstrap task list missing required columns: {sorted(missing)}"
        )

    output = df.copy()
    output["ticker"] = output["ticker"].astype(str).str.strip().str.upper()
    output["security_id"] = output["security_id"].astype(str).str.strip()

    output = output.dropna(subset=["ticker", "security_id"])
    output = output[(output["ticker"] != "") & (output["security_id"] != "")]
    output = output.drop_duplicates(subset=["ticker", "security_id"])

    return output


def load_latest_dwd_dates(dwd_price_root: Path) -> pd.DataFrame:
    """
    Read latest available DWD date by ticker/security_id from local Parquet.

    Returns empty DataFrame if DWD root does not exist.
    """
    if not dwd_price_root.exists():
        return pd.DataFrame(
            columns=["ticker", "security_id", "latest_dwd_date"]
        )

    parquet_glob = str(dwd_price_root / "**" / "*.parquet")

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
        return pd.DataFrame(
            columns=["ticker", "security_id", "latest_dwd_date"]
        )

    latest["latest_dwd_date"] = pd.to_datetime(
        latest["latest_dwd_date"]
    ).dt.date

    return latest


def build_price_gap_tasks(
    bootstrap_tasks: pd.DataFrame,
    latest_dwd_dates: pd.DataFrame,
    source: str,
    dataset_name: str,
    bootstrap_anchor_date: date,
    latest_complete_eod_date: date,
) -> pd.DataFrame:
    """
    Build gap tasks from latest local DWD date to latest complete EOD date.

    If a ticker has no DWD rows, start from bootstrap_anchor_date + 1.
    If request_start_date > latest_complete_eod_date, no task is emitted.
    """
    if latest_complete_eod_date <= bootstrap_anchor_date:
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

    base = bootstrap_tasks[["ticker", "security_id"]].copy()

    latest = latest_dwd_dates[["ticker", "security_id", "latest_dwd_date"]].copy()

    merged = base.merge(
        latest,
        on=["ticker", "security_id"],
        how="left",
    )

    default_latest = bootstrap_anchor_date
    merged["effective_latest_date"] = merged["latest_dwd_date"].apply(
        lambda x: x if pd.notna(x) else default_latest
    )

    merged["request_start_date"] = merged["effective_latest_date"].apply(
        lambda x: x + timedelta(days=1)
    )
    merged["request_end_date"] = latest_complete_eod_date

    tasks = merged[
        merged["request_start_date"] <= merged["request_end_date"]
    ].copy()

    tasks["source"] = source
    tasks["dataset_name"] = dataset_name
    tasks["reason"] = tasks["latest_dwd_date"].apply(
        lambda x: "no_dwd_rows" if pd.isna(x) else "dwd_lag"
    )
    tasks["generated_at_utc"] = pd.Timestamp.utcnow()

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


def save_tasks(tasks: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tasks.to_parquet(output_path, index=False)


def print_summary(
    tasks: pd.DataFrame,
    latest_complete_eod_date: date,
    output_path: Path,
) -> None:
    print("Price gap task generation complete")
    print(f"latest_complete_eod_date: {latest_complete_eod_date}")
    print(f"output_path: {output_path}")
    print(f"task_count: {len(tasks)}")

    if tasks.empty:
        print("No price gaps found.")
        return

    print("\nReason counts:")
    print(tasks["reason"].value_counts(dropna=False).to_string())

    print("\nRequest window summary:")
    summary = tasks.agg(
        {
            "request_start_date": ["min", "max"],
            "request_end_date": ["min", "max"],
        }
    )
    print(summary.to_string())

    print("\nSample tasks:")
    print(tasks.head(20).to_string(index=False))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate Tiingo price gap tasks for windowed incremental updates."
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
        "--dry-run",
        action="store_true",
        help="Print summary without writing output Parquet.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    config = load_price_gap_task_config(
        config_path=args.config,
        bootstrap_task_list_path=args.bootstrap_task_list,
        dwd_price_root=args.dwd_price_root,
        output_path=args.output,
    )

    bootstrap_tasks = load_bootstrap_task_list(config.bootstrap_task_list_path)
    latest_dwd_dates = load_latest_dwd_dates(config.dwd_price_root)

    tasks = build_price_gap_tasks(
        bootstrap_tasks=bootstrap_tasks,
        latest_dwd_dates=latest_dwd_dates,
        source=config.source,
        dataset_name=config.dataset_name,
        bootstrap_anchor_date=config.bootstrap_anchor_date,
        latest_complete_eod_date=config.latest_complete_eod_date,
    )

    print_summary(
        tasks=tasks,
        latest_complete_eod_date=config.latest_complete_eod_date,
        output_path=config.output_path,
    )

    if args.dry_run:
        print("\ndry_run: true, no file written")
        return

    save_tasks(tasks, config.output_path)
    print("\nSaved price gap task list.")


if __name__ == "__main__":
    main()