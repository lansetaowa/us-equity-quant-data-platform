from __future__ import annotations

import argparse
import json
import os
import shutil
from datetime import UTC, datetime
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


def to_date(value: Any) -> Any:
    """Convert value to Python date."""
    return pd.Timestamp(value).date()


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
    """Load and validate task list."""
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

    return df.reset_index(drop=True)


def validate_single_request_window(df: pd.DataFrame) -> tuple[str, str, Any, Any]:
    """Validate one source/dataset/request window."""
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


def fetch_metadata_statuses(
    conn: psycopg.Connection,
    task_df: pd.DataFrame,
    source: str,
    dataset_name: str,
    requested_start_date: Any,
    requested_end_date: Any,
) -> pd.DataFrame:
    """Fetch symbol ingestion statuses for task-list tickers."""
    tickers = sorted(task_df["ticker"].unique().tolist())

    sql = """
        SELECT
            ticker,
            security_id,
            status,
            attempt_count,
            last_successful_date,
            last_error_message
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
        "ticker",
        "metadata_security_id",
        "metadata_status",
        "metadata_attempt_count",
        "metadata_last_successful_date",
        "metadata_last_error_message",
    ]

    return pd.DataFrame(rows, columns=columns)


def select_successful_scope(
    task_df: pd.DataFrame,
    status_df: pd.DataFrame,
) -> pd.DataFrame:
    """Select task-list rows whose metadata status is success."""
    if status_df.empty:
        raise ValueError(
            "No metadata rows found. Run scripts.init_backfill_metadata first."
        )

    status_df = status_df.copy()
    status_df["ticker"] = status_df["ticker"].astype(str).str.upper()

    merged = task_df.merge(status_df, on="ticker", how="left")

    missing_status = merged["metadata_status"].isna()
    if missing_status.any():
        missing = merged.loc[missing_status, "ticker"].tolist()
        raise ValueError(
            "Some task-list tickers are missing metadata rows. "
            f"Examples: {missing[:20]}"
        )

    successful = merged[merged["metadata_status"] == "success"].copy()
    successful = successful.sort_values("ticker").reset_index(drop=True)

    if successful.empty:
        raise ValueError(
            "No successful tickers found for transform. "
            "Run scripts.run_tiingo_backfill first."
        )

    return successful


def get_raw_output_path(ods_root: Path, ticker: str) -> Path:
    """Return local raw ODS file path for one ticker."""
    return ods_root / f"symbol={ticker}" / f"{ticker.lower()}_prices.json"


def read_raw_json(path: Path) -> list[dict[str, Any]]:
    """Read raw Tiingo JSON file."""
    if not path.exists():
        raise FileNotFoundError(f"Raw ODS file not found: {path}")

    with path.open("r", encoding="utf-8") as file:
        data = json.load(file)

    if not isinstance(data, list):
        raise ValueError(f"Raw Tiingo file must contain a list: {path}")

    return data


def get_numeric_series(
    df: pd.DataFrame,
    aliases: list[str],
    default: Any,
) -> pd.Series:
    """Get numeric series by alias list, or default series if absent."""
    for alias in aliases:
        if alias in df.columns:
            return pd.to_numeric(df[alias], errors="coerce")

    return pd.to_numeric(
        pd.Series(default, index=df.index),
        errors="coerce",
    )


def standardize_raw_prices(
    raw_rows: list[dict[str, Any]],
    task_row: pd.Series,
    load_id: str,
    loaded_at: str,
) -> pd.DataFrame:
    """Standardize one ticker's raw Tiingo price rows to DWD schema."""
    if not raw_rows:
        return pd.DataFrame()

    raw_df = pd.DataFrame(raw_rows)

    if "date" not in raw_df.columns:
        raise ValueError(f"Raw rows for {task_row['ticker']} do not contain date.")

    output = pd.DataFrame(index=raw_df.index)

    output["security_id"] = task_row["security_id"]
    output["ticker"] = task_row["ticker"]
    output["date"] = pd.to_datetime(
        raw_df["date"],
        errors="coerce",
        utc=True,
    ).dt.date

    output["open"] = get_numeric_series(raw_df, ["open"], pd.NA)
    output["high"] = get_numeric_series(raw_df, ["high"], pd.NA)
    output["low"] = get_numeric_series(raw_df, ["low"], pd.NA)
    output["close"] = get_numeric_series(raw_df, ["close"], pd.NA)
    output["volume"] = get_numeric_series(raw_df, ["volume"], 0).astype("Int64")

    output["adj_open"] = get_numeric_series(
        raw_df,
        ["adjOpen", "adj_open"],
        pd.NA,
    ).fillna(output["open"])
    output["adj_high"] = get_numeric_series(
        raw_df,
        ["adjHigh", "adj_high"],
        pd.NA,
    ).fillna(output["high"])
    output["adj_low"] = get_numeric_series(
        raw_df,
        ["adjLow", "adj_low"],
        pd.NA,
    ).fillna(output["low"])
    output["adj_close"] = get_numeric_series(
        raw_df,
        ["adjClose", "adj_close"],
        pd.NA,
    ).fillna(output["close"])
    output["adj_volume"] = get_numeric_series(
        raw_df,
        ["adjVolume", "adj_volume"],
        pd.NA,
    ).fillna(output["volume"]).astype("Int64")

    output["div_cash"] = get_numeric_series(
        raw_df,
        ["divCash", "div_cash"],
        0.0,
    ).fillna(0.0)
    output["split_factor"] = get_numeric_series(
        raw_df,
        ["splitFactor", "split_factor"],
        1.0,
    ).fillna(1.0)

    output["source"] = "tiingo"
    output["load_id"] = load_id
    output["loaded_at"] = loaded_at

    ordered_columns = [
        "security_id",
        "ticker",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "adj_open",
        "adj_high",
        "adj_low",
        "adj_close",
        "adj_volume",
        "div_cash",
        "split_factor",
        "source",
        "load_id",
        "loaded_at",
    ]

    return output[ordered_columns]


def build_dwd_dataframe(scope_df: pd.DataFrame, ods_root: Path) -> pd.DataFrame:
    """Read raw ODS files and build combined DWD DataFrame."""
    load_id = f"backfill_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}"
    loaded_at = datetime.now(UTC).isoformat()

    frames = []

    for _, task_row in scope_df.iterrows():
        ticker = str(task_row["ticker"]).upper()
        raw_path = get_raw_output_path(ods_root, ticker)

        raw_rows = read_raw_json(raw_path)
        ticker_df = standardize_raw_prices(
            raw_rows=raw_rows,
            task_row=task_row,
            load_id=load_id,
            loaded_at=loaded_at,
        )

        if not ticker_df.empty:
            frames.append(ticker_df)

    if not frames:
        raise ValueError("No DWD rows were generated from ODS files.")

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.sort_values(["ticker", "date"]).reset_index(drop=True)

    return combined


def validate_dwd_dataframe(df: pd.DataFrame) -> None:
    """Validate combined DWD DataFrame before writing."""
    if df.empty:
        raise ValueError("DWD DataFrame is empty.")

    required_non_null = [
        "security_id",
        "ticker",
        "date",
        "adj_close",
        "source",
        "load_id",
        "loaded_at",
    ]

    null_columns = [col for col in required_non_null if df[col].isna().any()]
    if null_columns:
        raise ValueError(f"DWD DataFrame has nulls in columns: {null_columns}")

    duplicate_count = int(df.duplicated(subset=["security_id", "date"]).sum())
    if duplicate_count > 0:
        duplicates = df[df.duplicated(subset=["security_id", "date"], keep=False)]
        raise ValueError(
            "DWD DataFrame has duplicate security_id/date rows. "
            f"Duplicate count={duplicate_count}. "
            f"Examples={duplicates[['security_id', 'ticker', 'date']].head(10).to_dict('records')}"
        )

    invalid_adj_close = df[df["adj_close"] <= 0]
    if not invalid_adj_close.empty:
        raise ValueError(
            "DWD DataFrame has non-positive adj_close values. "
            f"Examples={invalid_adj_close[['ticker', 'date', 'adj_close']].head(10).to_dict('records')}"
        )

    invalid_volume = df[df["volume"].fillna(0) < 0]
    if not invalid_volume.empty:
        raise ValueError(
            "DWD DataFrame has negative volume values. "
            f"Examples={invalid_volume[['ticker', 'date', 'volume']].head(10).to_dict('records')}"
        )


def print_dwd_summary(df: pd.DataFrame) -> None:
    """Print DWD summary."""
    print("\nDWD transform summary")
    print("---------------------")
    print(f"Rows: {len(df):,}")
    print(f"Tickers: {df['ticker'].nunique():,}")
    print(f"Min date: {df['date'].min()}")
    print(f"Max date: {df['date'].max()}")

    print("\nRows by ticker:")
    print(
        df.groupby("ticker", as_index=False)
        .size()
        .sort_values("ticker")
        .head(50)
        .to_string(index=False)
    )


def clean_directory(path: Path) -> None:
    """Remove a directory if it exists."""
    if path.exists():
        shutil.rmtree(path)


def write_partitioned_parquet(df: pd.DataFrame, output_root: Path) -> None:
    """Write DWD DataFrame to year/month partitioned Parquet."""
    output_root.mkdir(parents=True, exist_ok=True)

    working = df.copy()
    date_values = pd.to_datetime(working["date"])

    working["year"] = date_values.dt.year
    working["month"] = date_values.dt.month

    for (year, month), part_df in working.groupby(["year", "month"]):
        partition_dir = output_root / f"year={int(year)}" / f"month={int(month):02d}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        output_path = partition_dir / "part-000.parquet"

        write_df = part_df.drop(columns=["year", "month"]).sort_values(
            ["ticker", "date"]
        )
        write_df.to_parquet(output_path, index=False)

    partition_count = len(list(output_root.glob("year=*/month=*/part-000.parquet")))
    print(f"\nWrote DWD partitions: {partition_count:,}")
    print(f"Output root: {output_root}")


def replace_final_output(tmp_root: Path, final_root: Path) -> None:
    """Replace final DWD output with validated temporary output."""
    if not tmp_root.exists():
        raise FileNotFoundError(f"Temp DWD root not found: {tmp_root}")

    archive_parent = PROJECT_ROOT / "data" / "_archive" / "dwd"
    archive_parent.mkdir(parents=True, exist_ok=True)

    if final_root.exists():
        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        archive_path = archive_parent / f"equity_price_daily_replaced_{timestamp}"
        shutil.move(str(final_root), str(archive_path))
        print(f"Archived previous final DWD root to: {archive_path}")

    final_root.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(tmp_root), str(final_root))
    print(f"Promoted temp DWD root to final DWD root: {final_root}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Transform successful Tiingo backfill ODS files to DWD Parquet."
    )
    parser.add_argument(
        "--task-list",
        default="pilot_500",
        help="Task list name from configs/backfill.yml.",
    )
    parser.add_argument(
        "--tickers",
        type=str,
        default=None,
        help="Optional comma-separated tickers to transform.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Select successful scope but do not write DWD output.",
    )
    parser.add_argument(
        "--no-replace",
        action="store_true",
        help="Write temp DWD output but do not replace final DWD root.",
    )
    args = parser.parse_args()

    load_dotenv(ENV_PATH)

    dsn = os.getenv("POSTGRES_DSN")
    if not dsn:
        raise RuntimeError("POSTGRES_DSN is missing. Check your .env file.")

    config = load_config()

    target_tickers = parse_tickers(args.tickers)

    task_df = load_task_list(
        config=config,
        task_list_name=args.task_list,
        target_tickers=target_tickers,
    )

    source, dataset_name, requested_start_date, requested_end_date = (
        validate_single_request_window(task_df)
    )

    ods_root = resolve_project_path(config["local_paths"]["ods_root"])
    dwd_tmp_root = resolve_project_path(config["local_paths"]["dwd_tmp_root"])
    dwd_final_root = resolve_project_path(config["local_paths"]["dwd_final_root"])

    clean_temp_before_run = bool(
        config["dwd_transform"].get("clean_temp_before_run", True)
    )
    replace_after_validation = bool(
        config["dwd_transform"].get("replace_target_after_validation", True)
    )

    with psycopg.connect(dsn) as conn:
        status_df = fetch_metadata_statuses(
            conn=conn,
            task_df=task_df,
            source=source,
            dataset_name=dataset_name,
            requested_start_date=requested_start_date,
            requested_end_date=requested_end_date,
        )

    scope_df = select_successful_scope(task_df, status_df)

    print("\nODS to DWD transform scope")
    print("--------------------------")
    print(f"Task list: {args.task_list}")
    print(f"Successful tickers in scope: {len(scope_df):,}")
    print(f"ODS root: {ods_root}")
    print(f"Temp DWD root: {dwd_tmp_root}")
    print(f"Final DWD root: {dwd_final_root}")

    print("\nScope tickers:")
    print(scope_df["ticker"].head(50).to_list())

    if args.dry_run:
        print("\n[DRY RUN] No DWD files were written.")
        return

    dwd_df = build_dwd_dataframe(scope_df=scope_df, ods_root=ods_root)
    validate_dwd_dataframe(dwd_df)
    print_dwd_summary(dwd_df)

    if clean_temp_before_run:
        clean_directory(dwd_tmp_root)

    write_partitioned_parquet(dwd_df, dwd_tmp_root)

    if replace_after_validation and not args.no_replace:
        replace_final_output(dwd_tmp_root, dwd_final_root)
    else:
        print("\nFinal DWD root was not replaced.")
        print(f"Validated temp DWD output remains at: {dwd_tmp_root}")

    print("\nODS to DWD transform completed successfully.")


if __name__ == "__main__":
    main()