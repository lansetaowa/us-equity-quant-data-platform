from __future__ import annotations

import argparse
import hashlib
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from dotenv import load_dotenv
from google.cloud import storage

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"
CONFIG_PATH = PROJECT_ROOT / "configs" / "security_master.yml"
BACKFILL_CONFIG_PATH = PROJECT_ROOT / "configs" / "backfill.yml"

DEFAULT_CANDIDATE_POOL_PATH = (
    PROJECT_ROOT / "data" / "dwd" / "security_master" / "candidate_security_pool.parquet"
)
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "data" / "dwd" / "security_master"

KNOWN_PILOT_TICKERS = [
    "AAPL",
    "MSFT",
    "NVDA",
    "AMZN",
    "META",
    "GOOGL",
    "TSLA",
    "SPY",
    "QQQ",
    "IWM",
]


def load_config(config_path: Path) -> dict[str, Any]:
    """Load a YAML config file."""
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    raw_text = config_path.read_text(encoding="utf-8")
    expanded_text = os.path.expandvars(raw_text)
    config = yaml.safe_load(expanded_text) or {}

    if not isinstance(config, dict):
        raise ValueError(f"{config_path.name} must contain a YAML mapping.")

    return config


def stable_hash(value: str) -> str:
    """Return stable hash for deterministic sampling."""
    return hashlib.md5(value.encode("utf-8")).hexdigest()


def parse_config_date(value: Any, field_name: str) -> Any:
    """Parse a config date value to a Python date."""
    if value is None:
        raise ValueError(f"Missing required date config: {field_name}")

    try:
        return pd.Timestamp(value).date()
    except Exception as exc:
        raise ValueError(
            f"Invalid date for {field_name}: {value!r}. "
            "Expected YYYY-MM-DD."
        ) from exc


def get_task_list_settings(
    config: dict[str, Any],
    task_list_name: str,
) -> tuple[int | None, int]:
    """Return limit and priority for a named task list."""
    backfill_config = config.get("backfill_planning", {})

    if task_list_name == "pilot_500":
        task_config = backfill_config.get("pilot_task_list", {})
        limit = task_config.get("limit", 500)
        priority = 1
    elif task_list_name == "bootstrap_candidates":
        task_config = backfill_config.get("bootstrap_task_list", {})
        limit = task_config.get("limit", None)
        priority = 2
    else:
        raise ValueError(
            f"Unsupported task_list_name={task_list_name}. "
            "Expected one of: pilot_500, bootstrap_candidates."
        )

    if limit is not None:
        limit = int(limit)

    return limit, priority


def get_requested_window(
    security_config: dict[str, Any],
    backfill_config: dict[str, Any],
    task_list_name: str,
    requested_end_date_override: str | None = None,
) -> tuple[Any, Any]:
    """
    Return requested start/end dates for a task list.

    For bootstrap_candidates, the formal bootstrap window must be frozen in
    configs/backfill.yml:

        bootstrap:
          requested_start_date: "2019-01-01"
          requested_end_date: "2026-06-10"

    This prevents task-list dates from changing silently whenever the task list
    is regenerated.
    """
    if task_list_name == "bootstrap_candidates":
        bootstrap_cfg = backfill_config.get("bootstrap", {})

        requested_start_date = parse_config_date(
            bootstrap_cfg.get("requested_start_date"),
            "bootstrap.requested_start_date",
        )

        if requested_end_date_override is not None:
            requested_end_date = parse_config_date(
                requested_end_date_override,
                "--end-date",
            )
        else:
            requested_end_date = parse_config_date(
                bootstrap_cfg.get("requested_end_date"),
                "bootstrap.requested_end_date",
            )

    elif task_list_name == "pilot_500":
        requested_start_date = parse_config_date(
            security_config.get("dates", {}).get("price_backfill_start_date"),
            "dates.price_backfill_start_date",
        )

        if requested_end_date_override is not None:
            requested_end_date = parse_config_date(
                requested_end_date_override,
                "--end-date",
            )
        else:
            # Legacy behavior for the already-completed Week 7 pilot.
            # Do not use this path for the formal Week 8 bootstrap.
            requested_end_date = datetime.now(UTC).date()

    else:
        raise ValueError(
            f"Unsupported task_list_name={task_list_name}. "
            "Expected one of: pilot_500, bootstrap_candidates."
        )

    if requested_end_date < requested_start_date:
        raise ValueError(
            "requested_end_date must be greater than or equal to "
            "requested_start_date. Got "
            f"{requested_start_date} -> {requested_end_date}."
        )

    return requested_start_date, requested_end_date


def validate_candidate_pool(candidate_pool: pd.DataFrame) -> None:
    """Validate required candidate pool columns."""
    required_columns = [
        "security_id",
        "ticker",
        "source_ticker",
        "exchange",
        "asset_type",
        "price_currency",
        "start_date",
        "end_date",
        "is_active",
        "candidate_pool_name",
    ]

    missing_columns = [
        col for col in required_columns if col not in candidate_pool.columns
    ]

    if missing_columns:
        raise KeyError(f"candidate_security_pool missing columns: {missing_columns}")

    if candidate_pool["security_id"].isna().any():
        raise ValueError("candidate_security_pool contains null security_id.")

    if candidate_pool["ticker"].isna().any():
        raise ValueError("candidate_security_pool contains null ticker.")


def select_task_universe(
    candidate_pool: pd.DataFrame,
    task_list_name: str,
    limit: int | None,
) -> pd.DataFrame:
    """
    Select rows for a task list.

    bootstrap_candidates:
        all candidates, sorted by ticker.

    pilot_500:
        deterministic sample, not alphabetical, with known tickers forced in
        when present in candidate_pool.
    """
    df = candidate_pool.copy()
    df["ticker"] = df["ticker"].astype("string").str.strip().str.upper()
    df["security_id"] = df["security_id"].astype("string").str.strip()

    if task_list_name == "pilot_500":
        if limit is None:
            raise ValueError("pilot_500 requires a non-null limit.")

        known_df = df[df["ticker"].isin(KNOWN_PILOT_TICKERS)].copy()
        remaining_df = df[~df["ticker"].isin(KNOWN_PILOT_TICKERS)].copy()

        remaining_df["pilot_sort_key"] = remaining_df["ticker"].map(stable_hash)
        remaining_df = remaining_df.sort_values("pilot_sort_key")

        fill_count = max(limit - len(known_df), 0)

        selected = pd.concat(
            [known_df, remaining_df.head(fill_count)],
            ignore_index=True,
        )

        selected = selected.drop_duplicates(subset=["security_id"], keep="first")
        selected = selected.head(limit).copy()
        selected = selected.sort_values(["ticker", "security_id"]).reset_index(
            drop=True
        )

        return selected

    selected = df.sort_values(["ticker", "security_id"]).reset_index(drop=True)

    if limit is not None:
        selected = selected.head(limit).copy()

    return selected


def build_backfill_task_list(
    candidate_pool: pd.DataFrame,
    config: dict[str, Any] | None = None,
    task_list_name: str | None = None,
    limit_override: int | None = None,
    requested_end_date: str | None = None,
    security_config: dict[str, Any] | None = None,
    backfill_config: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """
    Build a backfill task list from candidate pool.

    Backward compatible with old tests that call:
        build_backfill_task_list(candidate_pool=..., config=..., task_list_name=...)

    New production code should call with:
        security_config=...
        backfill_config=...
    """
    validate_candidate_pool(candidate_pool)

    if security_config is None:
        if config is None:
            raise ValueError(
                "Either security_config or backward-compatible config must be provided."
            )
        security_config = config

    if backfill_config is None:
        backfill_config = {}

    if task_list_name is None:
        raise ValueError("task_list_name is required.")

    source = str(security_config.get("source", "tiingo"))
    dataset = str(security_config["datasets"]["equity_price_daily"])

    requested_start_date, end_date = get_requested_window(
        security_config=security_config,
        backfill_config=backfill_config,
        task_list_name=task_list_name,
        requested_end_date_override=requested_end_date,
    )

    configured_limit, priority = get_task_list_settings(
        security_config,
        task_list_name,
    )
    limit = limit_override if limit_override is not None else configured_limit

    df = select_task_universe(
        candidate_pool=candidate_pool,
        task_list_name=task_list_name,
        limit=limit,
    )

    created_at = datetime.now(UTC).isoformat()

    output = pd.DataFrame(index=df.index)

    output["task_list_name"] = task_list_name
    output["source"] = source
    output["dataset_name"] = dataset
    output["security_id"] = df["security_id"]
    output["ticker"] = df["ticker"]
    output["requested_start_date"] = requested_start_date
    output["requested_end_date"] = end_date
    output["priority"] = priority
    output["status"] = "pending"
    output["created_at"] = created_at

    output["task_id"] = (
        output["task_list_name"].astype(str)
        + ":"
        + output["source"].astype(str)
        + ":"
        + output["dataset_name"].astype(str)
        + ":"
        + output["ticker"].astype(str)
        + ":"
        + output["requested_start_date"].astype(str)
        + ":"
        + output["requested_end_date"].astype(str)
    )

    ordered_columns = [
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

    output = output[ordered_columns].drop_duplicates(subset=["task_id"]).copy()

    required_non_null_columns = [
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

    null_columns = [
        col for col in required_non_null_columns
        if output[col].isna().any()
    ]

    if null_columns:
        raise ValueError(
            "Backfill task list contains null values in required columns: "
            f"{null_columns}"
        )

    if output["task_id"].str.contains("nan", case=False, na=False).any():
        raise ValueError("Backfill task list contains invalid task_id with 'nan'.")

    if output.empty:
        raise ValueError("Generated backfill task list is empty.")

    return output


def get_output_path(task_list_name: str) -> Path:
    """Return local output path for a task list."""
    return DEFAULT_OUTPUT_ROOT / f"backfill_task_list_{task_list_name}.parquet"


def print_summary(task_list: pd.DataFrame, output_path: Path) -> None:
    """Print task list summary."""
    print("\nBackfill task list summary")
    print("--------------------------")
    print(f"Output path: {output_path}")
    print(f"Rows: {len(task_list):,}")
    print(f"Task list name: {task_list['task_list_name'].iloc[0]}")
    print(f"Requested start date: {task_list['requested_start_date'].iloc[0]}")
    print(f"Requested end date: {task_list['requested_end_date'].iloc[0]}")

    print("\nStatus counts:")
    print(task_list["status"].value_counts(dropna=False).to_string())

    print("\nSample tasks:")
    print(task_list.head(20).to_string(index=False))


def upload_to_gcs(
    local_path: Path,
    bucket_name: str,
    destination_blob_name: str,
    dry_run: bool = False,
) -> None:
    """Upload a task list parquet to GCS."""
    if not bucket_name:
        raise ValueError("GCS_BUCKET is missing. Set it in .env.")

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


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate paid-month Tiingo backfill task lists."
    )
    parser.add_argument(
        "--task-list",
        choices=["pilot_500", "bootstrap_candidates"],
        required=True,
        help="Task list to generate.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional override for task list size.",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help=(
            "Optional requested end date in YYYY-MM-DD format. "
            "For bootstrap_candidates, this deliberately overrides "
            "configs/backfill.yml bootstrap.requested_end_date."
        ),
    )
    parser.add_argument(
        "--candidate-pool-path",
        type=str,
        default=str(DEFAULT_CANDIDATE_POOL_PATH),
        help="Path to candidate_security_pool parquet.",
    )
    parser.add_argument(
        "--no-gcs",
        action="store_true",
        help="Skip GCS upload.",
    )
    parser.add_argument(
        "--dry-run-gcs",
        action="store_true",
        help="Print planned GCS upload without uploading.",
    )
    args = parser.parse_args()

    load_dotenv(ENV_PATH)

    candidate_pool_path = Path(args.candidate_pool_path)

    if not candidate_pool_path.exists():
        raise FileNotFoundError(
            f"Candidate pool not found: {candidate_pool_path}. "
            "Run `python -m scripts.build_candidate_pool` first."
        )

    security_config = load_config(CONFIG_PATH)
    backfill_config = load_config(BACKFILL_CONFIG_PATH)
    candidate_pool = pd.read_parquet(candidate_pool_path)

    task_list = build_backfill_task_list(
        candidate_pool=candidate_pool,
        security_config=security_config,
        backfill_config=backfill_config,
        task_list_name=args.task_list,
        limit_override=args.limit,
        requested_end_date=args.end_date,
    )

    output_path = get_output_path(args.task_list)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    task_list.to_parquet(output_path, index=False)

    print_summary(task_list, output_path)

    if not args.no_gcs:
        bucket_name = os.getenv("GCS_BUCKET", "")
        destination_blob_name = (
            f"dwd/security_master/backfill_task_list_{args.task_list}.parquet"
        )

        upload_to_gcs(
            local_path=output_path,
            bucket_name=bucket_name,
            destination_blob_name=destination_blob_name,
            dry_run=args.dry_run_gcs,
        )


if __name__ == "__main__":
    main()