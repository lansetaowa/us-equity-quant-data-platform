from __future__ import annotations

import argparse
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from dotenv import load_dotenv
from google.cloud import storage


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"
CONFIG_PATH = PROJECT_ROOT / "configs" / "security_master.yml"

DEFAULT_DIM_SECURITY_PATH = (
    PROJECT_ROOT / "data" / "dwd" / "security_master" / "dim_security.parquet"
)
DEFAULT_OUTPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "dwd"
    / "security_master"
    / "candidate_security_pool.parquet"
)

GCS_DESTINATION = "dwd/security_master/candidate_security_pool.parquet"


def load_config(config_path: Path = CONFIG_PATH) -> dict[str, Any]:
    """Load security master config."""
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    raw_text = config_path.read_text(encoding="utf-8")
    expanded_text = os.path.expandvars(raw_text)
    config = yaml.safe_load(expanded_text)

    if not isinstance(config, dict):
        raise ValueError("security_master.yml must contain a YAML mapping.")

    return config


def require_config_list(config: dict[str, Any], key_path: list[str]) -> list[str]:
    """Read a required list value from nested config."""
    current: Any = config

    for key in key_path:
        current = current[key]

    if not isinstance(current, list):
        raise ValueError(f"Config value {'.'.join(key_path)} must be a list.")

    return [str(x).strip() for x in current]


def normalize_string_series(series: pd.Series) -> pd.Series:
    """Normalize string values for filtering."""
    return series.astype("string").str.strip()


def parse_date_column(series: pd.Series) -> pd.Series:
    """Parse date-like column to pandas datetime64."""
    return pd.to_datetime(series, errors="coerce")


def count_step(label: str, df: pd.DataFrame) -> None:
    """Print row count at a filtering step."""
    print(f"{label}: {len(df):,}")


def build_literal_regex(patterns: list[str]) -> re.Pattern[str] | None:
    """Build case-insensitive regex from literal text patterns."""
    clean_patterns = [p.strip() for p in patterns if str(p).strip()]

    if not clean_patterns:
        return None

    escaped = [re.escape(p) for p in clean_patterns]
    return re.compile("|".join(escaped), flags=re.IGNORECASE)


def build_raw_regex(patterns: list[str]) -> re.Pattern[str] | None:
    """Build case-insensitive regex from raw regex patterns."""
    clean_patterns = [p.strip() for p in patterns if str(p).strip()]

    if not clean_patterns:
        return None

    return re.compile("|".join(clean_patterns), flags=re.IGNORECASE)


def build_candidate_pool(
    dim_security: pd.DataFrame,
    config: dict[str, Any],
) -> pd.DataFrame:
    """
    Build candidate security pool from dim_security.

    This creates a broad backfill candidate pool, not the final dynamic universe.

    Current design choices:
    - Keep post-2020 IPOs if they overlap with the requested backfill window.
    - Require start_date and end_date to be present, based on manual inspection
      that null-date tickers are likely unusable or unavailable for this stage.
    - Exclude obvious non-common-stock ticker suffixes such as rights, warrants,
      and units.
    """
    df = dim_security.copy()

    requested_start_date = pd.Timestamp(config["dates"]["price_backfill_start_date"])
    requested_end_date = pd.Timestamp.today().normalize()

    asset_types = require_config_list(config, ["candidate_filters", "asset_types"])
    currencies = require_config_list(config, ["candidate_filters", "currencies"])
    exchanges = require_config_list(config, ["candidate_filters", "exchanges"])

    exclude_name_patterns = config["candidate_filters"].get(
        "exclude_name_patterns",
        [],
    )
    exclude_ticker_patterns = config["candidate_filters"].get(
        "exclude_ticker_patterns",
        [],
    )

    candidate_pool_name = str(
        config.get("candidate_pool", {}).get(
            "output_name",
            "us_common_stock_candidates",
        )
    )

    required_columns = [
        "security_id",
        "source",
        "source_ticker",
        "ticker",
        "exchange",
        "asset_type",
        "price_currency",
        "start_date",
        "end_date",
        "is_active",
        "company_name",
    ]

    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise KeyError(f"dim_security missing required columns: {missing_columns}")

    print("\nCandidate pool build summary")
    print("----------------------------")
    count_step("Raw dim_security rows", df)

    for col in [
        "security_id",
        "source",
        "source_ticker",
        "ticker",
        "exchange",
        "asset_type",
        "price_currency",
        "company_name",
    ]:
        df[col] = normalize_string_series(df[col])

    df["ticker"] = df["ticker"].str.upper()
    df["start_date"] = parse_date_column(df["start_date"])
    df["end_date"] = parse_date_column(df["end_date"])

    df = df[
        df["security_id"].notna()
        & df["ticker"].notna()
        & (df["ticker"] != "")
        & df["exchange"].notna()
        & df["asset_type"].notna()
        & df["price_currency"].notna()
    ].copy()
    count_step("After required-field filter", df)

    asset_types_norm = {x.upper() for x in asset_types}
    df = df[df["asset_type"].str.upper().isin(asset_types_norm)].copy()
    count_step(f"After asset_type filter {asset_types}", df)

    currencies_norm = {x.upper() for x in currencies}
    df = df[df["price_currency"].str.upper().isin(currencies_norm)].copy()
    count_step(f"After currency filter {currencies}", df)

    exchanges_norm = {x.upper() for x in exchanges}
    df = df[df["exchange"].str.upper().isin(exchanges_norm)].copy()
    count_step(f"After exchange filter {exchanges}", df)

    df = df[
        df["start_date"].notna()
        & (df["start_date"] <= requested_end_date)
    ].copy()
    count_step(
        f"After non-null start_date <= requested_end_date "
        f"{requested_end_date.date()}",
        df,
    )

    df = df[
        df["end_date"].notna()
        & (df["end_date"] >= requested_start_date)
    ].copy()
    count_step(
        f"After non-null end_date >= requested_start_date "
        f"{requested_start_date.date()}",
        df,
    )

    # Exclude obvious rights, warrants, and units using ticker suffix patterns.
    # Examples: UTF-R, STR-WS, VYX-W.
    ticker_exclusion_regex = build_raw_regex(exclude_ticker_patterns)

    if ticker_exclusion_regex is not None:
        ticker = df["ticker"].fillna("")
        exclude_mask = ticker.str.contains(ticker_exclusion_regex, regex=True)
        excluded_count = int(exclude_mask.sum())
        df = df[~exclude_mask].copy()
        print(
            "After ticker exclusion patterns "
            f"{exclude_ticker_patterns}: {len(df):,} "
            f"(excluded {excluded_count:,})"
        )

    # Tiingo supported_tickers usually does not include company names, so this
    # filter can legitimately remove zero rows for now. It is kept for future
    # enrichment when company_name is available.
    name_exclusion_regex = build_literal_regex(exclude_name_patterns)

    if name_exclusion_regex is not None:
        company_name = df["company_name"].fillna("")
        exclude_mask = company_name.str.contains(name_exclusion_regex, regex=True)
        excluded_count = int(exclude_mask.sum())
        df = df[~exclude_mask].copy()
        print(
            "After company_name exclusion patterns "
            f"{exclude_name_patterns}: {len(df):,} "
            f"(excluded {excluded_count:,})"
        )

    now_utc = datetime.now(timezone.utc).isoformat()

    df["candidate_pool_name"] = candidate_pool_name
    df["candidate_reason"] = (
        "asset_type_in_config;"
        "currency_in_config;"
        "major_us_exchange;"
        "overlaps_requested_backfill_window;"
        "ticker_pattern_not_excluded"
    )
    df["loaded_at"] = now_utc

    output_columns = [
        "security_id",
        "ticker",
        "source_ticker",
        "exchange",
        "asset_type",
        "price_currency",
        "start_date",
        "end_date",
        "is_active",
        "company_name",
        "candidate_pool_name",
        "candidate_reason",
        "loaded_at",
    ]

    output = df[output_columns].copy()
    output = output.drop_duplicates(subset=["security_id"], keep="first").copy()
    output = output.sort_values(["ticker", "security_id"]).reset_index(drop=True)

    if output.empty:
        raise ValueError("candidate pool is empty after filtering.")

    if output["security_id"].isna().any():
        raise ValueError("candidate pool contains null security_id.")

    if output["ticker"].isna().any() or (output["ticker"] == "").any():
        raise ValueError("candidate pool contains null or empty ticker.")

    return output


def print_summary(candidate_pool: pd.DataFrame, output_path: Path) -> None:
    """Print candidate pool summary."""
    print("\nCandidate pool final summary")
    print("----------------------------")
    print(f"Output path: {output_path}")
    print(f"Rows: {len(candidate_pool):,}")
    print(f"Columns: {list(candidate_pool.columns)}")

    print("\nExchange counts:")
    print(candidate_pool["exchange"].value_counts(dropna=False).head(30).to_string())

    print("\nAsset type counts:")
    print(candidate_pool["asset_type"].value_counts(dropna=False).head(20).to_string())

    print("\nCurrency counts:")
    print(
        candidate_pool["price_currency"]
        .value_counts(dropna=False)
        .head(20)
        .to_string()
    )

    print("\nActive counts:")
    print(candidate_pool["is_active"].value_counts(dropna=False).to_string())

    company_name_missing = candidate_pool["company_name"].isna().mean()
    print(f"\nCompany name missing ratio: {company_name_missing:.2%}")

    print("\nSample rows:")
    print(candidate_pool.head(20).to_string(index=False))


def upload_to_gcs(
    local_path: Path,
    bucket_name: str,
    destination_blob_name: str,
    dry_run: bool = False,
) -> None:
    """Upload candidate pool parquet to GCS."""
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
        description="Build US common stock candidate pool from dim_security."
    )
    parser.add_argument(
        "--input",
        type=str,
        default=str(DEFAULT_DIM_SECURITY_PATH),
        help="Path to dim_security parquet.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=str(DEFAULT_OUTPUT_PATH),
        help="Output path for candidate_security_pool parquet.",
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

    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        raise FileNotFoundError(
            f"dim_security parquet not found: {input_path}. "
            "Run `python -m scripts.build_security_master` first."
        )

    config = load_config()
    dim_security = pd.read_parquet(input_path)

    candidate_pool = build_candidate_pool(dim_security, config)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    candidate_pool.to_parquet(output_path, index=False)

    print_summary(candidate_pool, output_path)

    if not args.no_gcs:
        bucket_name = os.getenv("GCS_BUCKET", "")
        upload_to_gcs(
            local_path=output_path,
            bucket_name=bucket_name,
            destination_blob_name=GCS_DESTINATION,
            dry_run=args.dry_run_gcs,
        )


if __name__ == "__main__":
    main()