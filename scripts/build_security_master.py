from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from dotenv import load_dotenv
from google.cloud import storage


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"
CONFIG_PATH = PROJECT_ROOT / "configs" / "security_master.yml"


COLUMN_ALIASES = {
    "ticker": ["ticker", "symbol"],
    "exchange": ["exchange"],
    "asset_type": ["assetType", "asset_type", "assettype"],
    "price_currency": ["priceCurrency", "price_currency", "pricecurrency", "currency"],
    "start_date": ["startDate", "start_date", "startdate"],
    "end_date": ["endDate", "end_date", "enddate"],
    "company_name": [
        "companyName",
        "company_name",
        "name",
        "description",
        "securityName",
        "security_name",
    ],
}


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


def normalize_col_name(col: str) -> str:
    """Normalize a raw column name for robust matching."""
    return col.strip().replace("_", "").replace("-", "").lower()


def find_column(df: pd.DataFrame, aliases: list[str], required: bool = True) -> str | None:
    """Find a column by alias list, allowing case/underscore differences."""
    normalized_to_original = {
        normalize_col_name(col): col
        for col in df.columns
    }

    for alias in aliases:
        normalized_alias = normalize_col_name(alias)
        if normalized_alias in normalized_to_original:
            return normalized_to_original[normalized_alias]

    if required:
        raise KeyError(
            f"Could not find required column. Tried aliases={aliases}. "
            f"Available columns={list(df.columns)}"
        )

    return None


def get_source_path(config: dict[str, Any]) -> Path:
    """Resolve local supported tickers CSV path."""
    local_ods_root = config["security_master"]["local_ods_root"]
    return PROJECT_ROOT / local_ods_root / "supported_tickers.csv"


def get_output_path(config: dict[str, Any]) -> Path:
    """Resolve local dim_security parquet path."""
    local_dwd_root = config["security_master"]["local_dwd_root"]
    return PROJECT_ROOT / local_dwd_root / "dim_security.parquet"


def clean_ticker(value: object) -> str:
    """Standardize ticker string."""
    if pd.isna(value):
        return ""
    return str(value).strip().upper()


def parse_date_series(series: pd.Series) -> pd.Series:
    """Parse dates to pandas datetime date values."""
    parsed = pd.to_datetime(series, errors="coerce", utc=False)
    return parsed.dt.date


def build_dim_security(raw_df: pd.DataFrame,
                       active_end_date_grace_days: int = 7) -> pd.DataFrame:

    """Build standardized dim_security from Tiingo supported tickers."""
    ticker_col = find_column(raw_df, COLUMN_ALIASES["ticker"], required=True)
    exchange_col = find_column(raw_df, COLUMN_ALIASES["exchange"], required=True)
    asset_type_col = find_column(raw_df, COLUMN_ALIASES["asset_type"], required=True)
    currency_col = find_column(raw_df, COLUMN_ALIASES["price_currency"], required=True)
    start_date_col = find_column(raw_df, COLUMN_ALIASES["start_date"], required=True)
    end_date_col = find_column(raw_df, COLUMN_ALIASES["end_date"], required=False)
    company_name_col = find_column(raw_df, COLUMN_ALIASES["company_name"], required=False)

    now_utc = datetime.now(timezone.utc)

    output = pd.DataFrame(index=raw_df.index)

    output["source"] = "tiingo"
    output["source_raw_symbol"] = raw_df[ticker_col].astype("string")
    output["source_ticker"] = raw_df[ticker_col].map(clean_ticker)
    output["ticker"] = output["source_ticker"]

    output["security_id"] = "tiingo:" + output["ticker"]

    output["exchange"] = raw_df[exchange_col].astype("string").str.strip()
    output["asset_type"] = raw_df[asset_type_col].astype("string").str.strip()
    output["price_currency"] = raw_df[currency_col].astype("string").str.strip()

    output["start_date"] = parse_date_series(raw_df[start_date_col])

    if end_date_col is not None:
        output["end_date"] = parse_date_series(raw_df[end_date_col])
    else:
        output["end_date"] = pd.NaT

    if company_name_col is not None:
        output["company_name"] = raw_df[company_name_col].astype("string").str.strip()
    else:
        output["company_name"] = pd.NA

    valid_end_dates = pd.to_datetime(output["end_date"], errors="coerce").dropna()

    if valid_end_dates.empty:
        active_reference_date = now_utc.date()
    else:
        active_reference_date = valid_end_dates.max().date()

    active_cutoff_date = active_reference_date - timedelta(
        days=active_end_date_grace_days
    )

    output["is_active"] = (
        output["end_date"].isna()
        | (output["end_date"] >= active_cutoff_date)
    )

    print(
        "Active security logic: "
        f"reference_date={active_reference_date}, "
        f"grace_days={active_end_date_grace_days}, "
        f"cutoff_date={active_cutoff_date}"
    )

    output["loaded_at"] = now_utc.isoformat()

    # Drop unusable rows.
    before = len(output)
    output = output[output["ticker"].notna() & (output["ticker"] != "")].copy()
    after = len(output)

    if before != after:
        print(f"Dropped empty ticker rows: {before - after:,}")

    # Keep one record per security_id. If Tiingo contains duplicates, keep first.
    before_dedup = len(output)
    output = output.drop_duplicates(subset=["security_id"], keep="first").copy()
    after_dedup = len(output)

    if before_dedup != after_dedup:
        print(f"Dropped duplicate security_id rows: {before_dedup - after_dedup:,}")

    ordered_columns = [
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
        "source_raw_symbol",
        "loaded_at",
    ]

    return output[ordered_columns]


def upload_to_gcs(
    local_path: Path,
    bucket_name: str,
    destination_blob_name: str,
    dry_run: bool = False,
) -> None:
    """Upload dim_security parquet to GCS."""
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


def print_summary(dim_security: pd.DataFrame, output_path: Path) -> None:
    """Print useful summary for inspection."""
    print("\nSecurity master summary")
    print("-----------------------")
    print(f"Output path: {output_path}")
    print(f"Rows: {len(dim_security):,}")
    print(f"Columns: {list(dim_security.columns)}")

    print("\nAsset type counts:")
    print(dim_security["asset_type"].value_counts(dropna=False).head(20).to_string())

    print("\nExchange counts:")
    print(dim_security["exchange"].value_counts(dropna=False).head(20).to_string())

    print("\nCurrency counts:")
    print(dim_security["price_currency"].value_counts(dropna=False).head(20).to_string())

    print("\nActive counts:")
    print(dim_security["is_active"].value_counts(dropna=False).to_string())

    print("\nSample rows:")
    print(dim_security.head(10).to_string(index=False))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build standardized dim_security from Tiingo supported tickers."
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

    config = load_config()
    source_path = get_source_path(config)
    output_path = get_output_path(config)

    if not source_path.exists():
        raise FileNotFoundError(
            f"Supported tickers file not found: {source_path}. "
            "Run `python -m scripts.ingest_tiingo_supported_tickers` first."
        )

    raw_df = pd.read_csv(source_path)
    print(f"Loaded supported tickers: {source_path}")
    print(f"Raw rows: {len(raw_df):,}")
    print(f"Raw columns: {list(raw_df.columns)}")

    active_end_date_grace_days = int(
    config.get("security_master", {}).get(
        "active_end_date_grace_days",
        7,
    )
)

    dim_security = build_dim_security(
        raw_df,
        active_end_date_grace_days=active_end_date_grace_days,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    dim_security.to_parquet(output_path, index=False)

    print_summary(dim_security, output_path)

    if not args.no_gcs:
        bucket_name = os.getenv("GCS_BUCKET", "")
        destination_blob_name = "dwd/security_master/dim_security.parquet"

        upload_to_gcs(
            local_path=output_path,
            bucket_name=bucket_name,
            destination_blob_name=destination_blob_name,
            dry_run=args.dry_run_gcs,
        )


if __name__ == "__main__":
    main()