from __future__ import annotations

import argparse
import os
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

DEFAULT_INPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "ods"
    / "source=tiingo"
    / "dataset=supported_tickers"
    / "supported_tickers.csv"
)

DEFAULT_OUTPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "dwd"
    / "security_master"
    / "dim_security.parquet"
)

DEFAULT_REPORT_PATH = (
    PROJECT_ROOT
    / "reports"
    / "security_master"
    / "duplicate_ticker_resolution.csv"
)

DEFAULT_GCS_DESTINATION = "dwd/security_master/dim_security.parquet"

SOURCE = "tiingo"

US_MAJOR_EXCHANGE_PRIORITY = {
    "NASDAQ": 1,
    "NYSE": 2,
    "NYSE ARCA": 3,
    "NYSE MKT": 4,
    "AMEX": 5,
}

EXCHANGE_PRIORITY_FALLBACK = 999

ASSET_TYPE_PRIORITY = {
    "Stock": 1,
    "ETF": 2,
    "Mutual Fund": 3,
}


def load_config(config_path: Path = CONFIG_PATH) -> dict[str, Any]:
    """Load security master config."""
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    raw_text = config_path.read_text(encoding="utf-8")
    expanded_text = os.path.expandvars(raw_text)
    config = yaml.safe_load(expanded_text) or {}

    if not isinstance(config, dict):
        raise ValueError("security_master.yml must contain a YAML mapping.")

    return config


def normalize_string(series: pd.Series) -> pd.Series:
    """Normalize string-like pandas series."""
    return (
        series.astype("string")
        .str.strip()
        .replace({"": pd.NA, "nan": pd.NA, "NaN": pd.NA, "None": pd.NA})
    )


def get_required_column(df: pd.DataFrame, candidates: list[str]) -> str:
    """Return first matching column from candidates."""
    for col in candidates:
        if col in df.columns:
            return col

    raise KeyError(
        f"None of the expected columns exist: {candidates}. "
        f"Available columns: {list(df.columns)}"
    )


def normalize_supported_tickers(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Normalize Tiingo supported_tickers.csv into standard columns."""
    ticker_col = get_required_column(raw_df, ["ticker", "symbol"])
    exchange_col = get_required_column(raw_df, ["exchange"])
    asset_type_col = get_required_column(raw_df, ["assetType", "asset_type"])
    price_currency_col = get_required_column(
        raw_df,
        ["priceCurrency", "price_currency"],
    )
    start_date_col = get_required_column(raw_df, ["startDate", "start_date"])
    end_date_col = get_required_column(raw_df, ["endDate", "end_date"])

    df = pd.DataFrame(index=raw_df.index)

    df["source_ticker"] = normalize_string(raw_df[ticker_col]).str.upper()
    df["ticker"] = df["source_ticker"]
    df["exchange"] = normalize_string(raw_df[exchange_col]).str.upper()
    df["asset_type"] = normalize_string(raw_df[asset_type_col])
    df["price_currency"] = normalize_string(raw_df[price_currency_col]).str.upper()

    df["start_date"] = pd.to_datetime(raw_df[start_date_col], errors="coerce")
    df["end_date"] = pd.to_datetime(raw_df[end_date_col], errors="coerce")

    df["source_raw_symbol"] = df["source_ticker"]

    # Tiingo supported_tickers.csv does not provide company name.
    df["company_name"] = pd.NA

    before = len(df)

    df = df[df["ticker"].notna()].copy()
    df = df[df["ticker"] != ""].copy()

    after = len(df)
    if after < before:
        print(f"Dropped rows with missing ticker: {before - after:,}")

    return df.reset_index(drop=True)


def collapse_equivalent_listing_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Collapse duplicate rows for the same ticker/exchange/asset/currency.

    Tiingo can contain repeated rows for the same listing due to listing
    lifecycle updates, missing-date placeholders, or vendor corrections.
    For security-master planning, keep broad availability:
        start_date = earliest valid start date
        end_date   = latest valid end date
    """
    group_cols = [
        "ticker",
        "exchange",
        "asset_type",
        "price_currency",
    ]

    grouped = (
        df.groupby(group_cols, dropna=False)
        .agg(
            source_ticker=("source_ticker", "first"),
            source_raw_symbol=("source_raw_symbol", "first"),
            company_name=("company_name", "first"),
            start_date=("start_date", "min"),
            end_date=("end_date", "max"),
            raw_listing_row_count=("ticker", "size"),
        )
        .reset_index()
    )

    return grouped


def add_canonical_ranking_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add ranking columns for choosing one canonical row per ticker.

    Priority is designed for this project:
      - US common-stock research
      - Tiingo API requests are ticker-based
      - duplicate global listings should not let TSX/CAD rows override US/USD rows
    """
    output = df.copy()

    output["_asset_type_priority"] = (
        output["asset_type"]
        .map(ASSET_TYPE_PRIORITY)
        .fillna(99)
        .astype(int)
    )

    output["_is_usd"] = output["price_currency"].eq("USD").fillna(False).astype(int)

    output["_exchange_priority"] = (
        output["exchange"]
        .map(US_MAJOR_EXCHANGE_PRIORITY)
        .fillna(EXCHANGE_PRIORITY_FALLBACK)
        .astype(int)
    )

    output["_is_us_major_exchange"] = (
        output["_exchange_priority"] < EXCHANGE_PRIORITY_FALLBACK
    ).astype(int)

    output["_has_valid_start_date"] = output["start_date"].notna().astype(int)
    output["_has_valid_end_date"] = output["end_date"].notna().astype(int)
    output["_has_valid_dates"] = (
        output["start_date"].notna() & output["end_date"].notna()
    ).astype(int)

    output["_start_date_sort"] = output["start_date"]
    output["_end_date_sort"] = output["end_date"]

    output["_history_days"] = (
        output["end_date"] - output["start_date"]
    ).dt.days.fillna(-1)

    return output


def choose_canonical_ticker_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Choose one canonical row per ticker.

    Returns:
        canonical_df: one row per ticker
        resolution_report: all duplicate candidates with ranking columns
    """
    ranked = add_canonical_ranking_columns(df)

    ranked = ranked.sort_values(
        by=[
            "ticker",
            "_asset_type_priority",
            "_is_usd",
            "_is_us_major_exchange",
            "_exchange_priority",
            "_has_valid_dates",
            "_has_valid_end_date",
            "_end_date_sort",
            "_history_days",
            "_start_date_sort",
        ],
        ascending=[
            True,
            True,    # Stock before ETF / mutual fund
            False,   # USD before non-USD
            False,   # US major exchange before others
            True,    # NASDAQ / NYSE / NYSE ARCA / NYSE MKT
            False,   # valid start+end dates first
            False,   # valid end date first
            False,   # latest end date first
            False,   # longer history first
            True,    # earlier start date first
        ],
    ).reset_index(drop=True)

    ranked["canonical_rank"] = ranked.groupby("ticker").cumcount() + 1
    ranked["ticker_duplicate_count"] = ranked.groupby("ticker")["ticker"].transform(
        "size"
    )

    canonical = ranked[ranked["canonical_rank"] == 1].copy()

    duplicate_report = ranked[ranked["ticker_duplicate_count"] > 1].copy()

    return canonical, duplicate_report


def compute_latest_supported_end_date(df: pd.DataFrame) -> pd.Timestamp:
    """Compute latest valid supported end date."""
    valid_end_dates = pd.to_datetime(df["end_date"], errors="coerce").dropna()

    if valid_end_dates.empty:
        raise ValueError("No valid end_date values found in supported_tickers data.")

    return valid_end_dates.max().normalize()


def get_active_grace_days(config: dict[str, Any]) -> int:
    """Read active end-date grace window from config."""
    dates_cfg = config.get("dates", {})
    return int(dates_cfg.get("active_end_date_grace_days", 7))


def finalize_security_master(
    canonical_df: pd.DataFrame,
    config: dict[str, Any],
    latest_supported_end_date: pd.Timestamp,
) -> pd.DataFrame:
    """Create final dim_security dataframe."""
    active_grace_days = get_active_grace_days(config)
    active_cutoff = latest_supported_end_date - pd.Timedelta(days=active_grace_days)

    output = canonical_df.copy()

    output["source"] = SOURCE
    output["security_id"] = output["source"] + ":" + output["ticker"]

    output["is_active"] = output["end_date"].notna() & (
        output["end_date"] >= active_cutoff
    )

    output["loaded_at"] = datetime.now(timezone.utc).isoformat()

    output["start_date"] = pd.to_datetime(output["start_date"], errors="coerce").dt.date
    output["end_date"] = pd.to_datetime(output["end_date"], errors="coerce").dt.date

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

    output = output[ordered_columns].sort_values(["ticker"]).reset_index(drop=True)

    if output["security_id"].isna().any():
        raise ValueError("dim_security contains null security_id.")

    if output["ticker"].isna().any():
        raise ValueError("dim_security contains null ticker.")

    duplicate_security_ids = output[output["security_id"].duplicated(keep=False)]
    if not duplicate_security_ids.empty:
        raise ValueError(
            "dim_security still contains duplicate security_id values:\n"
            f"{duplicate_security_ids.head(50).to_string(index=False)}"
        )

    duplicate_tickers = output[output["ticker"].duplicated(keep=False)]
    if not duplicate_tickers.empty:
        raise ValueError(
            "dim_security still contains duplicate ticker values:\n"
            f"{duplicate_tickers.head(50).to_string(index=False)}"
        )

    return output


def write_duplicate_resolution_report(
    duplicate_report: pd.DataFrame,
    report_path: Path,
) -> None:
    """Write duplicate ticker resolution report for inspection."""
    report_path.parent.mkdir(parents=True, exist_ok=True)

    if duplicate_report.empty:
        pd.DataFrame(
            columns=[
                "ticker",
                "canonical_rank",
                "ticker_duplicate_count",
                "exchange",
                "asset_type",
                "price_currency",
                "start_date",
                "end_date",
            ]
        ).to_csv(report_path, index=False)
        return

    report = duplicate_report.copy()

    keep_cols = [
        "ticker",
        "canonical_rank",
        "ticker_duplicate_count",
        "exchange",
        "asset_type",
        "price_currency",
        "start_date",
        "end_date",
        "raw_listing_row_count",
        "_asset_type_priority",
        "_is_usd",
        "_is_us_major_exchange",
        "_exchange_priority",
        "_has_valid_dates",
        "_end_date_sort",
        "_history_days",
    ]

    keep_cols = [col for col in keep_cols if col in report.columns]

    report[keep_cols].sort_values(
        ["ticker", "canonical_rank"],
    ).to_csv(report_path, index=False)

    print(f"Duplicate resolution report written to: {report_path}")


def upload_to_gcs(
    local_path: Path,
    bucket_name: str,
    destination_blob_name: str,
    dry_run: bool = False,
) -> None:
    """Upload local file to GCS."""
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


def print_summary(
    raw_df: pd.DataFrame,
    normalized_df: pd.DataFrame,
    collapsed_df: pd.DataFrame,
    dim_security: pd.DataFrame,
    latest_supported_end_date: pd.Timestamp,
) -> None:
    """Print security master build summary."""
    print("\nSecurity master build summary")
    print("-----------------------------")
    print(f"Raw supported_tickers rows: {len(raw_df):,}")
    print(f"Normalized rows: {len(normalized_df):,}")
    print(f"Collapsed listing rows: {len(collapsed_df):,}")
    print(f"Final dim_security rows: {len(dim_security):,}")
    print(f"Latest supported end date: {latest_supported_end_date.date()}")

    print("\nAsset type counts:")
    print(dim_security["asset_type"].value_counts(dropna=False).head(20).to_string())

    print("\nExchange counts:")
    print(dim_security["exchange"].value_counts(dropna=False).head(30).to_string())

    print("\nCurrency counts:")
    print(dim_security["price_currency"].value_counts(dropna=False).head(20).to_string())

    print("\nActive counts:")
    print(dim_security["is_active"].value_counts(dropna=False).to_string())

    print("\nKnown ticker sanity check:")
    known_tickers = [
        "AAPL",
        "MSFT",
        "NVDA",
        "AMZN",
        "META",
        "GOOG",
        "GOOGL",
        "TSLA",
        "ABNB",
        "ABBV",
        "ABT",
    ]

    ticker_set = set(dim_security["ticker"].astype(str))

    for ticker in known_tickers:
        exists = ticker in ticker_set
        print(f"{ticker}: {exists}")
        if exists:
            row = dim_security.loc[
                dim_security["ticker"] == ticker,
                [
                    "ticker",
                    "exchange",
                    "asset_type",
                    "price_currency",
                    "start_date",
                    "end_date",
                    "is_active",
                ],
            ].iloc[0]
            print("  " + row.to_dict().__repr__())

def build_dim_security(
    raw_df: pd.DataFrame,
    config: dict[str, Any] | None = None,
    active_end_date_grace_days: int | None = None,
) -> pd.DataFrame:
    """
    Backward-compatible helper used by tests.

    Supports both:
        build_dim_security(raw_df, config)
    and old test style:
        build_dim_security(raw_df, active_end_date_grace_days=7)
    """
    if config is None:
        config = {}

    config = dict(config)
    dates_cfg = dict(config.get("dates", {}))

    if active_end_date_grace_days is not None:
        dates_cfg["active_end_date_grace_days"] = active_end_date_grace_days

    config["dates"] = dates_cfg

    normalized_df = normalize_supported_tickers(raw_df)
    latest_supported_end_date = compute_latest_supported_end_date(normalized_df)

    collapsed_df = collapse_equivalent_listing_rows(normalized_df)
    canonical_df, _duplicate_report = choose_canonical_ticker_rows(collapsed_df)

    dim_security = finalize_security_master(
        canonical_df=canonical_df,
        config=config,
        latest_supported_end_date=latest_supported_end_date,
    )

    return dim_security


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build canonical security master from Tiingo supported_tickers.csv."
    )
    parser.add_argument(
        "--input-path",
        type=str,
        default=str(DEFAULT_INPUT_PATH),
        help="Path to Tiingo supported_tickers.csv.",
    )
    parser.add_argument(
        "--output-path",
        type=str,
        default=str(DEFAULT_OUTPUT_PATH),
        help="Output path for dim_security.parquet.",
    )
    parser.add_argument(
        "--duplicate-report-path",
        type=str,
        default=str(DEFAULT_REPORT_PATH),
        help="Output path for duplicate ticker resolution CSV report.",
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

    input_path = Path(args.input_path)
    output_path = Path(args.output_path)
    duplicate_report_path = Path(args.duplicate_report_path)

    if not input_path.exists():
        raise FileNotFoundError(
            f"Supported tickers file not found: {input_path}. "
            "Run `python -m scripts.ingest_tiingo_supported_tickers` first."
        )

    config = load_config(CONFIG_PATH)

    raw_df = pd.read_csv(input_path)

    normalized_df = normalize_supported_tickers(raw_df)
    latest_supported_end_date = compute_latest_supported_end_date(normalized_df)

    collapsed_df = collapse_equivalent_listing_rows(normalized_df)
    canonical_df, duplicate_report = choose_canonical_ticker_rows(collapsed_df)

    dim_security = finalize_security_master(
        canonical_df=canonical_df,
        config=config,
        latest_supported_end_date=latest_supported_end_date,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    dim_security.to_parquet(output_path, index=False)

    write_duplicate_resolution_report(
        duplicate_report=duplicate_report,
        report_path=duplicate_report_path,
    )

    print_summary(
        raw_df=raw_df,
        normalized_df=normalized_df,
        collapsed_df=collapsed_df,
        dim_security=dim_security,
        latest_supported_end_date=latest_supported_end_date,
    )

    print(f"\nWrote dim_security to: {output_path}")

    if not args.no_gcs:
        bucket_name = os.getenv("GCS_BUCKET", "")
        upload_to_gcs(
            local_path=output_path,
            bucket_name=bucket_name,
            destination_blob_name=DEFAULT_GCS_DESTINATION,
            dry_run=args.dry_run_gcs,
        )


if __name__ == "__main__":
    main()