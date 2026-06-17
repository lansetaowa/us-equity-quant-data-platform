from __future__ import annotations

from pathlib import Path


SOURCE = "tiingo"
DATASET_NAME = "equity_price_daily"
DEFAULT_WINDOWED_PRICE_FILENAME = "prices.json"


def normalize_ticker(ticker: str) -> str:
    """Normalize ticker symbol for path generation."""
    ticker_norm = str(ticker).strip().upper()

    if not ticker_norm:
        raise ValueError("ticker must not be empty")

    return ticker_norm


def build_price_dataset_root(ods_root: str | Path) -> Path:
    """
    Build root path for raw Tiingo daily equity prices.

    Example:
    data/ods/source=tiingo/dataset=equity_price_daily
    """
    return Path(ods_root) / f"source={SOURCE}" / f"dataset={DATASET_NAME}"


def build_legacy_price_raw_path(
    ods_root: str | Path,
    ticker: str,
) -> Path:
    """
    Build the legacy Week 8 bootstrap raw price path.

    Example:
    data/ods/source=tiingo/dataset=equity_price_daily/symbol=AAPL/aapl_prices.json

    This function exists so the future transformer can read old bootstrap files.
    New incremental download code should not write to this path.
    """
    ticker_norm = normalize_ticker(ticker)

    return (
        build_price_dataset_root(ods_root)
        / f"symbol={ticker_norm}"
        / f"{ticker_norm.lower()}_prices.json"
    )


def build_windowed_price_raw_path(
    ods_root: str | Path,
    ticker: str,
    request_start_date: str,
    request_end_date: str,
    filename: str = DEFAULT_WINDOWED_PRICE_FILENAME,
) -> Path:
    """
    Build the Week 8.5+ windowed raw price path.

    Example:
    data/ods/source=tiingo/dataset=equity_price_daily/symbol=AAPL/
      request_start=2026-06-12/request_end=2026-06-12/prices.json
    """
    ticker_norm = normalize_ticker(ticker)

    request_start_date = str(request_start_date).strip()
    request_end_date = str(request_end_date).strip()
    filename = str(filename).strip()

    if not request_start_date:
        raise ValueError("request_start_date must not be empty")
    if not request_end_date:
        raise ValueError("request_end_date must not be empty")
    if not filename:
        raise ValueError("filename must not be empty")

    return (
        build_price_dataset_root(ods_root)
        / f"symbol={ticker_norm}"
        / f"request_start={request_start_date}"
        / f"request_end={request_end_date}"
        / filename
    )


def to_gcs_object_path(local_path: str | Path) -> str:
    """
    Convert local data lake path to GCS object-relative path.

    Example:
    data/ods/source=tiingo/... -> ods/source=tiingo/...

    This preserves the Week 8 convention that local `data/` should not become
    part of the GCS object prefix.
    """
    path = Path(local_path)

    if path.parts and path.parts[0] == "data":
        path = Path(*path.parts[1:])

    return path.as_posix()


# Backward-compatible alias for earlier Day 1 naming.
def strip_local_data_prefix_for_gcs(local_path: str | Path) -> str:
    """Backward-compatible alias for to_gcs_object_path."""
    return to_gcs_object_path(local_path)