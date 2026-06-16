from pathlib import Path

import pytest

from scripts.price_path_utils import (
    build_legacy_price_raw_path,
    build_price_dataset_root,
    build_windowed_price_raw_path,
    normalize_ticker,
    to_gcs_object_path,
)


def test_normalize_ticker_uppercases_and_strips():
    assert normalize_ticker(" aapl ") == "AAPL"


def test_normalize_ticker_rejects_empty():
    with pytest.raises(ValueError, match="ticker must not be empty"):
        normalize_ticker("   ")


def test_build_price_dataset_root():
    assert build_price_dataset_root("data/ods") == Path(
        "data/ods/source=tiingo/dataset=equity_price_daily"
    )


def test_legacy_price_raw_path():
    path = build_legacy_price_raw_path(
        ods_root="data/ods",
        ticker="AAPL",
    )

    assert path == Path(
        "data/ods/source=tiingo/dataset=equity_price_daily/"
        "symbol=AAPL/aapl_prices.json"
    )


def test_windowed_price_raw_path():
    path = build_windowed_price_raw_path(
        ods_root="data/ods",
        ticker="AAPL",
        request_start_date="2026-06-12",
        request_end_date="2026-06-12",
    )

    assert path == Path(
        "data/ods/source=tiingo/dataset=equity_price_daily/"
        "symbol=AAPL/request_start=2026-06-12/"
        "request_end=2026-06-12/prices.json"
    )


def test_windowed_path_does_not_equal_legacy_path():
    legacy = build_legacy_price_raw_path(
        ods_root="data/ods",
        ticker="AAPL",
    )

    windowed = build_windowed_price_raw_path(
        ods_root="data/ods",
        ticker="AAPL",
        request_start_date="2026-06-12",
        request_end_date="2026-06-12",
    )

    assert legacy != windowed
    assert legacy.name == "aapl_prices.json"
    assert windowed.name == "prices.json"


def test_windowed_path_rejects_empty_start_date():
    with pytest.raises(ValueError, match="request_start_date must not be empty"):
        build_windowed_price_raw_path(
            ods_root="data/ods",
            ticker="AAPL",
            request_start_date="",
            request_end_date="2026-06-12",
        )


def test_windowed_path_rejects_empty_end_date():
    with pytest.raises(ValueError, match="request_end_date must not be empty"):
        build_windowed_price_raw_path(
            ods_root="data/ods",
            ticker="AAPL",
            request_start_date="2026-06-12",
            request_end_date="",
        )


def test_to_gcs_object_path_strips_local_data_prefix():
    local_path = Path(
        "data/ods/source=tiingo/dataset=equity_price_daily/"
        "symbol=AAPL/request_start=2026-06-12/"
        "request_end=2026-06-12/prices.json"
    )

    assert to_gcs_object_path(local_path) == (
        "ods/source=tiingo/dataset=equity_price_daily/"
        "symbol=AAPL/request_start=2026-06-12/"
        "request_end=2026-06-12/prices.json"
    )


def test_to_gcs_object_path_without_data_prefix():
    path = Path(
        "ods/source=tiingo/dataset=equity_price_daily/"
        "symbol=AAPL/prices.json"
    )

    assert to_gcs_object_path(path) == (
        "ods/source=tiingo/dataset=equity_price_daily/"
        "symbol=AAPL/prices.json"
    )