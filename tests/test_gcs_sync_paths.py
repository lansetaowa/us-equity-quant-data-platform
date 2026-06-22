from pathlib import Path

import pytest

from quant_platform.storage.gcs_sync import (
    gcs_object_name_from_local_path,
)


def test_gcs_object_name_strips_local_data_prefix_for_ods():
    local_path = Path(
        "data/ods/source=tiingo/dataset=equity_price_daily/"
        "symbol=AAPL/aapl_prices.json"
    )

    object_name = gcs_object_name_from_local_path(local_path)

    assert object_name == (
        "ods/source=tiingo/dataset=equity_price_daily/"
        "symbol=AAPL/aapl_prices.json"
    )


def test_gcs_object_name_supports_windowed_ods_path():
    local_path = Path(
        "data/ods/source=tiingo/dataset=equity_price_daily/"
        "symbol=AAPL/request_start=2026-06-12/"
        "request_end=2026-06-12/prices.json"
    )

    object_name = gcs_object_name_from_local_path(local_path)

    assert object_name == (
        "ods/source=tiingo/dataset=equity_price_daily/"
        "symbol=AAPL/request_start=2026-06-12/"
        "request_end=2026-06-12/prices.json"
    )


def test_gcs_object_name_strips_local_data_prefix_for_dwd():
    local_path = Path(
        "data/dwd/equity_price_daily/"
        "year=2025/month=01/part-000.parquet"
    )

    object_name = gcs_object_name_from_local_path(local_path)

    assert object_name == (
        "dwd/equity_price_daily/"
        "year=2025/month=01/part-000.parquet"
    )


def test_gcs_object_name_rejects_non_data_path():
    with pytest.raises(
        ValueError,
        match="Local file must be under",
    ):
        gcs_object_name_from_local_path(Path("README.md"))