from pathlib import Path

from scripts.sync_data_to_gcs import gcs_object_name_from_local_path


def test_gcs_object_name_strips_local_data_prefix_for_ods() -> None:
    local_path = Path(
        "data/ods/source=tiingo/dataset=equity_price_daily/"
        "symbol=AAPL/aapl_prices.json"
    )

    object_name = gcs_object_name_from_local_path(local_path)

    assert object_name == (
        "ods/source=tiingo/dataset=equity_price_daily/"
        "symbol=AAPL/aapl_prices.json"
    )


def test_gcs_object_name_strips_local_data_prefix_for_dwd() -> None:
    local_path = Path(
        "data/dwd/equity_price_daily/year=2025/month=01/part-000.parquet"
    )

    object_name = gcs_object_name_from_local_path(local_path)

    assert object_name == (
        "dwd/equity_price_daily/year=2025/month=01/part-000.parquet"
    )


def test_gcs_object_name_rejects_non_data_path() -> None:
    local_path = Path("README.md")

    try:
        gcs_object_name_from_local_path(local_path)
    except ValueError as exc:
        assert "Local file must be under" in str(exc)
    else:
        raise AssertionError("Expected ValueError for path outside data/")