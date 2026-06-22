# import pandas as pd

from scripts.legacy.transform_tiingo_prices_to_dwd import (
    normalize_tiingo_price_data,
    validate_dwd_prices,
)


def test_normalize_tiingo_price_data_schema() -> None:
    payload = {
        "records": [
            {
                "date": "2025-01-02T00:00:00.000Z",
                "open": 100.0,
                "high": 102.0,
                "low": 99.0,
                "close": 101.0,
                "volume": 1_000_000,
                "adjOpen": 100.0,
                "adjHigh": 102.0,
                "adjLow": 99.0,
                "adjClose": 101.0,
                "adjVolume": 1_000_000,
                "divCash": 0.0,
                "splitFactor": 1.0,
            }
        ]
    }

    out = normalize_tiingo_price_data("AAPL", payload, "test-load-id")

    expected_columns = [
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

    assert list(out.columns) == expected_columns
    assert len(out) == 1
    assert out["ticker"].iloc[0] == "AAPL"
    assert out["security_id"].iloc[0] == "US_AAPL"
    assert out["adj_close"].iloc[0] == 101.0


def test_validate_dwd_prices_accepts_valid_tiingo_data() -> None:
    payload = {
        "records": [
            {
                "date": "2025-01-02T00:00:00.000Z",
                "open": 100.0,
                "high": 102.0,
                "low": 99.0,
                "close": 101.0,
                "volume": 1_000_000,
                "adjOpen": 100.0,
                "adjHigh": 102.0,
                "adjLow": 99.0,
                "adjClose": 101.0,
                "adjVolume": 1_000_000,
                "divCash": 0.0,
                "splitFactor": 1.0,
            }
        ]
    }

    out = normalize_tiingo_price_data("AAPL", payload, "test-load-id")

    validate_dwd_prices(out)