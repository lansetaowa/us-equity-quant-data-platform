from __future__ import annotations

from datetime import date

import pytest

from quant_platform.prices.normalize import (
    extract_tiingo_price_rows,
    normalize_tiingo_price_payload,
    normalize_tiingo_price_rows,
)
from quant_platform.prices.schema import DWD_PRICE_COLUMNS


def valid_raw_row(
    *,
    row_date: str = "2026-06-12T00:00:00.000Z",
    close: float = 101.0,
) -> dict:
    return {
        "date": row_date,
        "open": 100.0,
        "high": 102.0,
        "low": 99.0,
        "close": close,
        "volume": 1_000_000,
        "adjOpen": 100.0,
        "adjHigh": 102.0,
        "adjLow": 99.0,
        "adjClose": close,
        "adjVolume": 1_000_000,
        "divCash": 0.0,
        "splitFactor": 1.0,
    }


def normalize_rows(rows: list[dict]):
    return normalize_tiingo_price_rows(
        raw_rows=rows,
        ticker=" aapl ",
        security_id="tiingo:AAPL",
        load_id="test-load",
        loaded_at="2026-06-12T22:00:00Z",
    )


def test_normalize_tiingo_price_rows_schema_and_metadata():
    result = normalize_rows([valid_raw_row()])

    assert tuple(result.columns) == DWD_PRICE_COLUMNS
    assert len(result) == 1

    row = result.iloc[0]

    assert row["security_id"] == "tiingo:AAPL"
    assert row["ticker"] == "AAPL"
    assert row["date"] == date(2026, 6, 12)
    assert row["adj_close"] == 101.0
    assert row["source"] == "tiingo"
    assert row["load_id"] == "test-load"
    assert row["loaded_at"] == "2026-06-12T22:00:00+00:00"


def test_scalar_metadata_assignment_populates_all_rows():
    result = normalize_rows(
        [
            valid_raw_row(
                row_date="2026-06-11T00:00:00.000Z"
            ),
            valid_raw_row(
                row_date="2026-06-12T00:00:00.000Z"
            ),
        ]
    )

    assert len(result) == 2
    assert result["security_id"].notna().all()
    assert result["ticker"].notna().all()
    assert result["source"].notna().all()
    assert result["load_id"].notna().all()
    assert result["loaded_at"].notna().all()


def test_adjusted_fields_fall_back_to_unadjusted_fields():
    row = valid_raw_row()

    for column in [
        "adjOpen",
        "adjHigh",
        "adjLow",
        "adjClose",
        "adjVolume",
    ]:
        row.pop(column)

    result = normalize_rows([row])
    output = result.iloc[0]

    assert output["adj_open"] == output["open"]
    assert output["adj_high"] == output["high"]
    assert output["adj_low"] == output["low"]
    assert output["adj_close"] == output["close"]
    assert output["adj_volume"] == output["volume"]


def test_snake_case_adjusted_aliases_are_supported():
    row = valid_raw_row()

    row["adj_open"] = row.pop("adjOpen")
    row["adj_high"] = row.pop("adjHigh")
    row["adj_low"] = row.pop("adjLow")
    row["adj_close"] = row.pop("adjClose")
    row["adj_volume"] = row.pop("adjVolume")
    row["div_cash"] = row.pop("divCash")
    row["split_factor"] = row.pop("splitFactor")

    result = normalize_rows([row])

    assert result.loc[0, "adj_close"] == 101.0
    assert result.loc[0, "split_factor"] == 1.0


def test_empty_rows_return_empty_canonical_frame():
    result = normalize_rows([])

    assert result.empty
    assert tuple(result.columns) == DWD_PRICE_COLUMNS


def test_missing_date_column_is_rejected():
    row = valid_raw_row()
    row.pop("date")

    with pytest.raises(
        ValueError,
        match="do not contain date",
    ):
        normalize_rows([row])


def test_non_integral_volume_is_rejected():
    row = valid_raw_row()
    row["volume"] = 123.5

    with pytest.raises(
        ValueError,
        match="volume contains non-integral values",
    ):
        normalize_rows([row])


def test_extract_tiingo_rows_supports_list_payload():
    rows = [valid_raw_row()]

    assert extract_tiingo_price_rows(rows) == rows


def test_extract_tiingo_rows_supports_legacy_records_payload():
    rows = [valid_raw_row()]
    payload = {"records": rows}

    assert extract_tiingo_price_rows(payload) == rows


def test_extract_tiingo_rows_rejects_mapping_without_records():
    with pytest.raises(
        ValueError,
        match="must contain a 'records' field",
    ):
        extract_tiingo_price_rows({"ticker": "AAPL"})


def test_normalize_tiingo_payload_supports_legacy_shape():
    result = normalize_tiingo_price_payload(
        {"records": [valid_raw_row()]},
        ticker="AAPL",
        security_id="tiingo:AAPL",
        load_id="test-load",
        loaded_at="2026-06-12T22:00:00Z",
    )

    assert len(result) == 1
    assert result.loc[0, "ticker"] == "AAPL"