from __future__ import annotations

import pandas as pd
import pytest

from quant_platform.prices.normalize import (
    normalize_tiingo_price_rows,
)
from quant_platform.prices.schema import DWD_PRICE_COLUMNS
from quant_platform.prices.transform import (
    combine_dwd_price_frames,
    deduplicate_dwd_price_frame,
    validate_dwd_price_frame,
    write_year_month_partitions,
)


def make_frame(
    *,
    ticker: str = "AAPL",
    security_id: str = "tiingo:AAPL",
    row_date: str = "2026-06-12T00:00:00.000Z",
    close: float = 101.0,
    loaded_at: str = "2026-06-12T22:00:00Z",
) -> pd.DataFrame:
    rows = [
        {
            "date": row_date,
            "open": close - 1,
            "high": close + 1,
            "low": close - 2,
            "close": close,
            "volume": 1_000_000,
            "adjOpen": close - 1,
            "adjHigh": close + 1,
            "adjLow": close - 2,
            "adjClose": close,
            "adjVolume": 1_000_000,
            "divCash": 0.0,
            "splitFactor": 1.0,
        }
    ]

    return normalize_tiingo_price_rows(
        raw_rows=rows,
        ticker=ticker,
        security_id=security_id,
        load_id=f"load-{ticker}",
        loaded_at=loaded_at,
    )


def test_validate_dwd_price_frame_accepts_valid_data():
    validate_dwd_price_frame(make_frame())


def test_validate_dwd_price_frame_rejects_empty_frame():
    empty = pd.DataFrame(columns=list(DWD_PRICE_COLUMNS))

    with pytest.raises(
        ValueError,
        match="DataFrame is empty",
    ):
        validate_dwd_price_frame(empty)


def test_validate_dwd_price_frame_rejects_duplicate_key():
    frame = make_frame()
    duplicate = pd.concat(
        [frame, frame],
        ignore_index=True,
    )

    with pytest.raises(
        ValueError,
        match="duplicate security_id/date",
    ):
        validate_dwd_price_frame(duplicate)


def test_validate_dwd_price_frame_rejects_non_positive_adj_close():
    frame = make_frame()
    frame.loc[0, "adj_close"] = 0.0

    with pytest.raises(
        ValueError,
        match="invalid adj_close",
    ):
        validate_dwd_price_frame(frame)


def test_validate_dwd_price_frame_rejects_high_below_low():
    frame = make_frame()
    frame.loc[0, "high"] = 90.0
    frame.loc[0, "low"] = 100.0

    with pytest.raises(
        ValueError,
        match="high below low",
    ):
        validate_dwd_price_frame(frame)


def test_validate_dwd_price_frame_rejects_negative_volume():
    frame = make_frame()
    frame.loc[0, "volume"] = -1

    with pytest.raises(
        ValueError,
        match="negative volume",
    ):
        validate_dwd_price_frame(frame)


def test_deduplicate_dwd_price_frame_keeps_latest_loaded_at():
    earlier = make_frame(
        close=100.0,
        loaded_at="2026-06-12T21:00:00Z",
    )
    later = make_frame(
        close=101.0,
        loaded_at="2026-06-12T22:00:00Z",
    )

    combined = pd.concat(
        [earlier, later],
        ignore_index=True,
    )

    result = deduplicate_dwd_price_frame(combined)

    assert len(result) == 1
    assert result.loc[0, "close"] == 101.0
    assert result.loc[0, "adj_close"] == 101.0


def test_combine_dwd_price_frames_deduplicates_and_validates():
    earlier = make_frame(
        close=100.0,
        loaded_at="2026-06-12T21:00:00Z",
    )
    later = make_frame(
        close=101.0,
        loaded_at="2026-06-12T22:00:00Z",
    )
    msft = make_frame(
        ticker="MSFT",
        security_id="tiingo:MSFT",
        close=450.0,
    )

    result = combine_dwd_price_frames(
        [earlier, later, msft]
    )

    assert len(result) == 2
    assert set(result["ticker"]) == {"AAPL", "MSFT"}

    aapl = result[result["ticker"] == "AAPL"].iloc[0]

    assert aapl["close"] == 101.0


def test_combine_empty_frames_returns_empty_canonical_frame():
    empty = pd.DataFrame(columns=list(DWD_PRICE_COLUMNS))

    result = combine_dwd_price_frames([empty])

    assert result.empty
    assert tuple(result.columns) == DWD_PRICE_COLUMNS


def test_write_year_month_partitions_uses_temp_directory(tmp_path):
    january = make_frame(
        row_date="2026-01-30T00:00:00.000Z",
    )
    february = make_frame(
        ticker="MSFT",
        security_id="tiingo:MSFT",
        row_date="2026-02-02T00:00:00.000Z",
        close=450.0,
    )

    combined = combine_dwd_price_frames(
        [january, february]
    )

    output_root = tmp_path / "equity_price_daily"

    paths = write_year_month_partitions(
        combined,
        output_root,
    )

    assert paths == [
        (
            output_root
            / "year=2026"
            / "month=01"
            / "part-000.parquet"
        ),
        (
            output_root
            / "year=2026"
            / "month=02"
            / "part-000.parquet"
        ),
    ]

    assert all(path.exists() for path in paths)

    january_read = pd.read_parquet(paths[0])
    february_read = pd.read_parquet(paths[1])

    assert tuple(january_read.columns) == DWD_PRICE_COLUMNS
    assert tuple(february_read.columns) == DWD_PRICE_COLUMNS
    assert january_read.loc[0, "ticker"] == "AAPL"
    assert february_read.loc[0, "ticker"] == "MSFT"