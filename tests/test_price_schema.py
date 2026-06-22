from __future__ import annotations

import pandas as pd
import pytest

from quant_platform.prices.schema import (
    DWD_PRICE_COLUMNS,
    empty_dwd_price_frame,
    select_dwd_price_columns,
)


def test_empty_dwd_price_frame_has_canonical_columns():
    df = empty_dwd_price_frame()

    assert df.empty
    assert tuple(df.columns) == DWD_PRICE_COLUMNS


def test_select_dwd_price_columns_orders_and_drops_extra_columns():
    df = pd.DataFrame(
        {
            **{
                column: [None]
                for column in reversed(DWD_PRICE_COLUMNS)
            },
            "temporary_column": ["drop-me"],
        }
    )

    selected = select_dwd_price_columns(df)

    assert tuple(selected.columns) == DWD_PRICE_COLUMNS
    assert "temporary_column" not in selected.columns


def test_select_dwd_price_columns_rejects_missing_column():
    df = pd.DataFrame(
        {
            column: [None]
            for column in DWD_PRICE_COLUMNS
            if column != "adj_close"
        }
    )

    with pytest.raises(
        ValueError,
        match="missing columns",
    ):
        select_dwd_price_columns(df)