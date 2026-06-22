from __future__ import annotations

import pandas as pd


DWD_PRICE_COLUMNS: tuple[str, ...] = (
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
)

DWD_PRICE_KEY_COLUMNS: tuple[str, ...] = (
    "security_id",
    "date",
)

DWD_PRICE_REQUIRED_NON_NULL_COLUMNS: tuple[str, ...] = (
    "security_id",
    "ticker",
    "date",
    "adj_close",
    "source",
    "load_id",
    "loaded_at",
)

DWD_PRICE_NUMERIC_COLUMNS: tuple[str, ...] = (
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
)


def empty_dwd_price_frame() -> pd.DataFrame:
    """Return an empty DataFrame with the canonical DWD price columns."""
    return pd.DataFrame(columns=list(DWD_PRICE_COLUMNS))


def select_dwd_price_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return a copy containing canonical DWD columns in canonical order.

    Extra working columns are dropped. Missing canonical columns are rejected.
    """
    missing = [
        column
        for column in DWD_PRICE_COLUMNS
        if column not in df.columns
    ]

    if missing:
        raise ValueError(
            f"DWD price DataFrame is missing columns: {missing}"
        )

    return df.loc[:, list(DWD_PRICE_COLUMNS)].copy()