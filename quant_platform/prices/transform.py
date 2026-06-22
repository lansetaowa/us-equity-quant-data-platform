from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pandas as pd

from quant_platform.prices.schema import (
    DWD_PRICE_COLUMNS,
    DWD_PRICE_KEY_COLUMNS,
    DWD_PRICE_REQUIRED_NON_NULL_COLUMNS,
    empty_dwd_price_frame,
    select_dwd_price_columns,
)


def _example_records(
    df: pd.DataFrame,
    mask: pd.Series,
    columns: list[str],
    limit: int = 5,
) -> list[dict[str, Any]]:
    existing_columns = [
        column
        for column in columns
        if column in df.columns
    ]

    return (
        df.loc[mask, existing_columns]
        .head(limit)
        .to_dict("records")
    )


def validate_dwd_price_frame(df: pd.DataFrame) -> None:
    """Validate a canonical DWD price DataFrame."""
    working = select_dwd_price_columns(df)

    if working.empty:
        raise ValueError("DWD price DataFrame is empty")

    null_columns = [
        column
        for column in DWD_PRICE_REQUIRED_NON_NULL_COLUMNS
        if working[column].isna().any()
    ]

    if null_columns:
        raise ValueError(
            "DWD price DataFrame has nulls in required columns: "
            f"{null_columns}"
        )

    parsed_dates = pd.to_datetime(
        working["date"],
        errors="coerce",
    )

    invalid_dates = parsed_dates.isna()

    if bool(invalid_dates.any()):
        examples = _example_records(
            working,
            invalid_dates,
            ["security_id", "ticker", "date"],
        )

        raise ValueError(
            f"DWD price DataFrame has invalid dates: {examples}"
        )

    parsed_loaded_at = pd.to_datetime(
        working["loaded_at"],
        errors="coerce",
        utc=True,
    )

    invalid_loaded_at = parsed_loaded_at.isna()

    if bool(invalid_loaded_at.any()):
        examples = _example_records(
            working,
            invalid_loaded_at,
            ["security_id", "ticker", "loaded_at"],
        )

        raise ValueError(
            f"DWD price DataFrame has invalid loaded_at values: "
            f"{examples}"
        )

    duplicate_mask = working.duplicated(
        subset=list(DWD_PRICE_KEY_COLUMNS),
        keep=False,
    )

    if bool(duplicate_mask.any()):
        duplicate_count = int(
            working.duplicated(
                subset=list(DWD_PRICE_KEY_COLUMNS)
            ).sum()
        )

        examples = _example_records(
            working,
            duplicate_mask,
            ["security_id", "ticker", "date", "loaded_at"],
        )

        raise ValueError(
            "DWD price DataFrame has duplicate security_id/date rows. "
            f"Duplicate count={duplicate_count}. "
            f"Examples={examples}"
        )

    close = pd.to_numeric(
        working["close"],
        errors="coerce",
    )
    invalid_close = close.notna() & close.le(0)

    if bool(invalid_close.any()):
        examples = _example_records(
            working,
            invalid_close,
            ["ticker", "date", "close"],
        )

        raise ValueError(
            f"DWD price DataFrame has non-positive close: {examples}"
        )

    adj_close = pd.to_numeric(
        working["adj_close"],
        errors="coerce",
    )
    invalid_adj_close = adj_close.isna() | adj_close.le(0)

    if bool(invalid_adj_close.any()):
        examples = _example_records(
            working,
            invalid_adj_close,
            ["ticker", "date", "adj_close"],
        )

        raise ValueError(
            "DWD price DataFrame has invalid adj_close values: "
            f"{examples}"
        )

    high = pd.to_numeric(
        working["high"],
        errors="coerce",
    )
    low = pd.to_numeric(
        working["low"],
        errors="coerce",
    )

    invalid_high_low = (
        high.notna()
        & low.notna()
        & high.lt(low)
    )

    if bool(invalid_high_low.any()):
        examples = _example_records(
            working,
            invalid_high_low,
            ["ticker", "date", "high", "low"],
        )

        raise ValueError(
            f"DWD price DataFrame has high below low: {examples}"
        )

    volume = pd.to_numeric(
        working["volume"],
        errors="coerce",
    )
    invalid_volume = volume.notna() & volume.lt(0)

    if bool(invalid_volume.any()):
        examples = _example_records(
            working,
            invalid_volume,
            ["ticker", "date", "volume"],
        )

        raise ValueError(
            f"DWD price DataFrame has negative volume: {examples}"
        )

    adj_volume = pd.to_numeric(
        working["adj_volume"],
        errors="coerce",
    )
    invalid_adj_volume = (
        adj_volume.notna()
        & adj_volume.lt(0)
    )

    if bool(invalid_adj_volume.any()):
        examples = _example_records(
            working,
            invalid_adj_volume,
            ["ticker", "date", "adj_volume"],
        )

        raise ValueError(
            "DWD price DataFrame has negative adj_volume: "
            f"{examples}"
        )

    split_factor = pd.to_numeric(
        working["split_factor"],
        errors="coerce",
    )
    invalid_split_factor = (
        split_factor.notna()
        & split_factor.le(0)
    )

    if bool(invalid_split_factor.any()):
        examples = _example_records(
            working,
            invalid_split_factor,
            ["ticker", "date", "split_factor"],
        )

        raise ValueError(
            "DWD price DataFrame has non-positive split_factor: "
            f"{examples}"
        )


def deduplicate_dwd_price_frame(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Deduplicate canonical price rows by security_id/date.

    The row with the latest loaded_at wins. If loaded_at is identical, the
    later input row wins.
    """
    if df.empty:
        return empty_dwd_price_frame()

    working = select_dwd_price_columns(df)

    date_sort = pd.to_datetime(
        working["date"],
        errors="coerce",
    )
    loaded_at_sort = pd.to_datetime(
        working["loaded_at"],
        errors="coerce",
        utc=True,
    )

    if bool(date_sort.isna().any()):
        raise ValueError(
            "Cannot deduplicate DWD prices with invalid dates"
        )

    if bool(loaded_at_sort.isna().any()):
        raise ValueError(
            "Cannot deduplicate DWD prices with invalid loaded_at"
        )

    working["_date_sort"] = date_sort
    working["_loaded_at_sort"] = loaded_at_sort
    working["_input_order"] = range(len(working))

    working = working.sort_values(
        [
            "security_id",
            "_date_sort",
            "_loaded_at_sort",
            "_input_order",
        ]
    )

    working = working.drop_duplicates(
        subset=list(DWD_PRICE_KEY_COLUMNS),
        keep="last",
    )

    working = working.drop(
        columns=[
            "_date_sort",
            "_loaded_at_sort",
            "_input_order",
        ]
    )

    working = working.loc[:, list(DWD_PRICE_COLUMNS)]

    return working.sort_values(
        ["ticker", "date"],
    ).reset_index(drop=True)


def combine_dwd_price_frames(
    frames: Iterable[pd.DataFrame],
    *,
    validate: bool = True,
) -> pd.DataFrame:
    """
    Combine normalized price frames and deterministically deduplicate them.

    This is the core operation future legacy-plus-windowed transforms will use.
    """
    usable_frames = [
        select_dwd_price_columns(frame)
        for frame in frames
        if not frame.empty
    ]

    if not usable_frames:
        return empty_dwd_price_frame()

    combined = pd.concat(
        usable_frames,
        ignore_index=True,
    )

    combined = deduplicate_dwd_price_frame(combined)

    if validate:
        validate_dwd_price_frame(combined)

    return combined


def write_year_month_partitions(
    df: pd.DataFrame,
    output_root: str | Path,
    *,
    filename: str = "part-000.parquet",
) -> list[Path]:
    """
    Write validated DWD prices into year/month Parquet partitions.

    This function does not remove the output root, archive prior output, or
    promote a temporary directory. Those orchestration decisions remain outside
    the reusable transform core.
    """
    validate_dwd_price_frame(df)

    filename_norm = str(filename).strip()

    if not filename_norm:
        raise ValueError("filename must not be empty")

    root = Path(output_root)
    root.mkdir(parents=True, exist_ok=True)

    working = select_dwd_price_columns(df)
    dates = pd.to_datetime(working["date"])

    working["_year"] = dates.dt.year
    working["_month"] = dates.dt.month

    written_paths: list[Path] = []

    for (year, month), partition in working.groupby(
        ["_year", "_month"],
        sort=True,
    ):
        partition_dir = (
            root
            / f"year={int(year)}"
            / f"month={int(month):02d}"
        )
        partition_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        output_path = partition_dir / filename_norm

        write_df = (
            partition.drop(columns=["_year", "_month"])
            .loc[:, list(DWD_PRICE_COLUMNS)]
            .sort_values(["ticker", "date"])
            .reset_index(drop=True)
        )

        write_df.to_parquet(
            output_path,
            index=False,
        )

        written_paths.append(output_path)

    return written_paths