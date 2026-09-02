from __future__ import annotations

import math
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from quant_platform.research.config import ResearchPanelConfig
from quant_platform.research.io import (
    filter_frame_by_month,
    read_parquet_dataset_by_month,
    write_monthly_partitions,
)
from quant_platform.research.operation import parse_month_start

REQUIRED_LABEL_PRICE_COLUMNS = {
    "security_id",
    "ticker",
    "date",
    "adj_close",
}


def _prepare_label_price_frame(
    prices: pd.DataFrame,
) -> pd.DataFrame:
    missing = sorted(REQUIRED_LABEL_PRICE_COLUMNS - set(prices.columns))

    if missing:
        raise ValueError(f"Price frame missing label columns: {missing}")

    output = prices.copy()
    output["ticker"] = output["ticker"].astype(str).str.upper().str.strip()
    output["security_id"] = output["security_id"].astype(str).str.strip()
    output["date"] = pd.to_datetime(output["date"], errors="coerce").dt.date
    output["adj_close"] = pd.to_numeric(
        output["adj_close"],
        errors="coerce",
    )

    if output["date"].isna().any():
        raise ValueError("Price frame contains invalid dates")

    if output["adj_close"].isna().any() or (output["adj_close"] <= 0).any():
        raise ValueError("Price frame contains invalid adj_close values")

    return output.sort_values(["security_id", "date"]).reset_index(drop=True)


def _positive_horizons(
    horizons: tuple[int, ...] | list[int],
) -> tuple[int, ...]:
    output = tuple(sorted({int(value) for value in horizons}))

    if not output:
        raise ValueError("label horizons must not be empty")

    if any(value <= 0 for value in output):
        raise ValueError("label horizons must be positive")

    return output


def label_read_lookahead_months(
    horizons: tuple[int, ...] | list[int],
) -> int:
    max_horizon = max(_positive_horizons(horizons))

    # Approximate 21 trading days per month plus a safety buffer.
    return math.ceil(max_horizon / 21) + 2


def label_update_lookback_months(
    horizons: tuple[int, ...] | list[int],
) -> int:
    max_horizon = max(_positive_horizons(horizons))

    # New prices can complete labels for recent prior rows.
    return math.ceil(max_horizon / 21) + 1


def add_months(
    month: str | date | pd.Timestamp,
    months: int,
) -> date:
    parsed = parse_month_start(
        month,
        field_name="month",
    )

    return (pd.Period(parsed, freq="M") + months).to_timestamp().date()


def subtract_months(
    month: str | date | pd.Timestamp,
    months: int,
) -> date:
    parsed = parse_month_start(
        month,
        field_name="month",
    )

    return (pd.Period(parsed, freq="M") - months).to_timestamp().date()


def expand_label_output_start_month_for_price_report(
    source_start_month: str | date | pd.Timestamp,
    *,
    horizons: tuple[int, ...] | list[int],
) -> date:
    return subtract_months(
        source_start_month,
        label_update_lookback_months(horizons),
    )


def build_forward_return_labels(
    prices: pd.DataFrame,
    *,
    label_set: str,
    horizons: tuple[int, ...] | list[int],
) -> pd.DataFrame:
    working = _prepare_label_price_frame(prices)
    working["label_set"] = str(label_set).strip()

    if not working["label_set"].iloc[0]:
        raise ValueError("label_set must not be empty")

    grouped = working.groupby(
        "security_id",
        sort=False,
        group_keys=False,
    )

    parsed_horizons = _positive_horizons(horizons)

    for horizon in parsed_horizons:
        future_close = grouped["adj_close"].shift(-horizon)
        future_date = grouped["date"].shift(-horizon)

        label_col = f"label_fwd_ret_{horizon}d"
        has_label_col = f"has_label_fwd_ret_{horizon}d"
        future_date_col = f"label_fwd_date_{horizon}d"

        working[label_col] = future_close / working["adj_close"] - 1
        working[has_label_col] = working[label_col].notna()
        working[future_date_col] = future_date

    working["max_label_horizon_days"] = max(parsed_horizons)
    working["created_at_utc"] = datetime.now(UTC).isoformat()

    leading_columns = [
        "date",
        "security_id",
        "ticker",
        "label_set",
    ]
    label_columns = [
        column
        for horizon in parsed_horizons
        for column in [
            f"label_fwd_ret_{horizon}d",
            f"has_label_fwd_ret_{horizon}d",
            f"label_fwd_date_{horizon}d",
        ]
    ]
    trailing_columns = [
        "max_label_horizon_days",
        "created_at_utc",
    ]

    output_columns = [
        *leading_columns,
        *label_columns,
        *trailing_columns,
    ]

    return working.loc[:, output_columns].sort_values(
        ["date", "ticker", "security_id"]
    ).reset_index(drop=True)


def read_equity_prices_for_label_build(
    *,
    price_root: str | Path,
    start_month: str | date | None,
    end_month: str | date | None,
    horizons: tuple[int, ...] | list[int],
) -> pd.DataFrame:
    read_end_month = (
        add_months(
            end_month,
            label_read_lookahead_months(horizons),
        )
        if end_month is not None
        else None
    )

    return read_parquet_dataset_by_month(
        price_root,
        date_column="date",
        start_month=start_month,
        end_month=read_end_month,
    )


def write_label_partitions(
    labels: pd.DataFrame,
    *,
    config: ResearchPanelConfig,
    start_month: str | date | None = None,
    end_month: str | date | None = None,
    overwrite: bool = False,
    replace_existing_partitions: bool = False,
) -> tuple[list[Path], pd.DataFrame]:
    selected = filter_frame_by_month(
        labels,
        date_column="date",
        start_month=start_month,
        end_month=end_month,
    )

    root = config.label_output_root / f"label_set={config.label_set}"

    written = write_monthly_partitions(
        selected,
        output_root=root,
        date_column="date",
        overwrite=overwrite,
        replace_existing_partitions=replace_existing_partitions,
    )

    return written, selected


def label_column_names(
    horizons: tuple[int, ...] | list[int],
) -> list[str]:
    return [f"label_fwd_ret_{horizon}d" for horizon in _positive_horizons(horizons)]


def has_label_column_names(
    horizons: tuple[int, ...] | list[int],
) -> list[str]:
    return [
        f"has_label_fwd_ret_{horizon}d"
        for horizon in _positive_horizons(horizons)
    ]


def summarize_label_coverage(
    labels: pd.DataFrame,
    *,
    horizons: tuple[int, ...] | list[int],
) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "row_count": len(labels),
        "security_id_count": labels["security_id"].nunique(),
        "ticker_count": labels["ticker"].nunique(),
        "min_date": labels["date"].min(),
        "max_date": labels["date"].max(),
    }

    for horizon in _positive_horizons(horizons):
        label_col = f"label_fwd_ret_{horizon}d"
        has_label_col = f"has_label_fwd_ret_{horizon}d"

        summary[f"{label_col}_non_null"] = int(labels[label_col].notna().sum())
        summary[f"{has_label_col}_true"] = int(labels[has_label_col].sum())

    return summary