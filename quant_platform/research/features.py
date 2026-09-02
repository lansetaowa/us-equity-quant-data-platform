from __future__ import annotations

import math
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from quant_platform.research.config import (
    ResearchPanelConfig,
    TechnicalConfig,
)
from quant_platform.research.io import (
    filter_frame_by_month,
    read_parquet_dataset_by_month,
    subtract_months,
    write_monthly_partitions,
)
from quant_platform.research.technical import add_talib_technical_features

REQUIRED_PRICE_COLUMNS = {
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
}


def _prepare_price_frame(
    prices: pd.DataFrame,
    *,
    extra_required_columns: set[str] | None = None,
) -> pd.DataFrame:
    required = set(REQUIRED_PRICE_COLUMNS)

    if extra_required_columns:
        required |= set(extra_required_columns)

    missing = sorted(required - set(prices.columns))

    if missing:
        raise ValueError(f"Price frame missing columns: {missing}")

    output = prices.copy()
    output["ticker"] = output["ticker"].astype(str).str.upper().str.strip()
    output["security_id"] = output["security_id"].astype(str).str.strip()
    output["date"] = pd.to_datetime(output["date"], errors="coerce").dt.date

    if output["date"].isna().any():
        raise ValueError("Price frame contains invalid dates")

    numeric_columns = [
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
    ]

    for column in numeric_columns:
        if column in output.columns:
            output[column] = pd.to_numeric(
                output[column],
                errors="coerce",
            )

    if output["adj_close"].isna().any() or (output["adj_close"] <= 0).any():
        raise ValueError("Price frame contains invalid adj_close values")

    if output["adj_volume"].isna().any() or (output["adj_volume"] < 0).any():
        raise ValueError("Price frame contains invalid adj_volume values")

    return output.sort_values(["security_id", "date"]).reset_index(drop=True)


def _rolling_mean(
    groupby: Any,
    column: str,
    window: int,
) -> pd.Series:
    return groupby[column].transform(
        lambda series: series.rolling(
            window,
            min_periods=window,
        ).mean()
    )


def _rolling_std(
    groupby: Any,
    column: str,
    window: int,
) -> pd.Series:
    return groupby[column].transform(
        lambda series: series.rolling(
            window,
            min_periods=window,
        ).std()
    )


def _rolling_skew(
    groupby: Any,
    column: str,
    window: int,
) -> pd.Series:
    return groupby[column].transform(
        lambda series: series.rolling(
            window,
            min_periods=window,
        ).skew()
    )


def _rolling_kurt(
    groupby: Any,
    column: str,
    window: int,
) -> pd.Series:
    return groupby[column].transform(
        lambda series: series.rolling(
            window,
            min_periods=window,
        ).kurt()
    )


def _rolling_max(
    groupby: Any,
    column: str,
    window: int,
) -> pd.Series:
    return groupby[column].transform(
        lambda series: series.rolling(
            window,
            min_periods=window,
        ).max()
    )


def _rolling_min(
    groupby: Any,
    column: str,
    window: int,
) -> pd.Series:
    return groupby[column].transform(
        lambda series: series.rolling(
            window,
            min_periods=window,
        ).min()
    )


def _pct_change(
    groupby: Any,
    column: str,
    periods: int,
) -> pd.Series:
    return groupby[column].transform(
        lambda series: series / series.shift(periods) - 1
    )


def feature_lookback_days(
    rolling_windows: dict[str, Any],
) -> int:
    lookback_candidates: list[int] = []

    for key in [
        "returns",
        "momentum",
        "reversal",
        "volatility",
        "dollar_volume",
        "price_position",
        "sma",
    ]:
        values = rolling_windows.get(key, [])

        if values:
            lookback_candidates.extend(int(value) for value in values)

    for long_window, skip_window in rolling_windows.get(
        "skip_recent_momentum",
        [],
    ):
        lookback_candidates.extend([int(long_window), int(skip_window)])

    if not lookback_candidates:
        raise ValueError("No feature lookback windows configured")

    return max(lookback_candidates)


def feature_read_lookback_months(
    rolling_windows: dict[str, Any],
) -> int:
    lookback_days = feature_lookback_days(rolling_windows)
    return_lags = [
        int(value)
        for value in rolling_windows.get("return_lag_multiples", [])
    ]
    max_lag = max([0, *return_lags])

    # Approximate 21 trading days per month plus a safety buffer.
    return math.ceil((lookback_days + max_lag) / 21) + 2


def _ordered_columns(
    columns: pd.Index,
    leading_columns: list[str],
) -> list[str]:
    leading_set = set(leading_columns)

    return [
        *leading_columns,
        *[column for column in columns if column not in leading_set],
    ]


def _add_core_features(
    working: pd.DataFrame,
    *,
    group_columns: list[str],
    rolling_windows: dict[str, Any],
) -> pd.DataFrame:
    grouped = working.groupby(group_columns, sort=False, group_keys=False)

    return_windows = sorted(set(rolling_windows["returns"]))
    annualization_days = int(rolling_windows.get("annualization_days", 252))

    if 1 not in return_windows:
        raise ValueError("returns window config must include 1")

    working["dollar_volume"] = working["adj_close"] * working["adj_volume"]
    working["log_dollar_volume"] = np.log1p(
        working["dollar_volume"].clip(lower=0)
    )

    for window in return_windows:
        working[f"ret_{window}d"] = _pct_change(
            grouped,
            "adj_close",
            window,
        )

    for window in return_windows:
        ret_col = f"ret_{window}d"

        for lag in sorted(set(rolling_windows.get("return_lag_multiples", []))):
            working[f"{ret_col}_lag{lag}"] = grouped[ret_col].shift(lag)

    for window in sorted(set(rolling_windows.get("momentum", []))):
        ret_col = f"ret_{window}d"

        if ret_col not in working.columns:
            raise ValueError(
                f"Momentum window {window} requires missing column {ret_col}"
            )

        working[f"mom_{window}d"] = working[ret_col]

    for long_window, skip_window in rolling_windows.get(
        "skip_recent_momentum",
        [],
    ):
        column_name = f"mom_{long_window}_{skip_window}d"

        working[column_name] = grouped["adj_close"].transform(
            lambda series: (
                series.shift(skip_window) / series.shift(long_window) - 1
            )
        )

    for window in sorted(set(rolling_windows.get("reversal", []))):
        ret_col = f"ret_{window}d"

        if ret_col not in working.columns:
            raise ValueError(
                f"Reversal window {window} requires missing column {ret_col}"
            )

        working[f"rev_{window}d"] = -working[ret_col]

    for window in sorted(set(rolling_windows["volatility"])):
        rolling_std = _rolling_std(grouped, "ret_1d", window)

        working[f"realized_vol_{window}d"] = rolling_std * math.sqrt(
            annualization_days
        )
        working[f"rolling_std_ret_{window}d"] = rolling_std
        working[f"rolling_skew_ret_{window}d"] = _rolling_skew(
            grouped,
            "ret_1d",
            window,
        )
        working[f"rolling_kurt_ret_{window}d"] = _rolling_kurt(
            grouped,
            "ret_1d",
            window,
        )
        working[f"rolling_max_ret_{window}d"] = _rolling_max(
            grouped,
            "ret_1d",
            window,
        )
        working[f"rolling_min_ret_{window}d"] = _rolling_min(
            grouped,
            "ret_1d",
            window,
        )

    working["_amihud_component"] = (
        working["ret_1d"].abs()
        / working["dollar_volume"].replace(0, np.nan)
    )

    for window in sorted(set(rolling_windows["dollar_volume"])):
        working[f"avg_dollar_volume_{window}d"] = _rolling_mean(
            grouped,
            "dollar_volume",
            window,
        )
        working[f"log_avg_dollar_volume_{window}d"] = np.log1p(
            working[f"avg_dollar_volume_{window}d"].clip(lower=0)
        )
        working[f"std_log_dollar_volume_{window}d"] = _rolling_std(
            grouped,
            "log_dollar_volume",
            window,
        )
        working[f"amihud_{window}d"] = _rolling_mean(
            grouped,
            "_amihud_component",
            window,
        )

    for window in sorted(set(rolling_windows["price_position"])):
        rolling_min = _rolling_min(grouped, "adj_close", window)
        rolling_max = _rolling_max(grouped, "adj_close", window)
        denominator = (rolling_max - rolling_min).replace(0, np.nan)

        working[f"price_position_{window}d"] = (
            working["adj_close"] - rolling_min
        ) / denominator

    for window in sorted(set(rolling_windows.get("sma", []))):
        working[f"sma_{window}_ratio"] = (
            working["adj_close"]
            / grouped["adj_close"].transform(
                lambda series: series.rolling(
                    window,
                    min_periods=window,
                ).mean()
            )
            - 1
        )

    parsed_dates = pd.to_datetime(working["date"], errors="coerce")
    working["day_of_week"] = parsed_dates.dt.dayofweek
    working["month"] = parsed_dates.dt.month
    working["quarter"] = parsed_dates.dt.quarter
    working["is_month_start"] = parsed_dates.dt.is_month_start
    working["is_month_end"] = parsed_dates.dt.is_month_end

    working["feature_lookback_days"] = feature_lookback_days(rolling_windows)
    working["created_at_utc"] = datetime.now(UTC).isoformat()

    return working.drop(columns=["_amihud_component"])


def build_equity_core_features(
    prices: pd.DataFrame,
    *,
    config: ResearchPanelConfig,
) -> pd.DataFrame:
    working = _prepare_price_frame(prices)
    working["factor_set"] = config.factor_set

    working = _add_core_features(
        working,
        group_columns=["security_id"],
        rolling_windows=config.rolling_windows,
    )

    working = add_talib_technical_features(
        working,
        group_columns=["security_id"],
        technical_config=config.technical.raw,
        include_candlestick_patterns=bool(
            config.technical.raw.get("compute_candlestick_patterns", True)
        ),
    )

    leading_columns = [
        "date",
        "security_id",
        "ticker",
        "factor_set",
    ]

    ordered = _ordered_columns(working.columns, leading_columns)

    return working.loc[:, ordered].sort_values(
        ["date", "ticker", "security_id"]
    ).reset_index(drop=True)


def build_market_context_core_features(
    prices: pd.DataFrame,
    *,
    context_set: str,
    rolling_windows: dict[str, Any],
    technical: TechnicalConfig | None = None,
) -> pd.DataFrame:
    working = _prepare_price_frame(
        prices,
        extra_required_columns={"context_set", "context_group"},
    )

    working["context_set"] = working["context_set"].astype(str).str.strip()
    working["context_group"] = (
        working["context_group"].astype(str).str.strip()
    )

    working = working[working["context_set"] == context_set].copy()

    if working.empty:
        raise ValueError(f"No market context prices for {context_set}")

    working = _add_core_features(
        working,
        group_columns=["context_set", "security_id"],
        rolling_windows=rolling_windows,
    )

    if technical is not None:
        market_context_config_raw = technical.raw.get("market_context", {})
        market_context_config = (
            market_context_config_raw
            if isinstance(market_context_config_raw, dict)
            else {}
        )

        include_candlestick_patterns = bool(
            market_context_config.get("include_candlestick_patterns", False)
        )

        working = add_talib_technical_features(
            working,
            group_columns=["context_set", "security_id"],
            technical_config=technical.raw,
            include_candlestick_patterns=include_candlestick_patterns,
        )

    leading_columns = [
        "date",
        "context_set",
        "context_group",
        "security_id",
        "ticker",
    ]

    ordered = _ordered_columns(working.columns, leading_columns)

    return working.loc[:, ordered].sort_values(
        ["date", "context_group", "ticker"]
    ).reset_index(drop=True)


def _context_price_root(
    *,
    price_root: str | Path,
    context_set: str,
) -> Path:
    root = Path(price_root)
    context_part = f"context_set={context_set}"

    if root.name == context_part:
        return root

    return root / context_part


def read_equity_prices_for_feature_build(
    *,
    price_root: str | Path,
    start_month: str | date | None,
    end_month: str | date | None,
    rolling_windows: dict[str, Any],
) -> pd.DataFrame:
    read_start_month = (
        subtract_months(
            start_month,
            feature_read_lookback_months(rolling_windows),
        )
        if start_month is not None
        else None
    )

    return read_parquet_dataset_by_month(
        price_root,
        date_column="date",
        start_month=read_start_month,
        end_month=end_month,
    )


def read_market_context_prices_for_feature_build(
    *,
    price_root: str | Path,
    context_set: str,
    start_month: str | date | None,
    end_month: str | date | None,
    rolling_windows: dict[str, Any],
) -> pd.DataFrame:
    root = _context_price_root(
        price_root=price_root,
        context_set=context_set,
    )

    read_start_month = (
        subtract_months(
            start_month,
            feature_read_lookback_months(rolling_windows),
        )
        if start_month is not None
        else None
    )

    return read_parquet_dataset_by_month(
        root,
        date_column="date",
        start_month=read_start_month,
        end_month=end_month,
    )


def load_ever_member_security_ids(
    *,
    membership_root: str | Path,
    universe_name: str,
) -> set[str]:
    root = Path(membership_root)

    if not root.exists():
        raise FileNotFoundError(f"Universe membership root not found: {root}")

    files = sorted(root.rglob("*.parquet"))

    if not files:
        raise FileNotFoundError(f"No membership files found under {root}")

    frame = pd.concat(
        [pd.read_parquet(path) for path in files],
        ignore_index=True,
    )

    required = {"universe_name", "security_id"}
    missing = required - set(frame.columns)

    if missing:
        raise ValueError(f"Universe membership missing columns: {missing}")

    selected = frame[frame["universe_name"] == universe_name].copy()

    if selected.empty:
        raise ValueError(f"No membership rows for {universe_name}")

    return set(selected["security_id"].astype(str).str.strip())


def filter_to_security_ids(
    frame: pd.DataFrame,
    security_ids: set[str],
) -> pd.DataFrame:
    output = frame.copy()
    output["security_id"] = output["security_id"].astype(str).str.strip()

    return output[output["security_id"].isin(security_ids)].reset_index(
        drop=True
    )


def write_equity_feature_partitions(
    features: pd.DataFrame,
    *,
    config: ResearchPanelConfig,
    start_month: str | date | None = None,
    end_month: str | date | None = None,
    overwrite: bool = False,
    replace_existing_partitions: bool = False,
) -> tuple[list[Path], pd.DataFrame]:
    selected = filter_frame_by_month(
        features,
        date_column="date",
        start_month=start_month,
        end_month=end_month,
    )

    root = config.feature_output_root / f"factor_set={config.factor_set}"

    written = write_monthly_partitions(
        selected,
        output_root=root,
        date_column="date",
        overwrite=overwrite,
        replace_existing_partitions=replace_existing_partitions,
    )

    return written, selected


def write_market_context_feature_partitions(
    features: pd.DataFrame,
    *,
    output_root: str | Path,
    context_set: str,
    start_month: str | date | None = None,
    end_month: str | date | None = None,
    overwrite: bool = False,
    replace_existing_partitions: bool = False,
) -> tuple[list[Path], pd.DataFrame]:
    selected = filter_frame_by_month(
        features,
        date_column="date",
        start_month=start_month,
        end_month=end_month,
    )

    root = Path(output_root) / f"context_set={context_set}"

    written = write_monthly_partitions(
        selected,
        output_root=root,
        date_column="date",
        overwrite=overwrite,
        replace_existing_partitions=replace_existing_partitions,
    )

    return written, selected