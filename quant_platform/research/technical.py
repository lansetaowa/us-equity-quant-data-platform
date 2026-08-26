from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import numpy as np
import pandas as pd
import talib


def _mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value

    return {}


def _int_windows(
    config: Mapping[str, Any],
    section_name: str,
    default: list[int],
) -> list[int]:
    section = _mapping(config.get(section_name))
    values = section.get("windows", default)

    if not isinstance(values, list):
        raise ValueError(f"technical.{section_name}.windows must be a list")

    output = [int(value) for value in values]

    if any(value <= 0 for value in output):
        raise ValueError(f"technical.{section_name}.windows must be positive")

    return output


def _float_array(series: pd.Series) -> np.ndarray:
    return pd.to_numeric(
        series,
        errors="coerce",
    ).to_numpy(dtype=float)


def _safe_divide(
    numerator: np.ndarray,
    denominator: np.ndarray,
) -> np.ndarray:
    result = np.full_like(
        numerator,
        np.nan,
        dtype=float,
    )

    mask = (
        np.isfinite(numerator)
        & np.isfinite(denominator)
        & (denominator != 0)
    )

    np.divide(
        numerator,
        denominator,
        out=result,
        where=mask,
    )

    return result


def talib_pattern_functions() -> list[str]:
    groups = talib.get_function_groups()
    patterns = groups.get("Pattern Recognition", [])

    return sorted(str(pattern) for pattern in patterns)


def _candlestick_column_name(function_name: str) -> str:
    normalized = function_name.strip().lower()

    if normalized.startswith("cdl"):
        normalized = normalized.removeprefix("cdl")

    return f"cdl_{normalized}"


def selected_candlestick_patterns(
    technical_config: Mapping[str, Any],
) -> list[str]:
    pattern_config = _mapping(
        technical_config.get("candlestick_patterns")
    )

    mode = str(
        pattern_config.get("mode", "selected")
    ).strip()

    available = set(talib_pattern_functions())

    if mode == "all_talib_patterns":
        return sorted(available)

    selected = pattern_config.get("selected", [])

    if not isinstance(selected, list):
        raise ValueError("technical.candlestick_patterns.selected must be a list")

    output = [str(value).strip().upper() for value in selected]

    missing = sorted(set(output) - available)

    if missing:
        raise ValueError(f"Unknown TA-Lib candlestick patterns: {missing}")

    return output


def _add_talib_to_one_symbol(
    group: pd.DataFrame,
    *,
    technical_config: Mapping[str, Any],
    include_candlestick_patterns: bool,
) -> pd.DataFrame:
    output = group.sort_values("date").copy()

    open_ = _float_array(output["adj_open"])
    high = _float_array(output["adj_high"])
    low = _float_array(output["adj_low"])
    close = _float_array(output["adj_close"])
    volume = _float_array(output["adj_volume"])

    for window in _int_windows(technical_config, "rsi", [14]):
        output[f"tech_rsi_{window}"] = talib.RSI(
            close,
            timeperiod=window,
        )

    for window in _int_windows(technical_config, "mfi", [14]):
        output[f"tech_mfi_{window}"] = talib.MFI(
            high,
            low,
            close,
            volume,
            timeperiod=window,
        )

    for window in _int_windows(technical_config, "atr", [14]):
        atr = talib.ATR(
            high,
            low,
            close,
            timeperiod=window,
        )
        output[f"tech_atr_{window}"] = atr
        output[f"tech_atr_{window}_norm"] = _safe_divide(
            atr,
            close,
        )

    macd_config = _mapping(technical_config.get("macd"))
    fast_period = int(macd_config.get("fast_period", 12))
    slow_period = int(macd_config.get("slow_period", 26))
    signal_period = int(macd_config.get("signal_period", 9))

    macd, macd_signal, macd_hist = talib.MACD(
        close,
        fastperiod=fast_period,
        slowperiod=slow_period,
        signalperiod=signal_period,
    )

    macd_suffix = f"{fast_period}_{slow_period}_{signal_period}"

    output[f"tech_macd_{fast_period}_{slow_period}"] = macd
    output[f"tech_macd_signal_{macd_suffix}"] = macd_signal
    output[f"tech_macd_hist_{macd_suffix}"] = macd_hist
    output[f"tech_macd_hist_{macd_suffix}_norm"] = _safe_divide(
        macd_hist,
        close,
    )

    bollinger_config = _mapping(technical_config.get("bollinger"))
    bb_window = int(bollinger_config.get("window", 20))
    num_std_up = float(bollinger_config.get("num_std_up", 2.0))
    num_std_down = float(bollinger_config.get("num_std_down", 2.0))

    bb_upper, bb_middle, bb_lower = talib.BBANDS(
        close,
        timeperiod=bb_window,
        nbdevup=num_std_up,
        nbdevdn=num_std_down,
        matype=0,
    )

    output[f"tech_bb_upper_{bb_window}"] = bb_upper
    output[f"tech_bb_middle_{bb_window}"] = bb_middle
    output[f"tech_bb_lower_{bb_window}"] = bb_lower
    output[f"tech_bb_width_{bb_window}"] = _safe_divide(
        bb_upper - bb_lower,
        bb_middle,
    )
    output[f"tech_bb_position_{bb_window}"] = _safe_divide(
        close - bb_lower,
        bb_upper - bb_lower,
    )

    for window in _int_windows(technical_config, "tema", [20, 50]):
        tema = talib.TEMA(
            close,
            timeperiod=window,
        )
        output[f"tech_tema_{window}"] = tema
        output[f"tech_tema_{window}_ratio"] = (
            _safe_divide(
                close,
                tema,
            )
            - 1
        )

    for window in _int_windows(technical_config, "adx", [14]):
        adx = talib.ADX(
            high,
            low,
            close,
            timeperiod=window,
        )
        plus_di = talib.PLUS_DI(
            high,
            low,
            close,
            timeperiod=window,
        )
        minus_di = talib.MINUS_DI(
            high,
            low,
            close,
            timeperiod=window,
        )

        output[f"tech_adx_{window}"] = adx
        output[f"tech_plus_di_{window}"] = plus_di
        output[f"tech_minus_di_{window}"] = minus_di
        output[f"tech_di_spread_{window}"] = plus_di - minus_di

    for window in _int_windows(technical_config, "cmo", [14]):
        output[f"tech_cmo_{window}"] = talib.CMO(
            close,
            timeperiod=window,
        )

    ultosc_config = _mapping(
        technical_config.get("ultimate_oscillator")
    )
    ultosc_1 = int(ultosc_config.get("timeperiod1", 7))
    ultosc_2 = int(ultosc_config.get("timeperiod2", 14))
    ultosc_3 = int(ultosc_config.get("timeperiod3", 28))

    output[f"tech_ultosc_{ultosc_1}_{ultosc_2}_{ultosc_3}"] = talib.ULTOSC(
        high,
        low,
        close,
        timeperiod1=ultosc_1,
        timeperiod2=ultosc_2,
        timeperiod3=ultosc_3,
    )

    bop_config = _mapping(technical_config.get("bop"))

    if bool(bop_config.get("enabled", True)):
        output["tech_bop"] = talib.BOP(
            open_,
            high,
            low,
            close,
        )

    if include_candlestick_patterns:
        for pattern in selected_candlestick_patterns(technical_config):
            function = getattr(talib, pattern)
            values = function(
                open_,
                high,
                low,
                close,
            )
            output[_candlestick_column_name(pattern)] = (
                pd.Series(values, index=output.index)
                .fillna(0)
                .astype("int16")
            )

    return output


def add_talib_technical_features(
    frame: pd.DataFrame,
    *,
    group_columns: list[str],
    technical_config: Mapping[str, Any],
    include_candlestick_patterns: bool,
) -> pd.DataFrame:
    required = {
        "date",
        "adj_open",
        "adj_high",
        "adj_low",
        "adj_close",
        "adj_volume",
    }
    missing = sorted(required - set(frame.columns))

    if missing:
        raise ValueError(f"Price frame missing technical columns: {missing}")

    working = frame.copy()
    working["_technical_original_order"] = range(len(working))

    pieces: list[pd.DataFrame] = []

    for _, group in working.groupby(
        group_columns,
        sort=False,
        group_keys=False,
    ):
        pieces.append(
            _add_talib_to_one_symbol(
                group,
                technical_config=technical_config,
                include_candlestick_patterns=include_candlestick_patterns,
            )
        )

    if not pieces:
        return working.drop(columns=["_technical_original_order"])

    output = pd.concat(
        pieces,
        ignore_index=True,
    )

    output = output.sort_values("_technical_original_order").drop(
        columns=["_technical_original_order"]
    )

    return output.reset_index(drop=True)


def candlestick_feature_columns(columns: list[str]) -> list[str]:
    return sorted(column for column in columns if column.startswith("cdl_"))


def technical_feature_columns(columns: list[str]) -> list[str]:
    return sorted(column for column in columns if column.startswith("tech_"))