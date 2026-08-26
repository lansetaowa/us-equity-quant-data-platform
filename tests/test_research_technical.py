from __future__ import annotations

import pandas as pd

from quant_platform.research.technical import (
    add_talib_technical_features,
    candlestick_feature_columns,
    technical_feature_columns,
)


def sample_price_frame(periods: int = 120) -> pd.DataFrame:
    dates = pd.bdate_range(
        "2025-01-01",
        periods=periods,
    )

    rows = []

    for i, current_date in enumerate(dates):
        close = 100.0 + i * 0.2

        rows.append(
            {
                "security_id": "tiingo:AAPL",
                "ticker": "AAPL",
                "date": current_date.date(),
                "adj_open": close - 0.4,
                "adj_high": close + 1.0,
                "adj_low": close - 1.0,
                "adj_close": close,
                "adj_volume": 1_000_000 + i,
            }
        )

    return pd.DataFrame(rows)


def technical_config() -> dict:
    return {
        "backend": "talib",
        "compute_candlestick_patterns": True,
        "rsi": {"windows": [14]},
        "mfi": {"windows": [14]},
        "atr": {"windows": [14]},
        "macd": {
            "fast_period": 12,
            "slow_period": 26,
            "signal_period": 9,
        },
        "bollinger": {
            "window": 20,
            "num_std_up": 2.0,
            "num_std_down": 2.0,
        },
        "tema": {"windows": [20, 50]},
        "adx": {"windows": [14]},
        "cmo": {"windows": [14]},
        "ultimate_oscillator": {
            "timeperiod1": 7,
            "timeperiod2": 14,
            "timeperiod3": 28,
        },
        "bop": {"enabled": True},
        "candlestick_patterns": {
            "mode": "selected",
            "selected": [
                "CDLENGULFING",
                "CDLHAMMER",
                "CDLDOJI",
            ],
        },
    }


def test_add_talib_technical_features_with_selected_patterns():
    frame = sample_price_frame()

    output = add_talib_technical_features(
        frame,
        group_columns=["security_id"],
        technical_config=technical_config(),
        include_candlestick_patterns=True,
    )

    expected_columns = [
        "tech_rsi_14",
        "tech_mfi_14",
        "tech_atr_14",
        "tech_atr_14_norm",
        "tech_macd_12_26",
        "tech_macd_signal_12_26_9",
        "tech_macd_hist_12_26_9",
        "tech_macd_hist_12_26_9_norm",
        "tech_bb_upper_20",
        "tech_bb_middle_20",
        "tech_bb_lower_20",
        "tech_bb_width_20",
        "tech_bb_position_20",
        "tech_tema_20",
        "tech_tema_20_ratio",
        "tech_tema_50",
        "tech_tema_50_ratio",
        "tech_adx_14",
        "tech_plus_di_14",
        "tech_minus_di_14",
        "tech_di_spread_14",
        "tech_cmo_14",
        "tech_ultosc_7_14_28",
        "tech_bop",
        "cdl_engulfing",
        "cdl_hammer",
        "cdl_doji",
    ]

    for column in expected_columns:
        assert column in output.columns

    assert len(output) == len(frame)
    assert output["tech_rsi_14"].notna().sum() > 0
    assert output["tech_macd_hist_12_26_9"].notna().sum() > 0

    cdl_columns = candlestick_feature_columns(output.columns.tolist())
    tech_columns = technical_feature_columns(output.columns.tolist())

    assert set(cdl_columns) == {
        "cdl_doji",
        "cdl_engulfing",
        "cdl_hammer",
    }
    assert "tech_rsi_14" in tech_columns


def test_add_talib_technical_features_can_skip_patterns():
    frame = sample_price_frame()

    output = add_talib_technical_features(
        frame,
        group_columns=["security_id"],
        technical_config=technical_config(),
        include_candlestick_patterns=False,
    )

    assert "tech_rsi_14" in output.columns
    assert not candlestick_feature_columns(output.columns.tolist())