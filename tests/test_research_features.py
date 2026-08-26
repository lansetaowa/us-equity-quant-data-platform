from __future__ import annotations

from pathlib import Path

import pandas as pd

from quant_platform.research.config import (
    NormalizationConfig,
    ResearchPanelConfig,
    TechnicalConfig,
)
from quant_platform.research.features import (
    build_equity_core_features,
    build_market_context_core_features,
    feature_lookback_days,
    feature_read_lookback_months,
    filter_to_security_ids,
    write_equity_feature_partitions,
    write_market_context_feature_partitions,
)


def config(tmp_path: Path) -> ResearchPanelConfig:
    return ResearchPanelConfig(
        factor_set="core_v1",
        label_set="core_v1",
        default_universe_name="us_liquid_500",
        price_dwd_root=tmp_path / "data" / "dwd" / "equity_price_daily",
        universe_membership_root=(
            tmp_path
            / "data"
            / "dwd"
            / "universe_membership_monthly"
        ),
        market_context_symbol_path=tmp_path / "symbols.parquet",
        market_context_price_root=(
            tmp_path
            / "data"
            / "dwd"
            / "market_context_price_daily"
        ),
        market_context_feature_root=(
            tmp_path
            / "data"
            / "dws"
            / "market_context_features_daily"
        ),
        feature_output_root=(
            tmp_path
            / "data"
            / "dws"
            / "equity_features_daily"
        ),
        label_output_root=(
            tmp_path
            / "data"
            / "dws"
            / "equity_forward_returns_daily"
        ),
        panel_output_root=(
            tmp_path
            / "data"
            / "ads"
            / "equity_research_panel_daily"
        ),
        feature_scope={
            "use_ever_members": True,
            "universe_name": "us_liquid_500",
        },
        rolling_windows={
            "returns": [1, 2, 4, 5, 12, 21, 24, 48, 63, 72, 126, 252],
            "return_lag_multiples": [1, 2, 3],
            "momentum": [21, 63, 126],
            "reversal": [1, 5, 21],
            "volatility": [21, 63, 126],
            "dollar_volume": [3, 20, 60],
            "price_position": [252],
            "sma": [20, 50, 200],
            "skip_recent_momentum": [(252, 21)],
            "annualization_days": 252,
        },
        label_horizons=(1, 2, 4, 5, 12, 21, 24),
        technical=TechnicalConfig(
            backend="talib",
            raw={
                "backend": "talib",
                "compute_candlestick_patterns": True,
                "market_context": {
                    "include_candlestick_patterns": False,
                },
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
            },
        ),
        normalization=NormalizationConfig(
            winsorize_lower=0.01,
            winsorize_upper=0.99,
            min_cross_section_count=50,
        ),
        composites={},
    )


def sample_prices(
    *,
    ticker: str = "AAPL",
    security_id: str = "tiingo:AAPL",
    periods: int = 270,
) -> pd.DataFrame:
    dates = pd.bdate_range(
        "2025-01-01",
        periods=periods,
    )

    rows = []

    for i, current_date in enumerate(dates):
        close = 100.0 + i * 0.5

        rows.append(
            {
                "security_id": security_id,
                "ticker": ticker,
                "date": current_date.date(),
                "open": close - 0.5,
                "high": close + 1.0,
                "low": close - 1.0,
                "close": close,
                "volume": 1_000_000 + i,
                "adj_open": close - 0.5,
                "adj_high": close + 1.0,
                "adj_low": close - 1.0,
                "adj_close": close,
                "adj_volume": 1_000_000 + i,
                "div_cash": 0.0,
                "split_factor": 1.0,
                "source": "tiingo",
                "load_id": "test",
                "loaded_at": "2026-01-01T00:00:00+00:00",
            }
        )

    return pd.DataFrame(rows)


def test_feature_lookback_is_config_driven(tmp_path):
    cfg = config(tmp_path)

    assert feature_lookback_days(cfg.rolling_windows) == 252
    assert feature_read_lookback_months(cfg.rolling_windows) >= 14


def test_build_equity_core_features(tmp_path):
    cfg = config(tmp_path)

    features = build_equity_core_features(
        sample_prices(),
        config=cfg,
    )

    assert len(features) == 270
    assert features["factor_set"].unique().tolist() == ["core_v1"]

    expected_columns = [
        "ret_1d",
        "ret_2d",
        "ret_4d",
        "ret_5d",
        "ret_21d",
        "ret_252d",
        "ret_21d_lag3",
        "mom_21d",
        "mom_63d",
        "mom_126d",
        "mom_252_21d",
        "rev_1d",
        "rev_5d",
        "rev_21d",
        "realized_vol_21d",
        "rolling_skew_ret_21d",
        "rolling_kurt_ret_21d",
        "avg_dollar_volume_20d",
        "log_avg_dollar_volume_20d",
        "amihud_20d",
        "price_position_252d",
        "sma_20_ratio",
        "sma_50_ratio",
        "sma_200_ratio",
        "day_of_week",
        "tech_rsi_14",
        "tech_mfi_14",
        "tech_atr_14_norm",
        "tech_macd_hist_12_26_9",
        "tech_macd_hist_12_26_9_norm",
        "tech_bb_position_20",
        "tech_tema_20_ratio",
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
        assert column in features.columns

    row_5 = features.iloc[5]
    expected_ret_5d = row_5["adj_close"] / features.iloc[0]["adj_close"] - 1

    assert abs(row_5["ret_5d"] - expected_ret_5d) < 1e-12

    later = features.iloc[-1]

    assert pd.notna(later["ret_252d"])
    assert pd.notna(later["mom_252_21d"])
    assert pd.notna(later["price_position_252d"])
    assert features["tech_rsi_14"].notna().sum() > 0
    assert features["tech_macd_hist_12_26_9"].notna().sum() > 0


def test_filter_to_security_ids():
    prices = pd.concat(
        [
            sample_prices(ticker="AAPL", security_id="tiingo:AAPL", periods=5),
            sample_prices(ticker="MSFT", security_id="tiingo:MSFT", periods=5),
        ],
        ignore_index=True,
    )

    filtered = filter_to_security_ids(
        prices,
        {"tiingo:AAPL"},
    )

    assert set(filtered["ticker"]) == {"AAPL"}


def test_build_market_context_core_features(tmp_path):
    cfg = config(tmp_path)
    prices = sample_prices(ticker="SPY", security_id="tiingo:SPY")
    prices["context_set"] = "core_v1"
    prices["context_group"] = "broad_market"

    features = build_market_context_core_features(
        prices,
        context_set="core_v1",
        rolling_windows=cfg.rolling_windows,
        technical=cfg.technical,
    )

    assert len(features) == len(prices)
    assert features["context_set"].unique().tolist() == ["core_v1"]
    assert features["context_group"].unique().tolist() == ["broad_market"]
    assert "ret_21d" in features.columns
    assert "mom_252_21d" in features.columns
    assert "realized_vol_21d" in features.columns
    assert "sma_200_ratio" in features.columns
    assert "tech_rsi_14" in features.columns
    assert "tech_macd_hist_12_26_9" in features.columns
    assert "tech_adx_14" in features.columns
    assert "tech_bop" in features.columns
    assert not any(column.startswith("cdl_") for column in features.columns)


def test_write_equity_feature_partitions(tmp_path):
    cfg = config(tmp_path)
    features = build_equity_core_features(
        sample_prices(),
        config=cfg,
    )

    written, selected = write_equity_feature_partitions(
        features,
        config=cfg,
        start_month="2025-03",
        end_month="2025-03",
        replace_existing_partitions=True,
    )

    assert len(written) == 1
    assert not selected.empty
    assert written[0].exists()
    assert "factor_set=core_v1" in written[0].as_posix()
    assert "year=2025" in written[0].as_posix()
    assert "month=03" in written[0].as_posix()

    loaded = pd.read_parquet(written[0])

    assert not loaded.empty

    loaded_months = (
        pd.to_datetime(loaded["date"])
        .dt.to_period("M")
        .unique()
        .tolist()
    )

    assert loaded_months == [pd.Period("2025-03", freq="M")]


def test_write_market_context_feature_partitions(tmp_path):
    cfg = config(tmp_path)
    prices = sample_prices(ticker="SPY", security_id="tiingo:SPY")
    prices["context_set"] = "core_v1"
    prices["context_group"] = "broad_market"

    features = build_market_context_core_features(
        prices,
        context_set="core_v1",
        rolling_windows=cfg.rolling_windows,
        technical=cfg.technical,
    )

    written, selected = write_market_context_feature_partitions(
        features,
        output_root=cfg.market_context_feature_root,
        context_set="core_v1",
        start_month="2025-03",
        end_month="2025-03",
        replace_existing_partitions=True,
    )

    assert len(written) == 1
    assert not selected.empty
    assert written[0].exists()
    assert "context_set=core_v1" in written[0].as_posix()

    loaded = pd.read_parquet(written[0])

    assert loaded["context_group"].unique().tolist() == ["broad_market"]