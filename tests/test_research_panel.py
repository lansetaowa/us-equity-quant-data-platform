from __future__ import annotations

from pathlib import Path

import pandas as pd

from quant_platform.research.config import (
    NormalizationConfig,
    ResearchPanelConfig,
    TechnicalConfig,
)
from quant_platform.research.panel import (
    build_equity_research_panel,
    build_market_context_wide_features,
    summarize_panel,
    write_panel_partitions,
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
            "returns": [1, 5, 21],
            "return_lag_multiples": [1],
            "momentum": [21],
            "reversal": [1, 5],
            "volatility": [21],
            "dollar_volume": [20],
            "price_position": [252],
            "sma": [20],
            "skip_recent_momentum": [(252, 21)],
            "annualization_days": 252,
        },
        label_horizons=(1, 2, 5, 10, 21, 63),
        technical=TechnicalConfig(
            backend="talib",
            raw={"backend": "talib"},
        ),
        normalization=NormalizationConfig(
            winsorize_lower=0.01,
            winsorize_upper=0.99,
            min_cross_section_count=50,
        ),
        composites={},
    )


def features() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": pd.Timestamp("2026-08-03").date(),
                "security_id": "tiingo:AAPL",
                "ticker": "AAPL",
                "factor_set": "core_v1",
                "ret_1d": 0.01,
                "ret_5d": 0.03,
                "mom_21d": 0.05,
                "tech_rsi_14": 60.0,
            },
            {
                "date": pd.Timestamp("2026-08-03").date(),
                "security_id": "tiingo:MSFT",
                "ticker": "MSFT",
                "factor_set": "core_v1",
                "ret_1d": 0.02,
                "ret_5d": 0.04,
                "mom_21d": 0.06,
                "tech_rsi_14": 55.0,
            },
            {
                "date": pd.Timestamp("2026-09-02").date(),
                "security_id": "tiingo:AAPL",
                "ticker": "AAPL",
                "factor_set": "core_v1",
                "ret_1d": 0.03,
                "ret_5d": 0.05,
                "mom_21d": 0.07,
                "tech_rsi_14": 58.0,
            },
        ]
    )


def labels() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": pd.Timestamp("2026-08-03").date(),
                "security_id": "tiingo:AAPL",
                "ticker": "AAPL",
                "label_set": "core_v1",
                "label_fwd_ret_1d": 0.015,
                "has_label_fwd_ret_1d": True,
            },
            {
                "date": pd.Timestamp("2026-08-03").date(),
                "security_id": "tiingo:MSFT",
                "ticker": "MSFT",
                "label_set": "core_v1",
                "label_fwd_ret_1d": 0.025,
                "has_label_fwd_ret_1d": True,
            },
            {
                "date": pd.Timestamp("2026-09-02").date(),
                "security_id": "tiingo:AAPL",
                "ticker": "AAPL",
                "label_set": "core_v1",
                "label_fwd_ret_1d": 0.035,
                "has_label_fwd_ret_1d": True,
            },
        ]
    )


def membership() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "universe_name": "us_liquid_500",
                "membership_month": pd.Timestamp("2026-08-01").date(),
                "effective_start_date": pd.Timestamp("2026-08-01").date(),
                "effective_end_date": pd.Timestamp("2026-09-01").date(),
                "security_id": "tiingo:AAPL",
                "ticker": "AAPL",
                "rank": 1,
                "source_metric_month": pd.Timestamp("2026-07-01").date(),
                "liquidity_score": 100.0,
            },
            {
                "universe_name": "us_liquid_500",
                "membership_month": pd.Timestamp("2026-08-01").date(),
                "effective_start_date": pd.Timestamp("2026-08-01").date(),
                "effective_end_date": pd.Timestamp("2026-09-01").date(),
                "security_id": "tiingo:MSFT",
                "ticker": "MSFT",
                "rank": 2,
                "source_metric_month": pd.Timestamp("2026-07-01").date(),
                "liquidity_score": 90.0,
            },
            {
                "universe_name": "us_liquid_500",
                "membership_month": pd.Timestamp("2026-09-01").date(),
                "effective_start_date": pd.Timestamp("2026-09-01").date(),
                "effective_end_date": pd.Timestamp("2026-10-01").date(),
                "security_id": "tiingo:AAPL",
                "ticker": "AAPL",
                "rank": 1,
                "source_metric_month": pd.Timestamp("2026-08-01").date(),
                "liquidity_score": 101.0,
            },
        ]
    )


def market_context_features() -> pd.DataFrame:
    rows = []

    for current_date in [
        pd.Timestamp("2026-08-03").date(),
        pd.Timestamp("2026-09-02").date(),
    ]:
        rows.append(
            {
                "date": current_date,
                "context_set": "core_v1",
                "context_group": "broad_market",
                "security_id": "tiingo:SPY",
                "ticker": "SPY",
                "ret_1d": 0.005,
                "ret_5d": 0.01,
                "tech_rsi_14": 50.0,
            }
        )
        rows.append(
            {
                "date": current_date,
                "context_set": "core_v1",
                "context_group": "growth",
                "security_id": "tiingo:QQQ",
                "ticker": "QQQ",
                "ret_1d": 0.006,
                "ret_5d": 0.012,
                "tech_rsi_14": 52.0,
            }
        )

    return pd.DataFrame(rows)


def test_build_market_context_wide_features():
    wide = build_market_context_wide_features(market_context_features())

    assert len(wide) == 2
    assert "mkt_spy_ret_1d" in wide.columns
    assert "mkt_qqq_ret_5d" in wide.columns
    assert "mkt_spy_tech_rsi_14" in wide.columns


def test_build_equity_research_panel_point_in_time():
    panel = build_equity_research_panel(
        features=features(),
        labels=labels(),
        market_context_features=market_context_features(),
        membership=membership(),
        universe_name="us_liquid_500",
        factor_set="core_v1",
        label_set="core_v1",
    )

    assert len(panel) == 3
    assert set(panel["ticker"]) == {"AAPL", "MSFT"}
    assert set(panel["universe_name"]) == {"us_liquid_500"}
    assert "label_fwd_ret_1d" in panel.columns
    assert "mkt_spy_ret_1d" in panel.columns
    assert "mkt_qqq_ret_5d" in panel.columns
    assert "excess_ret_1d_vs_spy" in panel.columns

    august = panel[panel["membership_month"] == pd.Timestamp("2026-08-01").date()]
    september = panel[
        panel["membership_month"] == pd.Timestamp("2026-09-01").date()
    ]

    assert set(august["ticker"]) == {"AAPL", "MSFT"}
    assert set(september["ticker"]) == {"AAPL"}


def test_summarize_panel():
    panel = build_equity_research_panel(
        features=features(),
        labels=labels(),
        market_context_features=market_context_features(),
        membership=membership(),
        universe_name="us_liquid_500",
        factor_set="core_v1",
        label_set="core_v1",
    )

    summary = summarize_panel(panel)

    assert summary["row_count"] == 3
    assert summary["duplicate_key_count"] == 0
    assert summary["market_context_column_count"] > 0
    assert summary["label_column_count"] > 0


def test_write_panel_partitions(tmp_path):
    cfg = config(tmp_path)

    panel = build_equity_research_panel(
        features=features(),
        labels=labels(),
        market_context_features=market_context_features(),
        membership=membership(),
        universe_name="us_liquid_500",
        factor_set="core_v1",
        label_set="core_v1",
    )

    written, selected = write_panel_partitions(
        panel,
        config=cfg,
        universe_name="us_liquid_500",
        start_month="2026-08",
        end_month="2026-08",
        replace_existing_partitions=True,
    )

    assert len(written) == 1
    assert not selected.empty
    assert written[0].exists()
    assert "universe_name=us_liquid_500" in written[0].as_posix()
    assert "factor_set=core_v1" in written[0].as_posix()

    loaded = pd.read_parquet(written[0])

    assert set(loaded["ticker"]) == {"AAPL", "MSFT"}