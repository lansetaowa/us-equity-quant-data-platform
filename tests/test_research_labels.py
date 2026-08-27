from __future__ import annotations

from pathlib import Path

import pandas as pd

from quant_platform.research.config import (
    NormalizationConfig,
    ResearchPanelConfig,
    TechnicalConfig,
)
from quant_platform.research.labels import (
    build_forward_return_labels,
    expand_label_output_start_month_for_price_report,
    has_label_column_names,
    label_column_names,
    label_read_lookahead_months,
    label_update_lookback_months,
    read_equity_prices_for_label_build,
    summarize_label_coverage,
    write_label_partitions,
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
        label_horizons=(1, 2, 5),
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


def sample_prices(
    *,
    ticker: str = "AAPL",
    security_id: str = "tiingo:AAPL",
    periods: int = 10,
) -> pd.DataFrame:
    dates = pd.bdate_range(
        "2026-01-01",
        periods=periods,
    )

    rows = []

    for i, current_date in enumerate(dates):
        close = 100.0 + i

        rows.append(
            {
                "security_id": security_id,
                "ticker": ticker,
                "date": current_date.date(),
                "adj_close": close,
            }
        )

    return pd.DataFrame(rows)


def test_build_forward_return_labels():
    prices = sample_prices()

    labels = build_forward_return_labels(
        prices,
        label_set="core_v1",
        horizons=(1, 2, 5),
    )

    assert len(labels) == len(prices)
    assert labels["label_set"].unique().tolist() == ["core_v1"]

    expected_columns = [
        "label_fwd_ret_1d",
        "label_fwd_ret_2d",
        "label_fwd_ret_5d",
        "has_label_fwd_ret_1d",
        "has_label_fwd_ret_2d",
        "has_label_fwd_ret_5d",
        "label_fwd_date_1d",
        "label_fwd_date_2d",
        "label_fwd_date_5d",
    ]

    for column in expected_columns:
        assert column in labels.columns

    expected_1d = 101.0 / 100.0 - 1
    expected_5d = 105.0 / 100.0 - 1

    assert abs(labels.loc[0, "label_fwd_ret_1d"] - expected_1d) < 1e-12
    assert abs(labels.loc[0, "label_fwd_ret_5d"] - expected_5d) < 1e-12
    assert labels.loc[0, "has_label_fwd_ret_1d"]
    assert labels.loc[0, "has_label_fwd_ret_5d"]

    assert pd.isna(labels.loc[len(labels) - 1, "label_fwd_ret_1d"])
    assert not labels.loc[len(labels) - 1, "has_label_fwd_ret_1d"]


def test_build_forward_return_labels_groups_by_security():
    prices = pd.concat(
        [
            sample_prices(ticker="AAPL", security_id="tiingo:AAPL", periods=3),
            sample_prices(ticker="MSFT", security_id="tiingo:MSFT", periods=3),
        ],
        ignore_index=True,
    )

    labels = build_forward_return_labels(
        prices,
        label_set="core_v1",
        horizons=(1,),
    )

    assert len(labels) == 6

    grouped_last = labels.groupby("security_id").tail(1)

    assert grouped_last["label_fwd_ret_1d"].isna().all()
    assert not grouped_last["has_label_fwd_ret_1d"].any()


def test_label_month_helpers():
    assert label_read_lookahead_months((1, 2, 5, 24)) >= 4
    assert label_update_lookback_months((1, 2, 5, 24)) >= 3

    assert expand_label_output_start_month_for_price_report(
        "2026-08",
        horizons=(1, 2, 5, 24),
    ) <= pd.Timestamp("2026-06-01").date()


def test_label_column_name_helpers():
    assert label_column_names((2, 1)) == [
        "label_fwd_ret_1d",
        "label_fwd_ret_2d",
    ]
    assert has_label_column_names((2, 1)) == [
        "has_label_fwd_ret_1d",
        "has_label_fwd_ret_2d",
    ]


def test_read_equity_prices_for_label_build_reads_forward_buffer(tmp_path):
    cfg = config(tmp_path)
    root = cfg.price_dwd_root

    january = sample_prices(periods=5)
    february = sample_prices(periods=5)
    february["date"] = pd.bdate_range(
        "2026-02-02",
        periods=5,
    ).date

    jan_path = root / "year=2026" / "month=01" / "part-000.parquet"
    feb_path = root / "year=2026" / "month=02" / "part-000.parquet"

    jan_path.parent.mkdir(parents=True)
    feb_path.parent.mkdir(parents=True)

    january.to_parquet(jan_path, index=False)
    february.to_parquet(feb_path, index=False)

    prices = read_equity_prices_for_label_build(
        price_root=root,
        start_month="2026-01",
        end_month="2026-01",
        horizons=(5,),
    )

    assert pd.to_datetime(prices["date"]).dt.to_period("M").nunique() == 2


def test_write_label_partitions(tmp_path):
    cfg = config(tmp_path)

    labels = build_forward_return_labels(
        sample_prices(periods=30),
        label_set=cfg.label_set,
        horizons=cfg.label_horizons,
    )

    written, selected = write_label_partitions(
        labels,
        config=cfg,
        start_month="2026-01",
        end_month="2026-01",
        replace_existing_partitions=True,
    )

    assert len(written) == 1
    assert not selected.empty
    assert written[0].exists()
    assert "label_set=core_v1" in written[0].as_posix()
    assert "year=2026" in written[0].as_posix()
    assert "month=01" in written[0].as_posix()

    loaded = pd.read_parquet(written[0])

    assert "label_fwd_ret_1d" in loaded.columns
    assert loaded["label_set"].unique().tolist() == ["core_v1"]


def test_summarize_label_coverage():
    labels = build_forward_return_labels(
        sample_prices(periods=10),
        label_set="core_v1",
        horizons=(1, 5),
    )

    summary = summarize_label_coverage(
        labels,
        horizons=(1, 5),
    )

    assert summary["row_count"] == 10
    assert summary["label_fwd_ret_1d_non_null"] == 9
    assert summary["label_fwd_ret_5d_non_null"] == 5