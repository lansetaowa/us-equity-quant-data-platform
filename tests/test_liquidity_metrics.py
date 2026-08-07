from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from quant_platform.universe.liquidity import (
    LiquidityFilterConfig,
    build_monthly_liquidity_metrics,
    filter_liquidity_metrics_by_month,
    load_liquidity_build_config,
    write_liquidity_metrics,
)


def sample_prices() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "security_id": "tiingo:AAPL",
                "ticker": "AAPL",
                "date": "2026-06-01",
                "close": 10.0,
                "adj_close": 10.0,
                "volume": 100_000,
                "adj_volume": 100_000,
            },
            {
                "security_id": "tiingo:AAPL",
                "ticker": "AAPL",
                "date": "2026-06-02",
                "close": 12.0,
                "adj_close": 12.0,
                "volume": 200_000,
                "adj_volume": 200_000,
            },
            {
                "security_id": "tiingo:AAPL",
                "ticker": "AAPL",
                "date": "2026-06-03",
                "close": 14.0,
                "adj_close": 14.0,
                "volume": 300_000,
                "adj_volume": 300_000,
            },
            {
                "security_id": "tiingo:TINY",
                "ticker": "TINY",
                "date": "2026-06-01",
                "close": 2.0,
                "adj_close": 2.0,
                "volume": 1_000,
                "adj_volume": 1_000,
            },
            {
                "security_id": "tiingo:TINY",
                "ticker": "TINY",
                "date": "2026-06-02",
                "close": 2.1,
                "adj_close": 2.1,
                "volume": 1_000,
                "adj_volume": 1_000,
            },
        ]
    )


def filters() -> LiquidityFilterConfig:
    return LiquidityFilterConfig(
        min_median_close=5.0,
        min_trading_day_coverage=0.0,
        min_median_dollar_volume=1_000_000.0,
        require_positive_volume_days=True,
    )


def test_build_monthly_liquidity_metrics_calculates_filters():
    metrics = build_monthly_liquidity_metrics(
        sample_prices(),
        calendar_name="XNYS",
        filters=filters(),
        include_incomplete_months=True,
    )

    assert set(metrics["ticker"]) == {"AAPL", "TINY"}

    by_ticker = metrics.set_index("ticker")

    assert by_ticker.loc["AAPL", "trading_day_count"] == 3
    assert by_ticker.loc["AAPL", "median_close"] == 12.0
    assert by_ticker.loc["AAPL", "median_dollar_volume"] == 2_400_000.0
    assert bool(by_ticker.loc["AAPL", "passes_liquidity_filters"])

    assert by_ticker.loc["TINY", "trading_day_count"] == 2
    assert not bool(by_ticker.loc["TINY", "passes_price_filter"])
    assert not bool(by_ticker.loc["TINY", "passes_liquidity_filters"])


def test_incomplete_months_are_excluded_by_default():
    metrics = build_monthly_liquidity_metrics(
        sample_prices(),
        calendar_name="XNYS",
        filters=filters(),
        include_incomplete_months=False,
    )

    assert metrics.empty


def test_write_liquidity_metrics_partitions(tmp_path):
    metrics = build_monthly_liquidity_metrics(
        sample_prices(),
        calendar_name="XNYS",
        filters=filters(),
        include_incomplete_months=True,
    )

    output_root = tmp_path / "data" / "dws" / "equity_liquidity_monthly"

    written = write_liquidity_metrics(
        metrics,
        output_root,
        overwrite=False,
    )

    assert written == [
        output_root / "year=2026" / "month=06" / "part-000.parquet"
    ]
    assert written[0].exists()

    loaded = pd.read_parquet(written[0])
    assert len(loaded) == len(metrics)

    with pytest.raises(FileExistsError):
        write_liquidity_metrics(
            metrics,
            output_root,
            overwrite=False,
        )


def test_load_liquidity_build_config(tmp_path):
    config_path = tmp_path / "liquid_universe.yml"
    config_path.write_text(
        """
liquid_universe:
  source_price_dwd_root: "data/dwd/equity_price_daily"
  liquidity_monthly_output_root: "data/dws/equity_liquidity_monthly"
  universe_membership_output_root: "data/dwd/universe_membership_monthly"
  calendar: "XNYS"
  include_incomplete_months: false
  filters:
    min_median_close: 5.0
    min_trading_day_coverage: 0.8
    min_median_dollar_volume: 1000000.0
    require_positive_volume_days: true
  ranking:
    liquidity_score_method: "median_dollar_volume"
    lookback_months: 3
  universes:
    - name: "us_liquid_100"
      size: 100
""",
        encoding="utf-8",
    )

    config = load_liquidity_build_config(config_path)

    assert config.source_price_dwd_root == Path("data/dwd/equity_price_daily")
    assert config.liquidity_monthly_output_root == Path(
        "data/dws/equity_liquidity_monthly"
    )
    assert config.calendar == "XNYS"
    assert config.filters.min_median_close == 5.0
    assert config.filters.min_trading_day_coverage == 0.8

def test_write_liquidity_metrics_missing_only_skips_existing_partitions(
    tmp_path,
):
    metrics = build_monthly_liquidity_metrics(
        sample_prices(),
        calendar_name="XNYS",
        filters=filters(),
        include_incomplete_months=True,
    )

    output_root = tmp_path / "data" / "dws" / "equity_liquidity_monthly"

    first_written = write_liquidity_metrics(
        metrics,
        output_root,
        overwrite=False,
    )

    second_written = write_liquidity_metrics(
        metrics,
        output_root,
        missing_only=True,
    )

    assert first_written
    assert second_written == []


def test_write_liquidity_metrics_can_replace_existing_partitions(
    tmp_path,
):
    metrics = build_monthly_liquidity_metrics(
        sample_prices(),
        calendar_name="XNYS",
        filters=filters(),
        include_incomplete_months=True,
    )

    output_root = tmp_path / "data" / "dws" / "equity_liquidity_monthly"

    first_written = write_liquidity_metrics(
        metrics,
        output_root,
        overwrite=False,
    )

    replaced = write_liquidity_metrics(
        metrics,
        output_root,
        replace_existing_partitions=True,
    )

    assert replaced == first_written


def test_filter_liquidity_metrics_by_month():
    metrics = pd.DataFrame(
        {
            "metric_month": [
                "2026-06-01",
                "2026-07-01",
            ],
            "ticker": ["AAPL", "AAPL"],
        }
    )

    filtered = filter_liquidity_metrics_by_month(
        metrics,
        start_month="2026-07",
        end_month="2026-07",
    )

    assert len(filtered) == 1
    assert filtered.loc[0, "metric_month"] == pd.Timestamp(
        "2026-07-01"
    ).date()