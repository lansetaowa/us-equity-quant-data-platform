from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from quant_platform.universe.membership import (
    UniverseMembershipConfig,
    UniverseSpec,
    build_universe_membership,
    load_universe_membership_config,
    write_universe_membership,
)


def sample_metrics() -> pd.DataFrame:
    rows = []

    for month in [
        "2026-05-01",
        "2026-06-01",
    ]:
        rows.extend(
            [
                {
                    "metric_month": month,
                    "security_id": "tiingo:AAPL",
                    "ticker": "AAPL",
                    "trading_day_count": 21,
                    "expected_trading_days": 21,
                    "trading_day_coverage": 1.0,
                    "median_close": 100.0,
                    "median_dollar_volume": 10_000_000_000.0,
                    "last_price_date": "2026-06-30",
                    "is_complete_month": True,
                    "passes_liquidity_filters": True,
                },
                {
                    "metric_month": month,
                    "security_id": "tiingo:MSFT",
                    "ticker": "MSFT",
                    "trading_day_count": 21,
                    "expected_trading_days": 21,
                    "trading_day_coverage": 1.0,
                    "median_close": 90.0,
                    "median_dollar_volume": 8_000_000_000.0,
                    "last_price_date": "2026-06-30",
                    "is_complete_month": True,
                    "passes_liquidity_filters": True,
                },
                {
                    "metric_month": month,
                    "security_id": "tiingo:TINY",
                    "ticker": "TINY",
                    "trading_day_count": 21,
                    "expected_trading_days": 21,
                    "trading_day_coverage": 1.0,
                    "median_close": 10.0,
                    "median_dollar_volume": 2_000_000.0,
                    "last_price_date": "2026-06-30",
                    "is_complete_month": True,
                    "passes_liquidity_filters": True,
                },
            ]
        )

    rows.append(
        {
            "metric_month": "2026-06-01",
            "security_id": "tiingo:FAIL",
            "ticker": "FAIL",
            "trading_day_count": 21,
            "expected_trading_days": 21,
            "trading_day_coverage": 1.0,
            "median_close": 10.0,
            "median_dollar_volume": 100_000_000_000.0,
            "last_price_date": "2026-06-30",
            "is_complete_month": True,
            "passes_liquidity_filters": False,
        }
    )

    rows.append(
        {
            "metric_month": "2026-07-01",
            "security_id": "tiingo:PARTIAL",
            "ticker": "PARTIAL",
            "trading_day_count": 10,
            "expected_trading_days": 21,
            "trading_day_coverage": 0.5,
            "median_close": 100.0,
            "median_dollar_volume": 100_000_000_000.0,
            "last_price_date": "2026-07-15",
            "is_complete_month": False,
            "passes_liquidity_filters": True,
        }
    )

    return pd.DataFrame(rows)


def config() -> UniverseMembershipConfig:
    return UniverseMembershipConfig(
        liquidity_monthly_output_root=Path("data/dws/equity_liquidity_monthly"),
        universe_membership_output_root=Path(
            "data/dwd/universe_membership_monthly"
        ),
        liquidity_score_method="median_dollar_volume",
        score_aggregation="median",
        lookback_months=2,
        universes=(
            UniverseSpec(name="us_liquid_2", size=2),
            UniverseSpec(name="us_liquid_3", size=3),
        ),
    )


def test_build_universe_membership_is_point_in_time():
    membership = build_universe_membership(
        sample_metrics(),
        config=config(),
    )

    july = membership[
        membership["membership_month"]
        == pd.Timestamp("2026-07-01").date()
    ].copy()

    assert set(july["universe_name"]) == {
        "us_liquid_2",
        "us_liquid_3",
    }

    assert len(july[july["universe_name"] == "us_liquid_2"]) == 2
    assert len(july[july["universe_name"] == "us_liquid_3"]) == 3

    assert "FAIL" not in set(july["ticker"])
    assert "PARTIAL" not in set(july["ticker"])

    us2 = july[july["universe_name"] == "us_liquid_2"].sort_values("rank")

    assert us2["ticker"].tolist() == ["AAPL", "MSFT"]
    assert us2["rank"].tolist() == [1, 2]

    assert set(us2["source_metric_month"]) == {
        pd.Timestamp("2026-06-01").date()
    }


def test_membership_month_is_next_month():
    membership = build_universe_membership(
        sample_metrics(),
        config=config(),
    )

    rows = membership[
        membership["source_metric_month"]
        == pd.Timestamp("2026-06-01").date()
    ]

    assert set(rows["membership_month"]) == {
        pd.Timestamp("2026-07-01").date()
    }
    assert set(rows["effective_start_date"]) == {
        pd.Timestamp("2026-07-01").date()
    }
    assert set(rows["effective_end_date"]) == {
        pd.Timestamp("2026-08-01").date()
    }


def test_write_universe_membership_partitions(tmp_path):
    membership = build_universe_membership(
        sample_metrics(),
        config=config(),
    )

    output_root = tmp_path / "data" / "dwd" / "universe_membership_monthly"

    written = write_universe_membership(
        membership,
        output_root,
        overwrite=False,
    )

    assert written

    expected_path = (
        output_root
        / "universe_name=us_liquid_2"
        / "year=2026"
        / "month=07"
        / "part-000.parquet"
    )

    assert expected_path in written
    assert expected_path.exists()

    loaded = pd.read_parquet(expected_path)
    assert len(loaded) == 2

    with pytest.raises(FileExistsError):
        write_universe_membership(
            membership,
            output_root,
            overwrite=False,
        )


def test_load_universe_membership_config(tmp_path):
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
    score_aggregation: "median"
  universes:
    - name: "us_liquid_100"
      size: 100
    - name: "us_liquid_500"
      size: 500
""",
        encoding="utf-8",
    )

    parsed = load_universe_membership_config(config_path)

    assert parsed.liquidity_monthly_output_root == Path(
        "data/dws/equity_liquidity_monthly"
    )
    assert parsed.universe_membership_output_root == Path(
        "data/dwd/universe_membership_monthly"
    )
    assert parsed.liquidity_score_method == "median_dollar_volume"
    assert parsed.score_aggregation == "median"
    assert parsed.lookback_months == 3
    assert [item.name for item in parsed.universes] == [
        "us_liquid_100",
        "us_liquid_500",
    ]