from __future__ import annotations

from pathlib import Path

import pytest

from quant_platform.research.config import (
    load_market_context_config,
    load_research_panel_config,
)


def write_research_config(tmp_path: Path) -> Path:
    path = tmp_path / "research_panel.yml"

    path.write_text(
        """
research_panel:
  factor_set: "core_v1"
  label_set: "core_v1"
  default_universe_name: "us_liquid_500"

  price_dwd_root: "data/dwd/equity_price_daily"
  universe_membership_root: "data/dwd/universe_membership_monthly"

  market_context_symbol_path: "data/dwd/security_master/dim_market_context_symbol/context_set=core_v1/part-000.parquet"
  market_context_price_root: "data/dwd/market_context_price_daily"
  market_context_feature_root: "data/dws/market_context_features_daily"

  feature_output_root: "data/dws/equity_features_daily"
  label_output_root: "data/dws/equity_forward_returns_daily"
  panel_output_root: "data/ads/equity_research_panel_daily"

  feature_scope:
    use_ever_members: true
    universe_name: "us_liquid_500"

  rolling_windows:
    returns: [1, 2, 4, 5, 12, 21, 24, 48, 63, 72, 126, 252]
    return_lag_multiples: [1, 2, 3]
    volatility: [21, 63, 126]
    dollar_volume: [3, 20, 60]
    price_position: [252]

  labels:
    horizons: [1, 2, 4, 5, 12, 21, 24]

  technical:
    backend: "talib"

  normalization:
    winsorize_lower: 0.01
    winsorize_upper: 0.99
    min_cross_section_count: 50

  composites:
    score_core_v1:
      momentum: 0.40
      reversal: 0.20
      low_vol: 0.20
      liquidity: 0.10
      technical: 0.10
""",
        encoding="utf-8",
    )

    return path


def write_market_context_config(tmp_path: Path) -> Path:
    path = tmp_path / "market_context.yml"

    path.write_text(
        """
market_context:
  context_set: "core_v1"
  source: "tiingo"
  dataset_name: "market_context_price_daily"
  dim_security_path: "data/dwd/security_master/dim_security.parquet"
  symbol_output_path: "data/dwd/security_master/dim_market_context_symbol/context_set=core_v1/part-000.parquet"

  primary_benchmarks:
    - SPY
    - QQQ

  symbols:
    broad_market:
      - SPY
      - VOO
    growth:
      - QQQ
""",
        encoding="utf-8",
    )

    return path


def test_load_research_panel_config(tmp_path):
    config = load_research_panel_config(write_research_config(tmp_path))

    assert config.factor_set == "core_v1"
    assert config.label_set == "core_v1"
    assert config.default_universe_name == "us_liquid_500"
    assert config.price_dwd_root == Path("data/dwd/equity_price_daily")
    assert config.market_context_symbol_path == Path(
        "data/dwd/security_master/dim_market_context_symbol/"
        "context_set=core_v1/part-000.parquet"
    )
    assert config.rolling_windows["return_lag_multiples"] == [1, 2, 3]
    assert config.label_horizons == (1, 2, 4, 5, 12, 21, 24)
    assert config.technical.backend == "talib"
    assert config.normalization.winsorize_lower == 0.01
    assert config.normalization.winsorize_upper == 0.99


def test_research_config_rejects_non_talib_backend(tmp_path):
    path = write_research_config(tmp_path)
    text = path.read_text(encoding="utf-8")
    path.write_text(
        text.replace('backend: "talib"', 'backend: "pandas"'),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="Only technical.backend='talib'",
    ):
        load_research_panel_config(path)


def test_load_market_context_config(tmp_path):
    config = load_market_context_config(write_market_context_config(tmp_path))

    assert config.context_set == "core_v1"
    assert config.source == "tiingo"
    assert config.dataset_name == "market_context_price_daily"
    assert config.dim_security_path == Path(
        "data/dwd/security_master/dim_security.parquet"
    )
    assert len(config.symbols) == 3

    by_ticker = {item.ticker: item for item in config.symbols}

    assert by_ticker["SPY"].context_group == "broad_market"
    assert by_ticker["SPY"].is_primary_benchmark
    assert by_ticker["VOO"].is_required
    assert by_ticker["QQQ"].context_group == "growth"


def test_market_context_config_rejects_duplicate_tickers(tmp_path):
    path = write_market_context_config(tmp_path)
    text = path.read_text(encoding="utf-8")
    text = text.replace(
        "growth:\n      - QQQ",
        "growth:\n      - QQQ\n      - SPY",
    )
    path.write_text(text, encoding="utf-8")

    with pytest.raises(
        ValueError,
        match="Duplicate market context ticker",
    ):
        load_market_context_config(path)