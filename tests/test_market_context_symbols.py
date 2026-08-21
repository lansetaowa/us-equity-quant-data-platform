from __future__ import annotations

import pandas as pd
import pytest

from quant_platform.research.config import (
    MarketContextConfig,
    MarketContextSymbolSpec,
)
from quant_platform.research.market_context import (
    build_dim_market_context_symbol,
)


def config() -> MarketContextConfig:
    return MarketContextConfig(
        context_set="core_v1",
        source="tiingo",
        dataset_name="market_context_price_daily",
        dim_security_path="dim_security.parquet",
        symbol_output_path="market_context_symbols.parquet",
        symbols=(
            MarketContextSymbolSpec(
                context_group="broad_market",
                ticker="SPY",
                is_required=True,
                is_primary_benchmark=True,
            ),
            MarketContextSymbolSpec(
                context_group="growth",
                ticker="QQQ",
                is_required=True,
                is_primary_benchmark=True,
            ),
        ),
    )


def dim_security() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ticker": "SPY",
                "security_id": "tiingo:SPY",
                "source_ticker": "SPY",
                "exchange": "NYSE",
                "asset_type": "ETF",
                "price_currency": "USD",
                "start_date": "1993-01-29",
                "end_date": "2026-08-14",
                "is_active": True,
            },
            {
                "ticker": "QQQ",
                "security_id": "tiingo:QQQ",
                "source_ticker": "QQQ",
                "exchange": "NASDAQ",
                "asset_type": "ETF",
                "price_currency": "USD",
                "start_date": "1999-03-10",
                "end_date": "2026-08-14",
                "is_active": True,
            },
        ]
    )


def test_build_dim_market_context_symbol():
    output = build_dim_market_context_symbol(
        dim_security=dim_security(),
        config=config(),
    )

    assert len(output) == 2
    assert set(output["ticker"]) == {"SPY", "QQQ"}
    assert set(output["asset_type"]) == {"ETF"}
    assert output["context_set"].unique().tolist() == ["core_v1"]
    assert output["is_primary_benchmark"].all()
    assert "loaded_at_utc" in output.columns


def test_build_dim_market_context_symbol_rejects_missing_required():
    with pytest.raises(
        ValueError,
        match="Required market context tickers missing",
    ):
        build_dim_market_context_symbol(
            dim_security=dim_security().iloc[:1].copy(),
            config=config(),
        )


def test_build_dim_market_context_symbol_rejects_non_etf():
    bad = dim_security()
    bad.loc[bad["ticker"] == "QQQ", "asset_type"] = "Stock"

    with pytest.raises(
        ValueError,
        match="Market context symbols must be ETFs",
    ):
        build_dim_market_context_symbol(
            dim_security=bad,
            config=config(),
        )