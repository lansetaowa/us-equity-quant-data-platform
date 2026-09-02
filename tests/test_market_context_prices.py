from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from quant_platform.research.config import (
    MarketContextConfig,
    MarketContextSymbolSpec,
)
from quant_platform.research.market_context_prices import (
    build_market_context_price_tasks,
    build_market_context_raw_path,
    combine_market_context_prices,
    context_dwd_root,
    write_market_context_price_partitions,
)


def config(tmp_path: Path) -> MarketContextConfig:
    return MarketContextConfig(
        context_set="core_v1",
        source="tiingo",
        dataset_name="market_context_price_daily",
        price_start_date=date(2019, 1, 1),
        dim_security_path=Path("dim_security.parquet"),
        symbol_output_path=Path("symbols.parquet"),
        price_ods_root=tmp_path / "data" / "ods" / "market_context",
        price_dwd_root=tmp_path / "data" / "dwd" / "market_context_price_daily",
        symbols=(
            MarketContextSymbolSpec(
                context_group="broad_market",
                ticker="SPY",
                is_required=True,
                is_primary_benchmark=True,
            ),
        ),
    )


def symbols() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "context_set": ["core_v1"],
            "context_group": ["broad_market"],
            "ticker": ["SPY"],
            "security_id": ["tiingo:SPY"],
            "source_ticker": ["SPY"],
            "exchange": ["NYSE"],
            "asset_type": ["ETF"],
            "price_currency": ["USD"],
            "start_date": [date(1993, 1, 29)],
            "end_date": [date(2026, 8, 14)],
            "is_active": [True],
            "is_required": [True],
            "is_primary_benchmark": [True],
            "loaded_at_utc": ["2026-08-17T00:00:00+00:00"],
        }
    )


def dwd_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "security_id": "tiingo:SPY",
                "ticker": "SPY",
                "date": date(2026, 8, 14),
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1000,
                "adj_open": 100.0,
                "adj_high": 101.0,
                "adj_low": 99.0,
                "adj_close": 100.5,
                "adj_volume": 1000,
                "div_cash": 0.0,
                "split_factor": 1.0,
                "source": "tiingo",
                "load_id": "test",
                "loaded_at": "2026-08-17T00:00:00+00:00",
            }
        ]
    )


def test_build_market_context_raw_path(tmp_path):
    path = build_market_context_raw_path(
        ods_root=tmp_path / "ods",
        context_set="core_v1",
        ticker="SPY",
        request_start_date=date(2026, 8, 1),
        request_end_date=date(2026, 8, 14),
    )

    assert path == (
        tmp_path
        / "ods"
        / "context_set=core_v1"
        / "symbol=SPY"
        / "request_start=2026-08-01"
        / "request_end=2026-08-14"
        / "prices.json"
    )


def test_build_market_context_price_tasks_initial(tmp_path):
    cfg = config(tmp_path)

    tasks = build_market_context_price_tasks(
        symbols=symbols(),
        config=cfg,
        request_end_date=date(2026, 8, 14),
        existing_prices=pd.DataFrame(),
    )

    assert len(tasks) == 1
    assert tasks[0].ticker == "SPY"
    assert tasks[0].request_start_date == date(2019, 1, 1)
    assert tasks[0].request_end_date == date(2026, 8, 14)


def test_build_market_context_price_tasks_skips_current(tmp_path):
    cfg = config(tmp_path)
    existing = dwd_rows()
    existing["context_set"] = "core_v1"
    existing["context_group"] = "broad_market"

    tasks = build_market_context_price_tasks(
        symbols=symbols(),
        config=cfg,
        request_end_date=date(2026, 8, 14),
        existing_prices=existing,
    )

    assert tasks == []


def test_combine_market_context_prices_adds_context_columns(tmp_path):
    output = combine_market_context_prices(
        existing_prices=pd.DataFrame(),
        new_prices=dwd_rows(),
        symbols=symbols(),
        context_set="core_v1",
    )

    assert len(output) == 1
    assert output.loc[0, "context_set"] == "core_v1"
    assert output.loc[0, "context_group"] == "broad_market"
    assert output.loc[0, "ticker"] == "SPY"


def test_write_market_context_price_partitions(tmp_path):
    price = combine_market_context_prices(
        existing_prices=pd.DataFrame(),
        new_prices=dwd_rows(),
        symbols=symbols(),
        context_set="core_v1",
    )

    written = write_market_context_price_partitions(
        prices=price,
        dwd_root=tmp_path / "data" / "dwd" / "market_context_price_daily",
        context_set="core_v1",
        replace_existing=False,
    )

    expected = (
        context_dwd_root(
            dwd_root=tmp_path / "data" / "dwd" / "market_context_price_daily",
            context_set="core_v1",
        )
        / "year=2026"
        / "month=08"
        / "part-000.parquet"
    )

    assert written == [expected]
    assert expected.exists()

    loaded = pd.read_parquet(expected)

    assert loaded.loc[0, "context_set"] == "core_v1"
    assert loaded.loc[0, "context_group"] == "broad_market"

def test_build_market_context_price_tasks_catches_up_from_latest_date(
    tmp_path,
):
    cfg = config(tmp_path)
    existing = dwd_rows()
    existing["context_set"] = "core_v1"
    existing["context_group"] = "broad_market"

    tasks = build_market_context_price_tasks(
        symbols=symbols(),
        config=cfg,
        request_end_date=date(2026, 8, 18),
        existing_prices=existing,
    )

    assert len(tasks) == 1
    assert tasks[0].ticker == "SPY"
    assert tasks[0].request_start_date == date(2026, 8, 15)
    assert tasks[0].request_end_date == date(2026, 8, 18)

def test_context_dwd_root_accepts_context_partition_root(tmp_path):
    root = (
        tmp_path
        / "data"
        / "dwd"
        / "market_context_price_daily"
        / "context_set=core_v1"
    )

    assert context_dwd_root(
        dwd_root=root,
        context_set="core_v1",
    ) == root

def test_write_market_context_price_partitions_selected_month_only(
    tmp_path,
):
    price = combine_market_context_prices(
        existing_prices=pd.DataFrame(),
        new_prices=dwd_rows(),
        symbols=symbols(),
        context_set="core_v1",
    )

    dwd_root = tmp_path / "data" / "dwd" / "market_context_price_daily"

    written = write_market_context_price_partitions(
        prices=price,
        dwd_root=dwd_root,
        context_set="core_v1",
        replace_existing=False,
        months_to_write={date(2026, 8, 1)},
    )

    assert len(written) == 1
    assert written[0].exists()
    assert "year=2026" in written[0].as_posix()
    assert "month=08" in written[0].as_posix()