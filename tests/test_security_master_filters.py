import pandas as pd

from scripts.build_candidate_pool import build_candidate_pool
from scripts.build_security_master import build_dim_security
from scripts.generate_backfill_task_list import build_backfill_task_list


def test_build_dim_security_fills_source_and_uses_grace_window() -> None:
    raw_df = pd.DataFrame(
        {
            "ticker": ["AAPL", "OLD"],
            "exchange": ["NASDAQ", "NYSE"],
            "assetType": ["Stock", "Stock"],
            "priceCurrency": ["USD", "USD"],
            "startDate": ["2010-01-01", "2010-01-01"],
            "endDate": ["2026-05-29", "2026-05-20"],
        }
    )

    result = build_dim_security(raw_df, active_end_date_grace_days=7)

    assert result["source"].notna().all()
    assert set(result["source"]) == {"tiingo"}

    active_by_ticker = dict(zip(result["ticker"], result["is_active"], strict=True))

    assert bool(active_by_ticker["AAPL"]) is True
    assert bool(active_by_ticker["OLD"]) is False


def test_candidate_pool_filters_to_us_common_stock_candidates() -> None:
    dim_security = pd.DataFrame(
        {
            "security_id": [
                "tiingo:AAPL",
                "tiingo:ETF1",
                "tiingo:CAD1",
                "tiingo:OTC1",
                "tiingo:NEW1",
                "tiingo:OLD1",
                "tiingo:NOSTART",
                "tiingo:NOEND",
                "tiingo:STR-WS",
                "tiingo:UTF-R",
                "tiingo:VYX-W",
                "tiingo:TFC-P-R",
                "tiingo:BRK-B",
            ],
            "source": ["tiingo"] * 13,
            "source_ticker": [
                "AAPL",
                "ETF1",
                "CAD1",
                "OTC1",
                "NEW1",
                "OLD1",
                "NOSTART",
                "NOEND",
                "STR-WS",
                "UTF-R",
                "VYX-W",
                "TFC-P-R",
                "BRK-B",
            ],
            "ticker": [
                "AAPL",
                "ETF1",
                "CAD1",
                "OTC1",
                "NEW1",
                "OLD1",
                "NOSTART",
                "NOEND",
                "STR-WS",
                "UTF-R",
                "VYX-W",
                "TFC-P-R",
                "BRK-B",
            ],
            "exchange": [
                "NASDAQ",
                "NYSE",
                "NASDAQ",
                "OTC",
                "NASDAQ",
                "NASDAQ",
                "NASDAQ",
                "NASDAQ",
                "NYSE",
                "NYSE",
                "NYSE",
                "NYSE",
                "NYSE",
            ],
            "asset_type": [
                "Stock",
                "ETF",
                "Stock",
                "Stock",
                "Stock",
                "Stock",
                "Stock",
                "Stock",
                "Stock",
                "Stock",
                "Stock",
                "Stock",
                "Stock",
            ],
            "price_currency": [
                "USD",
                "USD",
                "CAD",
                "USD",
                "USD",
                "USD",
                "USD",
                "USD",
                "USD",
                "USD",
                "USD",
                "USD",
                "USD",
            ],
            "start_date": [
                "2010-01-01",  # AAPL: keep
                "2010-01-01",  # ETF1: exclude by asset_type
                "2010-01-01",  # CAD1: exclude by currency
                "2010-01-01",  # OTC1: exclude by exchange
                "2023-01-01",  # NEW1: keep; post-2020 IPO-style ticker
                "2010-01-01",  # OLD1: exclude by old end_date
                None,          # NOSTART: exclude by null start_date
                "2010-01-01",  # NOEND: exclude by null end_date
                "2020-01-01",  # STR-WS: exclude by ticker suffix
                "2020-01-01",  # UTF-R: exclude by ticker suffix
                "2020-01-01",  # VYX-W: exclude by ticker suffix
                "2020-01-01",  # TFC-P-R: exclude by preferred suffix
                "2010-01-01",  # BRK-B: keep; share class, not preferred
            ],
            "end_date": [
                "2099-12-31",
                "2099-12-31",
                "2099-12-31",
                "2099-12-31",
                "2099-12-31",
                "2018-01-01",
                "2099-12-31",
                None,
                "2099-12-31",
                "2099-12-31",
                "2099-12-31",
                "2099-12-31",
                "2099-12-31",
            ],
            "is_active": [
                True,
                True,
                True,
                True,
                True,
                False,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
            ],
            "company_name": [None] * 13,
        }
    )

    config = {
        "dates": {
            "research_start_date": "2020-01-01",
            "price_backfill_start_date": "2019-01-01",
        },
        "candidate_filters": {
            "asset_types": ["Stock"],
            "currencies": ["USD"],
            "exchanges": ["NASDAQ", "NYSE", "NYSE ARCA", "NYSE MKT"],
            "exclude_name_patterns": [
                "ETF",
                "ETN",
                "Fund",
                "Trust",
                "Warrant",
                "Unit",
                "Right",
                "Preferred",
            ],
            "exclude_ticker_patterns": [
                "-R$",
                "-RT$",
                "-WS$",
                "-W$",
                "-WT$",
                "-U$",
                "-UN$",
                "-P-[A-Z]$",
                "-P[A-Z]$",
            ],
        },
        "candidate_pool": {"output_name": "us_common_stock_candidates"},
    }

    result = build_candidate_pool(dim_security, config)

    tickers = set(result["ticker"])

    assert "AAPL" in tickers
    assert "NEW1" in tickers
    assert "BRK-B" in tickers

    assert "ETF1" not in tickers
    assert "CAD1" not in tickers
    assert "OTC1" not in tickers
    assert "OLD1" not in tickers
    assert "NOSTART" not in tickers
    assert "NOEND" not in tickers

    assert "STR-WS" not in tickers
    assert "UTF-R" not in tickers
    assert "VYX-W" not in tickers
    assert "TFC-P-R" not in tickers


def test_build_backfill_task_list_creates_pending_tasks_without_nan_fields() -> None:
    candidate_pool = pd.DataFrame(
        {
            "security_id": ["tiingo:AAPL", "tiingo:MSFT"],
            "ticker": ["AAPL", "MSFT"],
            "source_ticker": ["AAPL", "MSFT"],
            "exchange": ["NASDAQ", "NASDAQ"],
            "asset_type": ["Stock", "Stock"],
            "price_currency": ["USD", "USD"],
            "start_date": ["2010-01-01", "2010-01-01"],
            "end_date": ["2099-12-31", "2099-12-31"],
            "is_active": [True, True],
            "company_name": [None, None],
            "candidate_pool_name": [
                "us_common_stock_candidates",
                "us_common_stock_candidates",
            ],
        }
    )

    config = {
        "source": "tiingo",
        "datasets": {"equity_price_daily": "equity_price_daily"},
        "dates": {"price_backfill_start_date": "2019-01-01"},
        "backfill_planning": {
            "pilot_task_list": {"limit": 500},
            "bootstrap_task_list": {"limit": None},
        },
    }

    result = build_backfill_task_list(
        candidate_pool=candidate_pool,
        config=config,
        task_list_name="pilot_500",
        limit_override=1,
        requested_end_date="2026-06-01",
    )

    assert len(result) == 1

    assert result["task_list_name"].iloc[0] == "pilot_500"
    assert result["source"].iloc[0] == "tiingo"
    assert result["dataset_name"].iloc[0] == "equity_price_daily"
    assert result["status"].iloc[0] == "pending"

    assert str(result["requested_start_date"].iloc[0]) == "2019-01-01"
    assert str(result["requested_end_date"].iloc[0]) == "2026-06-01"

    required_columns = [
        "task_id",
        "task_list_name",
        "source",
        "dataset_name",
        "security_id",
        "ticker",
        "requested_start_date",
        "requested_end_date",
        "priority",
        "status",
        "created_at",
    ]

    assert not result[required_columns].isna().any().any()
    assert "nan" not in result["task_id"].iloc[0].lower()