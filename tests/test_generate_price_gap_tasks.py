from __future__ import annotations

from datetime import date

import pandas as pd

from scripts.generate_price_gap_tasks import build_price_gap_tasks


def test_build_price_gap_tasks_from_bootstrap_anchor_when_no_dwd_rows():
    bootstrap = pd.DataFrame(
        {
            "ticker": ["AAPL", "MSFT"],
            "security_id": ["tiingo:AAPL", "tiingo:MSFT"],
        }
    )

    latest = pd.DataFrame(
        columns=["ticker", "security_id", "latest_dwd_date"]
    )

    tasks = build_price_gap_tasks(
        bootstrap_tasks=bootstrap,
        latest_dwd_dates=latest,
        source="tiingo",
        dataset_name="equity_price_daily",
        bootstrap_anchor_date=date(2026, 6, 11),
        latest_complete_eod_date=date(2026, 6, 12),
    )

    assert len(tasks) == 2
    assert set(tasks["ticker"]) == {"AAPL", "MSFT"}
    assert set(tasks["request_start_date"]) == {date(2026, 6, 12)}
    assert set(tasks["request_end_date"]) == {date(2026, 6, 12)}
    assert set(tasks["reason"]) == {"no_dwd_rows"}


def test_build_price_gap_tasks_from_latest_dwd_date():
    bootstrap = pd.DataFrame(
        {
            "ticker": ["AAPL", "MSFT"],
            "security_id": ["tiingo:AAPL", "tiingo:MSFT"],
        }
    )

    latest = pd.DataFrame(
        {
            "ticker": ["AAPL", "MSFT"],
            "security_id": ["tiingo:AAPL", "tiingo:MSFT"],
            "latest_dwd_date": [date(2026, 6, 11), date(2026, 6, 10)],
        }
    )

    tasks = build_price_gap_tasks(
        bootstrap_tasks=bootstrap,
        latest_dwd_dates=latest,
        source="tiingo",
        dataset_name="equity_price_daily",
        bootstrap_anchor_date=date(2026, 6, 11),
        latest_complete_eod_date=date(2026, 6, 12),
    )

    by_ticker = tasks.set_index("ticker")

    assert by_ticker.loc["AAPL", "request_start_date"] == date(2026, 6, 12)
    assert by_ticker.loc["MSFT", "request_start_date"] == date(2026, 6, 11)
    assert set(tasks["request_end_date"]) == {date(2026, 6, 12)}
    assert set(tasks["reason"]) == {"dwd_lag"}


def test_no_task_when_already_current():
    bootstrap = pd.DataFrame(
        {
            "ticker": ["AAPL"],
            "security_id": ["tiingo:AAPL"],
        }
    )

    latest = pd.DataFrame(
        {
            "ticker": ["AAPL"],
            "security_id": ["tiingo:AAPL"],
            "latest_dwd_date": [date(2026, 6, 12)],
        }
    )

    tasks = build_price_gap_tasks(
        bootstrap_tasks=bootstrap,
        latest_dwd_dates=latest,
        source="tiingo",
        dataset_name="equity_price_daily",
        bootstrap_anchor_date=date(2026, 6, 11),
        latest_complete_eod_date=date(2026, 6, 12),
    )

    assert tasks.empty


def test_no_task_when_latest_complete_eod_not_after_bootstrap_anchor():
    bootstrap = pd.DataFrame(
        {
            "ticker": ["AAPL"],
            "security_id": ["tiingo:AAPL"],
        }
    )

    latest = pd.DataFrame(
        columns=["ticker", "security_id", "latest_dwd_date"]
    )

    tasks = build_price_gap_tasks(
        bootstrap_tasks=bootstrap,
        latest_dwd_dates=latest,
        source="tiingo",
        dataset_name="equity_price_daily",
        bootstrap_anchor_date=date(2026, 6, 11),
        latest_complete_eod_date=date(2026, 6, 11),
    )

    assert tasks.empty


def test_mixed_current_and_lagged_tickers():
    bootstrap = pd.DataFrame(
        {
            "ticker": ["AAPL", "MSFT", "NVDA"],
            "security_id": ["tiingo:AAPL", "tiingo:MSFT", "tiingo:NVDA"],
        }
    )

    latest = pd.DataFrame(
        {
            "ticker": ["AAPL", "MSFT", "NVDA"],
            "security_id": ["tiingo:AAPL", "tiingo:MSFT", "tiingo:NVDA"],
            "latest_dwd_date": [
                date(2026, 6, 12),
                date(2026, 6, 11),
                date(2026, 6, 10),
            ],
        }
    )

    tasks = build_price_gap_tasks(
        bootstrap_tasks=bootstrap,
        latest_dwd_dates=latest,
        source="tiingo",
        dataset_name="equity_price_daily",
        bootstrap_anchor_date=date(2026, 6, 11),
        latest_complete_eod_date=date(2026, 6, 12),
    )

    assert set(tasks["ticker"]) == {"MSFT", "NVDA"}

    by_ticker = tasks.set_index("ticker")
    assert by_ticker.loc["MSFT", "request_start_date"] == date(2026, 6, 12)
    assert by_ticker.loc["NVDA", "request_start_date"] == date(2026, 6, 11)