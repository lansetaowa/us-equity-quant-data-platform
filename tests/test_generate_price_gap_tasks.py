from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from quant_platform.prices.gap_tasks import (
    attach_daily_update_eligibility,
    build_price_gap_tasks,
)


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


def test_attach_daily_update_eligibility_excludes_old_end_date():
    bootstrap = pd.DataFrame(
        {
            "ticker": ["AAPL", "AABA", "AAC"],
            "security_id": ["tiingo:AAPL", "tiingo:AABA", "tiingo:AAC"],
        }
    )

    dim_security = pd.DataFrame(
        {
            "ticker": ["AAPL", "AABA", "AAC"],
            "security_id": ["tiingo:AAPL", "tiingo:AABA", "tiingo:AAC"],
            "end_date": [
                date(2026, 6, 11),
                date(2019, 10, 2),
                date(2021, 12, 31),
            ],
            "is_active": [True, False, False],
        }
    )

    result = attach_daily_update_eligibility(
        bootstrap_tasks=bootstrap,
        dim_security=dim_security,
        bootstrap_anchor_date=date(2026, 6, 11),
        active_end_date_grace_days=7,
    )

    by_ticker = result.set_index("ticker")

    assert bool(by_ticker.loc["AAPL", "eligible_for_daily_update"])
    assert not bool(by_ticker.loc["AABA", "eligible_for_daily_update"])
    assert not bool(by_ticker.loc["AAC", "eligible_for_daily_update"])
    assert (
        by_ticker.loc["AABA", "daily_update_exclusion_reason"]
        == "inactive_or_stale_end_date"
    )


def test_attach_daily_update_eligibility_keeps_recent_end_date_with_grace():
    bootstrap = pd.DataFrame(
        {
            "ticker": ["XYZ"],
            "security_id": ["tiingo:XYZ"],
        }
    )

    dim_security = pd.DataFrame(
        {
            "ticker": ["XYZ"],
            "security_id": ["tiingo:XYZ"],
            "end_date": [date(2026, 6, 8)],
            "is_active": [False],
        }
    )

    result = attach_daily_update_eligibility(
        bootstrap_tasks=bootstrap,
        dim_security=dim_security,
        bootstrap_anchor_date=date(2026, 6, 11),
        active_end_date_grace_days=7,
    )

    assert bool(result.loc[0, "eligible_for_daily_update"])


def test_attach_daily_update_eligibility_keeps_missing_end_date():
    bootstrap = pd.DataFrame(
        {
            "ticker": ["OPENEND"],
            "security_id": ["tiingo:OPENEND"],
        }
    )

    dim_security = pd.DataFrame(
        {
            "ticker": ["OPENEND"],
            "security_id": ["tiingo:OPENEND"],
            "end_date": [pd.NaT],
            "is_active": [False],
        }
    )

    result = attach_daily_update_eligibility(
        bootstrap_tasks=bootstrap,
        dim_security=dim_security,
        bootstrap_anchor_date=date(2026, 6, 11),
        active_end_date_grace_days=7,
    )

    assert bool(result.loc[0, "eligible_for_daily_update"])


def test_attach_daily_update_eligibility_keeps_active_even_with_old_end_date():
    bootstrap = pd.DataFrame(
        {
            "ticker": ["ACTIVEOLD"],
            "security_id": ["tiingo:ACTIVEOLD"],
        }
    )

    dim_security = pd.DataFrame(
        {
            "ticker": ["ACTIVEOLD"],
            "security_id": ["tiingo:ACTIVEOLD"],
            "end_date": [date(2020, 1, 1)],
            "is_active": [True],
        }
    )

    result = attach_daily_update_eligibility(
        bootstrap_tasks=bootstrap,
        dim_security=dim_security,
        bootstrap_anchor_date=date(2026, 6, 11),
        active_end_date_grace_days=7,
    )

    assert bool(result.loc[0, "eligible_for_daily_update"])


def test_attach_daily_update_eligibility_excludes_missing_dim_security():
    bootstrap = pd.DataFrame(
        {
            "ticker": ["AAPL", "MISSING"],
            "security_id": ["tiingo:AAPL", "tiingo:MISSING"],
        }
    )

    dim_security = pd.DataFrame(
        {
            "ticker": ["AAPL"],
            "security_id": ["tiingo:AAPL"],
            "end_date": [date(2026, 6, 11)],
            "is_active": [True],
        }
    )

    result = attach_daily_update_eligibility(
        bootstrap_tasks=bootstrap,
        dim_security=dim_security,
        bootstrap_anchor_date=date(2026, 6, 11),
        active_end_date_grace_days=7,
    )

    by_ticker = result.set_index("ticker")

    assert bool(by_ticker.loc["AAPL", "eligible_for_daily_update"])
    assert not bool(by_ticker.loc["MISSING", "eligible_for_daily_update"])
    assert (
        by_ticker.loc["MISSING", "daily_update_exclusion_reason"]
        == "missing_dim_security"
    )


def test_attach_daily_update_eligibility_rejects_negative_grace_days():
    bootstrap = pd.DataFrame(
        {
            "ticker": ["AAPL"],
            "security_id": ["tiingo:AAPL"],
        }
    )

    dim_security = pd.DataFrame(
        {
            "ticker": ["AAPL"],
            "security_id": ["tiingo:AAPL"],
            "end_date": [date(2026, 6, 11)],
            "is_active": [True],
        }
    )

    with pytest.raises(
        ValueError,
        match="active_end_date_grace_days must be >= 0",
    ):
        attach_daily_update_eligibility(
            bootstrap_tasks=bootstrap,
            dim_security=dim_security,
            bootstrap_anchor_date=date(2026, 6, 11),
            active_end_date_grace_days=-1,
        )

def test_attach_daily_update_eligibility_uses_dim_security_end_date_only():
    bootstrap = pd.DataFrame(
        {
            "ticker": ["AAPL"],
            "security_id": ["tiingo:AAPL"],
            # This column can exist in task-list-like inputs.
            # Eligibility should use dim_security.end_date, not this value.
            "end_date": [date(2020, 1, 1)],
        }
    )

    dim_security = pd.DataFrame(
        {
            "ticker": ["AAPL"],
            "security_id": ["tiingo:AAPL"],
            "end_date": [date(2026, 6, 11)],
            "is_active": [False],
        }
    )

    result = attach_daily_update_eligibility(
        bootstrap_tasks=bootstrap,
        dim_security=dim_security,
        bootstrap_anchor_date=date(2026, 6, 11),
        active_end_date_grace_days=7,
    )

    assert bool(result.loc[0, "eligible_for_daily_update"])
    assert result.loc[0, "end_date"] == date(2026, 6, 11)
    assert "end_date_x" not in result.columns
    assert "end_date_y" not in result.columns