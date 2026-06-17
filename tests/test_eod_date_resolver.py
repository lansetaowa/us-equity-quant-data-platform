from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from quant_platform.calendar.eod import (
    EodResolutionConfig,
    latest_complete_eod_by_calendar,
    resolve_latest_complete_eod_date,
)


NY = ZoneInfo("America/New_York")


def test_manual_override_wins():
    config = EodResolutionConfig(
        manual_latest_complete_eod_date="2026-06-12",
        market_calendar="XNYS",
        market_timezone="America/New_York",
        market_close_buffer_minutes=90,
    )

    resolved = resolve_latest_complete_eod_date(
        config=config,
        now=datetime(2026, 6, 15, 18, 0, tzinfo=NY),
    )

    assert resolved.isoformat() == "2026-06-12"


def test_regular_trading_day_before_close_uses_previous_session():
    resolved = latest_complete_eod_by_calendar(
        now=datetime(2026, 6, 15, 10, 0, tzinfo=NY),
        market_calendar="XNYS",
        market_timezone="America/New_York",
        market_close_buffer_minutes=90,
    )

    assert resolved.isoformat() == "2026-06-12"


def test_regular_trading_day_after_close_but_before_buffer_uses_previous_session():
    resolved = latest_complete_eod_by_calendar(
        now=datetime(2026, 6, 15, 16, 30, tzinfo=NY),
        market_calendar="XNYS",
        market_timezone="America/New_York",
        market_close_buffer_minutes=90,
    )

    assert resolved.isoformat() == "2026-06-12"


def test_regular_trading_day_after_buffer_uses_today():
    resolved = latest_complete_eod_by_calendar(
        now=datetime(2026, 6, 15, 17, 31, tzinfo=NY),
        market_calendar="XNYS",
        market_timezone="America/New_York",
        market_close_buffer_minutes=90,
    )

    assert resolved.isoformat() == "2026-06-15"


def test_weekend_uses_previous_session():
    resolved = latest_complete_eod_by_calendar(
        now=datetime(2026, 6, 14, 12, 0, tzinfo=NY),
        market_calendar="XNYS",
        market_timezone="America/New_York",
        market_close_buffer_minutes=90,
    )

    assert resolved.isoformat() == "2026-06-12"


def test_xnys_holiday_uses_previous_session():
    resolved = latest_complete_eod_by_calendar(
        now=datetime(2026, 7, 3, 12, 0, tzinfo=NY),
        market_calendar="XNYS",
        market_timezone="America/New_York",
        market_close_buffer_minutes=90,
    )

    assert resolved.isoformat() == "2026-07-02"


def test_early_close_after_buffer_uses_same_session():
    resolved = latest_complete_eod_by_calendar(
        now=datetime(2026, 11, 27, 15, 0, tzinfo=NY),
        market_calendar="XNYS",
        market_timezone="America/New_York",
        market_close_buffer_minutes=90,
    )

    assert resolved.isoformat() == "2026-11-27"


def test_negative_buffer_rejected():
    with pytest.raises(ValueError, match="market_close_buffer_minutes must be >= 0"):
        latest_complete_eod_by_calendar(
            now=datetime(2026, 6, 15, 18, 0, tzinfo=NY),
            market_calendar="XNYS",
            market_timezone="America/New_York",
            market_close_buffer_minutes=-1,
        )