from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import pandas_market_calendars as mcal


@dataclass(frozen=True)
class EodResolutionConfig:
    """
    Config for resolving the latest complete EOD date.

    manual_latest_complete_eod_date:
        Optional override for controlled testing.

    market_calendar:
        Exchange calendar name. For US equities, use XNYS.

    market_timezone:
        Time zone used for operational clock logic.

    market_close_buffer_minutes:
        Extra buffer after exchange close before today's EOD is considered
        eligible.
    """

    manual_latest_complete_eod_date: str | None = None
    market_calendar: str = "XNYS"
    market_timezone: str = "America/New_York"
    market_close_buffer_minutes: int = 90


def parse_iso_date(value: str) -> date:
    """Parse YYYY-MM-DD into date."""
    return date.fromisoformat(str(value).strip())


def _as_local_timestamp(value: datetime, timezone: str) -> pd.Timestamp:
    """Convert datetime into timezone-aware pandas Timestamp in target timezone."""
    tz = ZoneInfo(timezone)

    if value.tzinfo is None:
        return pd.Timestamp(value.replace(tzinfo=tz))

    return pd.Timestamp(value).tz_convert(timezone)


def _get_calendar(calendar_name: str):
    """Return market calendar instance."""
    return mcal.get_calendar(calendar_name)


def _schedule_around(
    calendar,
    current_date: date,
    lookback_days: int = 14,
    lookahead_days: int = 1,
) -> pd.DataFrame:
    """
    Get exchange schedule around current date.

    A 14-day lookback handles normal weekends and market holidays while keeping
    the resolver simple.
    """
    start_date = current_date - timedelta(days=lookback_days)
    end_date = current_date + timedelta(days=lookahead_days)

    schedule = calendar.schedule(
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
    )

    if schedule.empty:
        calendar_name = getattr(calendar, "name", "<unknown>")
        raise ValueError(
            f"No market sessions found for calendar={calendar_name} "
            f"between {start_date} and {end_date}"
        )

    return schedule


def _session_dates(schedule: pd.DataFrame) -> list[date]:
    """Extract session dates from a pandas_market_calendars schedule."""
    return [pd.Timestamp(idx).date() for idx in schedule.index]


def _previous_session_date(schedule: pd.DataFrame, current_date: date) -> date:
    """Return the latest session date strictly before current_date."""
    previous_sessions = [
        session_date
        for session_date in _session_dates(schedule)
        if session_date < current_date
    ]

    if not previous_sessions:
        raise ValueError(f"No previous market session found before {current_date}")

    return max(previous_sessions)


def _market_close_for_session(
    schedule: pd.DataFrame,
    session_date: date,
    timezone: str,
) -> pd.Timestamp:
    """Return market close timestamp for a session in the target timezone."""
    session_key = pd.Timestamp(session_date)

    if session_key not in schedule.index:
        raise ValueError(f"{session_date} is not present in the market schedule")

    market_close = schedule.loc[session_key, "market_close"]

    return pd.Timestamp(market_close).tz_convert(timezone)


def latest_complete_eod_by_calendar(
    now: datetime,
    market_calendar: str = "XNYS",
    market_timezone: str = "America/New_York",
    market_close_buffer_minutes: int = 90,
) -> date:
    """
    Resolve latest complete EOD date using exchange calendar and close buffer.

    Logic:
    - If today is a market session and current time is after market close + buffer,
      today is complete.
    - Otherwise, use the previous market session.
    - If today is not a market session, use the previous market session.
    """
    if market_close_buffer_minutes < 0:
        raise ValueError("market_close_buffer_minutes must be >= 0")

    now_local = _as_local_timestamp(now, market_timezone)
    today = now_local.date()

    calendar = _get_calendar(market_calendar)
    schedule = _schedule_around(calendar, current_date=today)

    session_dates = set(_session_dates(schedule))

    if today not in session_dates:
        return _previous_session_date(schedule, today)

    market_close = _market_close_for_session(
        schedule=schedule,
        session_date=today,
        timezone=market_timezone,
    )
    available_after = market_close + pd.Timedelta(minutes=market_close_buffer_minutes)

    if now_local >= available_after:
        return today

    return _previous_session_date(schedule, today)


def resolve_latest_complete_eod_date(
    config: EodResolutionConfig,
    now: datetime | None = None,
) -> date:
    """
    Resolve the latest complete EOD date.

    Priority:
    1. Manual override.
    2. XNYS calendar + market close buffer.
    """
    if config.manual_latest_complete_eod_date:
        return parse_iso_date(config.manual_latest_complete_eod_date)

    if now is None:
        now = datetime.now(ZoneInfo(config.market_timezone))

    return latest_complete_eod_by_calendar(
        now=now,
        market_calendar=config.market_calendar,
        market_timezone=config.market_timezone,
        market_close_buffer_minutes=config.market_close_buffer_minutes,
    )