from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any
from urllib.parse import quote, urlencode

import requests
from requests import Session
from requests.exceptions import RequestException

from quant_platform.paths.price_paths import normalize_ticker

TIINGO_API_BASE_URL = "https://api.tiingo.com"
DEFAULT_USER_AGENT = "us-equity-quant-data-platform/1.0"

RETRYABLE_HTTP_STATUS_CODES = {
    408,
    425,
    429,
    500,
    502,
    503,
    504,
}


class TiingoClientError(RuntimeError):
    """Raised when a Tiingo request cannot be completed or validated."""


@dataclass(frozen=True)
class TiingoClientConfig:
    """Runtime configuration for Tiingo HTTP requests."""

    api_token: str = field(repr=False)
    timeout_seconds: float = 60.0
    max_attempts: int = 3
    retry_sleep_seconds: float = 2.0
    user_agent: str = DEFAULT_USER_AGENT

    def __post_init__(self) -> None:
        if not self.api_token.strip():
            raise ValueError("api_token must not be empty")

        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be > 0")

        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")

        if self.retry_sleep_seconds < 0:
            raise ValueError("retry_sleep_seconds must be >= 0")

        if not self.user_agent.strip():
            raise ValueError("user_agent must not be empty")


def _coerce_date(value: str | date | datetime) -> date:
    """Convert supported date-like input into a Python date."""
    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    return date.fromisoformat(str(value).strip())


def build_daily_prices_url(
    ticker: str,
    start_date: str | date | datetime,
    end_date: str | date | datetime,
) -> str:
    """
    Build a Tiingo EOD price URL for one ticker and request window.

    Example:
    https://api.tiingo.com/tiingo/daily/AAPL/prices
      ?startDate=2026-06-12
      &endDate=2026-06-12
      &format=json
    """
    ticker_norm = normalize_ticker(ticker)
    start = _coerce_date(start_date)
    end = _coerce_date(end_date)

    if start > end:
        raise ValueError(
            "start_date must be less than or equal to end_date"
        )

    encoded_ticker = quote(ticker_norm, safe="")

    query = urlencode(
        {
            "startDate": start.isoformat(),
            "endDate": end.isoformat(),
            "format": "json",
        }
    )

    return (
        f"{TIINGO_API_BASE_URL}/tiingo/daily/"
        f"{encoded_ticker}/prices?{query}"
    )


def build_auth_headers(
    api_token: str,
    user_agent: str = DEFAULT_USER_AGENT,
) -> dict[str, str]:
    """Build request headers for authenticated Tiingo API requests."""
    token = str(api_token).strip()
    agent = str(user_agent).strip()

    if not token:
        raise ValueError("api_token must not be empty")

    if not agent:
        raise ValueError("user_agent must not be empty")

    return {
        "Authorization": f"Token {token}",
        "Accept": "application/json",
        "User-Agent": agent,
    }


def _response_error_message(
    ticker: str,
    status_code: int,
    response_text: str,
) -> str:
    text = str(response_text or "").strip().replace("\n", " ")[:500]

    if text:
        return f"Tiingo HTTP {status_code} for {ticker}: {text}"

    return f"Tiingo HTTP {status_code} for {ticker}"


def fetch_daily_prices(
    ticker: str,
    start_date: str | date | datetime,
    end_date: str | date | datetime,
    config: TiingoClientConfig,
    *,
    session: Session | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> list[dict[str, Any]]:
    """
    Fetch Tiingo EOD rows for one ticker and request window.

    Empty lists are returned unchanged. The caller decides whether an empty
    response should be marked skipped, retried later, or treated as an error.

    A supplied session is useful for connection reuse and unit testing.
    """
    ticker_norm = normalize_ticker(ticker)

    url = build_daily_prices_url(
        ticker=ticker_norm,
        start_date=start_date,
        end_date=end_date,
    )

    headers = build_auth_headers(
        api_token=config.api_token,
        user_agent=config.user_agent,
    )

    owns_session = session is None
    client = session or requests.Session()
    last_error: str | None = None

    try:
        for attempt in range(1, config.max_attempts + 1):
            try:
                response = client.get(
                    url,
                    headers=headers,
                    timeout=config.timeout_seconds,
                )
            except RequestException as exc:
                last_error = (
                    f"Tiingo request error for {ticker_norm}: {exc!r}"
                )

                if attempt < config.max_attempts:
                    sleep_fn(config.retry_sleep_seconds)
                    continue

                raise TiingoClientError(last_error) from exc

            status_code = int(response.status_code)

            if not 200 <= status_code < 300:
                last_error = _response_error_message(
                    ticker=ticker_norm,
                    status_code=status_code,
                    response_text=response.text,
                )

                retryable = (
                    status_code in RETRYABLE_HTTP_STATUS_CODES
                    or status_code >= 500
                )

                if retryable and attempt < config.max_attempts:
                    sleep_fn(config.retry_sleep_seconds)
                    continue

                raise TiingoClientError(last_error)

            try:
                payload = response.json()
            except ValueError as exc:
                raise TiingoClientError(
                    f"Tiingo returned invalid JSON for {ticker_norm}"
                ) from exc

            if not isinstance(payload, list):
                raise TiingoClientError(
                    "Expected Tiingo price response to be a list, "
                    f"got {type(payload).__name__} for {ticker_norm}"
                )

            if not all(isinstance(row, dict) for row in payload):
                raise TiingoClientError(
                    f"Tiingo returned non-object rows for {ticker_norm}"
                )

            return [dict(row) for row in payload]

    finally:
        if owns_session:
            client.close()

    raise TiingoClientError(
        last_error or f"Tiingo request failed for {ticker_norm}"
    )


def _parse_price_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    text = str(value or "").strip()

    if len(text) < 10:
        return None

    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def extract_latest_price_date(
    rows: Sequence[dict[str, Any]],
) -> date | None:
    """Return the latest valid Tiingo price date from response rows."""
    parsed_dates = [
        parsed
        for row in rows
        if (parsed := _parse_price_date(row.get("date"))) is not None
    ]

    if not parsed_dates:
        return None

    return max(parsed_dates)