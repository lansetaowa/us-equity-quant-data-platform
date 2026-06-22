from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from quant_platform.clients.tiingo import (
    TiingoClientConfig,
    TiingoClientError,
    build_auth_headers,
    build_daily_prices_url,
    extract_latest_price_date,
    fetch_daily_prices,
)


class FakeResponse:
    def __init__(
        self,
        status_code: int,
        *,
        payload: Any = None,
        text: str = "",
        json_error: ValueError | None = None,
    ) -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = text
        self._json_error = json_error

    def json(self) -> Any:
        if self._json_error is not None:
            raise self._json_error

        return self._payload


class FakeSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    def get(
        self,
        url: str,
        *,
        headers: dict[str, str],
        timeout: float,
    ) -> FakeResponse:
        self.calls.append(
            {
                "url": url,
                "headers": headers,
                "timeout": timeout,
            }
        )

        if not self.responses:
            raise AssertionError("No fake response remains")

        return self.responses.pop(0)


def test_build_daily_prices_url():
    url = build_daily_prices_url(
        ticker=" aapl ",
        start_date=date(2026, 6, 12),
        end_date=date(2026, 6, 15),
    )

    assert url == (
        "https://api.tiingo.com/tiingo/daily/AAPL/prices"
        "?startDate=2026-06-12"
        "&endDate=2026-06-15"
        "&format=json"
    )


def test_build_daily_prices_url_encodes_ticker():
    url = build_daily_prices_url(
        ticker="BRK/B",
        start_date="2026-06-12",
        end_date="2026-06-12",
    )

    assert "/BRK%2FB/prices" in url


def test_build_daily_prices_url_rejects_reversed_window():
    with pytest.raises(
        ValueError,
        match="start_date must be less than or equal to end_date",
    ):
        build_daily_prices_url(
            ticker="AAPL",
            start_date="2026-06-13",
            end_date="2026-06-12",
        )


def test_build_auth_headers():
    headers = build_auth_headers("secret-token")

    assert headers["Authorization"] == "Token secret-token"
    assert headers["Accept"] == "application/json"
    assert headers["User-Agent"]


def test_fetch_daily_prices_success():
    session = FakeSession(
        [
            FakeResponse(
                200,
                payload=[
                    {
                        "date": "2026-06-12T00:00:00.000Z",
                        "close": 200.0,
                    }
                ],
            )
        ]
    )

    config = TiingoClientConfig(
        api_token="secret",
        max_attempts=1,
    )

    rows = fetch_daily_prices(
        ticker="AAPL",
        start_date="2026-06-12",
        end_date="2026-06-12",
        config=config,
        session=session,
    )

    assert len(rows) == 1
    assert rows[0]["close"] == 200.0
    assert len(session.calls) == 1
    assert (
        session.calls[0]["headers"]["Authorization"]
        == "Token secret"
    )


def test_fetch_daily_prices_returns_empty_list():
    session = FakeSession(
        [FakeResponse(200, payload=[])]
    )

    config = TiingoClientConfig(
        api_token="secret",
        max_attempts=1,
    )

    rows = fetch_daily_prices(
        ticker="AAPL",
        start_date="2026-06-12",
        end_date="2026-06-12",
        config=config,
        session=session,
    )

    assert rows == []


def test_fetch_daily_prices_does_not_retry_404():
    session = FakeSession(
        [
            FakeResponse(
                404,
                text="Ticker not found",
            )
        ]
    )

    sleep_calls: list[float] = []

    config = TiingoClientConfig(
        api_token="secret",
        max_attempts=3,
        retry_sleep_seconds=1.0,
    )

    with pytest.raises(
        TiingoClientError,
        match="HTTP 404",
    ):
        fetch_daily_prices(
            ticker="OLD",
            start_date="2026-06-12",
            end_date="2026-06-12",
            config=config,
            session=session,
            sleep_fn=sleep_calls.append,
        )

    assert len(session.calls) == 1
    assert sleep_calls == []


def test_fetch_daily_prices_retries_429():
    session = FakeSession(
        [
            FakeResponse(
                429,
                text="Rate limit",
            ),
            FakeResponse(
                200,
                payload=[
                    {
                        "date": "2026-06-12T00:00:00.000Z",
                        "close": 200.0,
                    }
                ],
            ),
        ]
    )

    sleep_calls: list[float] = []

    config = TiingoClientConfig(
        api_token="secret",
        max_attempts=2,
        retry_sleep_seconds=0.25,
    )

    rows = fetch_daily_prices(
        ticker="AAPL",
        start_date="2026-06-12",
        end_date="2026-06-12",
        config=config,
        session=session,
        sleep_fn=sleep_calls.append,
    )

    assert len(rows) == 1
    assert len(session.calls) == 2
    assert sleep_calls == [0.25]


def test_fetch_daily_prices_rejects_non_list_payload():
    session = FakeSession(
        [
            FakeResponse(
                200,
                payload={"error": "unexpected"},
            )
        ]
    )

    config = TiingoClientConfig(
        api_token="secret",
        max_attempts=1,
    )

    with pytest.raises(
        TiingoClientError,
        match="response to be a list",
    ):
        fetch_daily_prices(
            ticker="AAPL",
            start_date="2026-06-12",
            end_date="2026-06-12",
            config=config,
            session=session,
        )


def test_extract_latest_price_date():
    rows = [
        {"date": "2026-06-10T00:00:00.000Z"},
        {"date": "bad-value"},
        {"date": "2026-06-12T00:00:00.000Z"},
        {"date": None},
    ]

    assert extract_latest_price_date(rows) == date(2026, 6, 12)


def test_extract_latest_price_date_returns_none():
    assert extract_latest_price_date([]) is None
    assert extract_latest_price_date([{"date": "bad"}]) is None