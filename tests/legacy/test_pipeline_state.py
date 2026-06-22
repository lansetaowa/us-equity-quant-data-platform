from datetime import date

import pytest

from scripts.legacy.pipeline_state import compute_refresh_window


def test_compute_refresh_window_backfill() -> None:
    start_date, end_date = compute_refresh_window(
        mode="backfill",
        backfill_start_date="2020-01-01",
        default_lookback_days=7,
        last_successful_data_end_date=None,
        today=date(2026, 5, 21),
    )

    assert start_date == date(2020, 1, 1)
    assert end_date == date(2026, 5, 21)


def test_compute_refresh_window_incremental_without_previous_run() -> None:
    start_date, end_date = compute_refresh_window(
        mode="incremental",
        backfill_start_date="2020-01-01",
        default_lookback_days=7,
        last_successful_data_end_date=None,
        today=date(2026, 5, 21),
    )

    assert start_date == date(2020, 1, 1)
    assert end_date == date(2026, 5, 21)


def test_compute_refresh_window_incremental_with_previous_run() -> None:
    start_date, end_date = compute_refresh_window(
        mode="incremental",
        backfill_start_date="2020-01-01",
        default_lookback_days=7,
        last_successful_data_end_date=date(2026, 5, 20),
        today=date(2026, 5, 21),
    )

    assert start_date == date(2026, 5, 13)
    assert end_date == date(2026, 5, 21)


def test_compute_refresh_window_rejects_invalid_mode() -> None:
    with pytest.raises(ValueError):
        compute_refresh_window(
            mode="invalid",
            backfill_start_date="2020-01-01",
            default_lookback_days=7,
            last_successful_data_end_date=None,
            today=date(2026, 5, 21),
        )