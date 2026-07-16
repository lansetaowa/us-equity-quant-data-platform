from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from quant_platform.prices.download import run_price_download_tasks

from quant_platform.clients.tiingo import (
    TiingoClientConfig,
)
from quant_platform.paths.price_paths import (
    build_windowed_price_raw_path,
)
from quant_platform.prices.download import (
    PriceDownloadSettings,
    build_price_download_plan,
    load_price_gap_tasks,
    process_price_download_task,
    select_price_download_tasks,
    validate_price_rows_for_window,
)
from quant_platform.storage.local_json import (
    read_json_rows,
    write_json_rows,
)


def make_task(
    ticker: str = "AAPL",
) -> dict[str, Any]:
    return {
        "source": "tiingo",
        "dataset_name": "equity_price_daily",
        "ticker": ticker,
        "security_id": f"tiingo:{ticker}",
        "request_start_date": date(2026, 6, 12),
        "request_end_date": date(2026, 6, 15),
        "reason": "dwd_lag",
    }


def make_rows() -> list[dict[str, Any]]:
    return [
        {
            "date": "2026-06-12T00:00:00.000Z",
            "close": 100.0,
        },
        {
            "date": "2026-06-15T00:00:00.000Z",
            "close": 101.0,
        },
    ]


def test_load_price_gap_tasks(tmp_path):
    path = tmp_path / "tasks.parquet"

    pd.DataFrame(
        [make_task()]
    ).to_parquet(path, index=False)

    tasks = load_price_gap_tasks(path)

    assert len(tasks) == 1
    assert tasks.loc[0, "ticker"] == "AAPL"
    assert (
        tasks.loc[0, "request_start_date"]
        == date(2026, 6, 12)
    )


def test_load_price_gap_tasks_rejects_duplicate_keys(
    tmp_path,
):
    path = tmp_path / "tasks.parquet"

    pd.DataFrame(
        [make_task(), make_task()]
    ).to_parquet(path, index=False)

    with pytest.raises(
        ValueError,
        match="Duplicate ticker/security_id",
    ):
        load_price_gap_tasks(path)


def test_select_price_download_tasks_filters_and_limits():
    tasks = pd.DataFrame(
        [
            make_task("AAPL"),
            make_task("MSFT"),
            make_task("NVDA"),
        ]
    )

    selected = select_price_download_tasks(
        tasks,
        tickers=["MSFT", "NVDA"],
        limit=1,
    )

    assert selected["ticker"].tolist() == ["MSFT"]


def test_select_price_download_tasks_rejects_unknown_ticker():
    tasks = pd.DataFrame([make_task("AAPL")])

    with pytest.raises(
        ValueError,
        match="not present",
    ):
        select_price_download_tasks(
            tasks,
            tickers=["MISSING"],
        )


def test_validate_price_rows_for_window():
    first_date, last_date = (
        validate_price_rows_for_window(
            make_rows(),
            request_start_date=date(2026, 6, 12),
            request_end_date=date(2026, 6, 15),
        )
    )

    assert first_date == date(2026, 6, 12)
    assert last_date == date(2026, 6, 15)


def test_validate_price_rows_rejects_outside_window():
    rows = [
        {
            "date": "2026-06-16T00:00:00.000Z",
        }
    ]

    with pytest.raises(
        ValueError,
        match="outside the request window",
    ):
        validate_price_rows_for_window(
            rows,
            request_start_date=date(2026, 6, 12),
            request_end_date=date(2026, 6, 15),
        )


def test_validate_price_rows_rejects_duplicate_dates():
    rows = [
        {
            "date": "2026-06-12T00:00:00.000Z",
        },
        {
            "date": "2026-06-12T00:00:00.000Z",
        },
    ]

    with pytest.raises(
        ValueError,
        match="duplicate dates",
    ):
        validate_price_rows_for_window(
            rows,
            request_start_date=date(2026, 6, 12),
            request_end_date=date(2026, 6, 15),
        )


def test_process_task_downloads_to_windowed_path(
    tmp_path,
):
    calls: list[dict[str, Any]] = []

    def fake_fetch(**kwargs):
        calls.append(kwargs)
        return make_rows()

    settings = PriceDownloadSettings()
    ods_root = tmp_path / "data" / "ods"

    result = process_price_download_task(
        make_task(),
        client_config=TiingoClientConfig(
            api_token="secret",
            max_attempts=1,
        ),
        settings=settings,
        ods_root=ods_root,
        fetch_fn=fake_fetch,
    )

    expected_path = build_windowed_price_raw_path(
        ods_root=ods_root,
        ticker="AAPL",
        request_start_date="2026-06-12",
        request_end_date="2026-06-15",
        filename="prices.json",
    )

    assert result["status"] == "downloaded"
    assert result["api_called"] is True
    assert result["row_count"] == 2
    assert len(calls) == 1
    assert expected_path.exists()
    assert read_json_rows(expected_path) == make_rows()


def test_process_task_reuses_existing_file(
    tmp_path,
):
    ods_root = tmp_path / "data" / "ods"

    path = build_windowed_price_raw_path(
        ods_root=ods_root,
        ticker="AAPL",
        request_start_date="2026-06-12",
        request_end_date="2026-06-15",
    )

    write_json_rows(path, make_rows())

    def fail_if_called(**kwargs):
        raise AssertionError(
            f"fetch should not be called: {kwargs}"
        )

    result = process_price_download_task(
        make_task(),
        client_config=TiingoClientConfig(
            api_token="secret",
            max_attempts=1,
        ),
        settings=PriceDownloadSettings(),
        ods_root=ods_root,
        fetch_fn=fail_if_called,
    )

    assert result["status"] == "existing"
    assert result["api_called"] is False


def test_process_task_overwrite_refetches_existing_file(
    tmp_path,
):
    ods_root = tmp_path / "data" / "ods"

    path = build_windowed_price_raw_path(
        ods_root=ods_root,
        ticker="AAPL",
        request_start_date="2026-06-12",
        request_end_date="2026-06-15",
    )

    write_json_rows(
        path,
        [
            {
                "date": "2026-06-12T00:00:00.000Z",
                "close": 1.0,
            }
        ],
    )

    calls: list[bool] = []

    def fake_fetch(**kwargs):
        calls.append(True)
        return make_rows()

    result = process_price_download_task(
        make_task(),
        client_config=TiingoClientConfig(
            api_token="secret",
            max_attempts=1,
        ),
        settings=PriceDownloadSettings(),
        ods_root=ods_root,
        overwrite=True,
        fetch_fn=fake_fetch,
    )

    assert result["status"] == "downloaded"
    assert result["api_called"] is True
    assert calls == [True]
    assert read_json_rows(path) == make_rows()


def test_process_task_writes_empty_response(
    tmp_path,
):
    ods_root = tmp_path / "data" / "ods"

    def fake_fetch(**kwargs):
        return []

    result = process_price_download_task(
        make_task(),
        client_config=TiingoClientConfig(
            api_token="secret",
            max_attempts=1,
        ),
        settings=PriceDownloadSettings(),
        ods_root=ods_root,
        fetch_fn=fake_fetch,
    )

    path = Path(result["local_path"])

    assert result["status"] == "empty"
    assert result["row_count"] == 0
    assert path.exists()
    assert read_json_rows(path) == []


def test_process_task_uploads_existing_file(
    tmp_path,
):
    ods_root = tmp_path / "data" / "ods"

    path = build_windowed_price_raw_path(
        ods_root=ods_root,
        ticker="AAPL",
        request_start_date="2026-06-12",
        request_end_date="2026-06-15",
    )

    write_json_rows(path, make_rows())

    upload_calls: list[Path] = []

    def fake_upload(
        *,
        bucket,
        local_path,
    ):
        upload_calls.append(Path(local_path))
        return "gs://test-bucket/window/prices.json"

    result = process_price_download_task(
        make_task(),
        client_config=TiingoClientConfig(
            api_token="secret",
            max_attempts=1,
        ),
        settings=PriceDownloadSettings(),
        ods_root=ods_root,
        bucket=object(),
        upload_fn=fake_upload,
    )

    assert result["status"] == "existing"
    assert result["api_called"] is False
    assert result["uploaded_to_gcs"] is True
    assert upload_calls == [path]


def test_build_price_download_plan(tmp_path):
    tasks = pd.DataFrame([make_task()])

    plan = build_price_download_plan(
        tasks,
        ods_root=tmp_path / "data" / "ods",
    )

    assert len(plan) == 1
    assert plan.loc[0, "file_exists"] == False  # noqa: E712
    assert plan.loc[0, "would_call_api"] == True  # noqa: E712
    assert (
        "request_start=2026-06-12"
        in plan.loc[0, "local_path"]
    )

    assert not bool(plan.loc[0, "file_exists"])
    assert bool(plan.loc[0, "would_call_api"])

def test_run_price_download_tasks_calls_result_callback(tmp_path):
    tasks = pd.DataFrame([make_task()])
    callback_results: list[dict] = []

    def fake_fetch(**kwargs):
        return make_rows()

    settings = PriceDownloadSettings()
    ods_root = tmp_path / "data" / "ods"

    client_config = TiingoClientConfig(
        api_token="secret",
        max_attempts=1,
    )

    results = run_price_download_tasks(
        tasks,
        client_config=client_config,
        settings=settings,
        ods_root=ods_root,
        fetch_fn=fake_fetch,  # remove this line if your function does not expose fetch_fn
        result_callback=callback_results.append,
    )

    assert len(results) == 1
    assert len(callback_results) == 1
    assert callback_results[0]["ticker"] == "AAPL"
    assert callback_results[0]["status"] == "downloaded"