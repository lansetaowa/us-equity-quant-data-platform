from __future__ import annotations

import json

import pandas as pd
import pytest

from quant_platform.metadata.price_update import (
    build_price_update_run_summary,
    load_end_to_end_artifact_summary,
    load_price_update_report,
    split_tasks_for_run_resume,
)

from datetime import date

def write_report(tmp_path):
    path = tmp_path / "price_download_test.csv"

    pd.DataFrame(
        [
            {
                "source": "tiingo",
                "dataset_name": "equity_price_daily",
                "ticker": "AAPL",
                "security_id": "tiingo:AAPL",
                "request_start_date": "2026-06-12",
                "request_end_date": "2026-06-12",
                "status": "downloaded",
                "row_count": 1,
                "first_price_date": "2026-06-12",
                "last_price_date": "2026-06-12",
                "api_called": True,
                "uploaded_to_gcs": True,
                "local_path": "aapl.json",
                "gcs_uri": "gs://bucket/aapl.json",
                "error_message": None,
                "completed_at_utc": "2026-06-12T22:00:00Z",
            },
            {
                "source": "tiingo",
                "dataset_name": "equity_price_daily",
                "ticker": "MSFT",
                "security_id": "tiingo:MSFT",
                "request_start_date": "2026-06-12",
                "request_end_date": "2026-06-12",
                "status": "existing",
                "row_count": 1,
                "first_price_date": "2026-06-12",
                "last_price_date": "2026-06-12",
                "api_called": False,
                "uploaded_to_gcs": True,
                "local_path": "msft.json",
                "gcs_uri": "gs://bucket/msft.json",
                "error_message": None,
                "completed_at_utc": "2026-06-12T22:01:00Z",
            },
            {
                "source": "tiingo",
                "dataset_name": "equity_price_daily",
                "ticker": "EMPTY",
                "security_id": "tiingo:EMPTY",
                "request_start_date": "2026-06-12",
                "request_end_date": "2026-06-12",
                "status": "empty",
                "row_count": 0,
                "first_price_date": None,
                "last_price_date": None,
                "api_called": True,
                "uploaded_to_gcs": True,
                "local_path": "empty.json",
                "gcs_uri": "gs://bucket/empty.json",
                "error_message": None,
                "completed_at_utc": "2026-06-12T22:02:00Z",
            },
        ]
    ).to_csv(path, index=False)

    return path


def write_artifacts(tmp_path):
    root = tmp_path / "transform"
    root.mkdir()

    (root / "prepare_summary.json").write_text(
        json.dumps(
            {
                "normalized_new_row_count": 2,
                "affected_partition_count": 1,
            }
        ),
        encoding="utf-8",
    )

    (root / "promotion_complete.json").write_text(
        json.dumps({"affected_partition_count": 1}),
        encoding="utf-8",
    )

    pd.DataFrame(
        [
            {
                "year": 2026,
                "month": 6,
                "status": "in_sync",
            }
        ]
    ).to_csv(
        root / "gcs_post_sync_validation.csv",
        index=False,
    )

    (root / "bigquery_apply_summary.json").write_text(
        json.dumps(
            {
                "applied_transaction": True,
                "target_validation": {
                    "duplicate_groups": 0,
                    "missing_keys": 0,
                    "extra_keys": 0,
                    "mismatched_rows": 0,
                },
                "global_after": {"n_rows": 100},
            }
        ),
        encoding="utf-8",
    )

    return root


def test_load_price_update_report(tmp_path):
    report = load_price_update_report(write_report(tmp_path))

    assert len(report) == 3

    assert report["persistent_status"].value_counts().to_dict() == {
        "success": 2,
        "empty": 1,
    }

    assert int(report["api_called"].sum()) == 2


def test_build_price_update_run_summary(tmp_path):
    report_path = write_report(tmp_path)
    report = load_price_update_report(report_path)
    artifacts = load_end_to_end_artifact_summary(write_artifacts(tmp_path))

    summary = build_price_update_run_summary(
        report,
        report_path=report_path,
        artifact_summary=artifacts,
        audit_report_path=tmp_path / "audit",
    )

    assert summary.pipeline_status == "success"
    assert summary.symbols_count == 3
    assert summary.ods_records == 2
    assert summary.dwd_records == 2
    assert summary.metrics["api_call_count"] == 2
    assert summary.metrics["empty_window_count"] == 1

def test_normalize_price_update_result_frame_accepts_dataframe(tmp_path):
    report_path = write_report(tmp_path)
    raw = pd.read_csv(report_path)

    from quant_platform.metadata.price_update import (
        normalize_price_update_result_frame,
    )

    result = normalize_price_update_result_frame(raw)

    assert len(result) == 3
    assert result["persistent_status"].value_counts().to_dict() == {
        "success": 2,
        "empty": 1,
    }

def test_empty_action_requires_zero_rows(tmp_path):
    report_path = write_report(tmp_path)
    df = pd.read_csv(report_path)

    df.loc[df["status"] == "empty", "row_count"] = 1
    df.to_csv(report_path, index=False)

    with pytest.raises(ValueError, match="Empty actions must have"):
        load_price_update_report(report_path)


def test_artifact_validation_rejects_bad_gcs(tmp_path):
    root = write_artifacts(tmp_path)

    pd.DataFrame(
        [
            {
                "year": 2026,
                "month": 6,
                "status": "needs_sync",
            }
        ]
    ).to_csv(
        root / "gcs_post_sync_validation.csv",
        index=False,
    )

    with pytest.raises(ValueError, match="GCS post-sync validation"):
        load_end_to_end_artifact_summary(root)

def test_split_tasks_for_run_resume_skips_completed_and_retries_failed():
    tasks = pd.DataFrame(
        [
            {
                "source": "tiingo",
                "dataset_name": "equity_price_daily",
                "ticker": "AAPL",
                "security_id": "tiingo:AAPL",
                "request_start_date": date(2026, 6, 23),
                "request_end_date": date(2026, 7, 17),
            },
            {
                "source": "tiingo",
                "dataset_name": "equity_price_daily",
                "ticker": "EMPTY",
                "security_id": "tiingo:EMPTY",
                "request_start_date": date(2026, 6, 23),
                "request_end_date": date(2026, 7, 17),
            },
            {
                "source": "tiingo",
                "dataset_name": "equity_price_daily",
                "ticker": "FAILME",
                "security_id": "tiingo:FAILME",
                "request_start_date": date(2026, 6, 23),
                "request_end_date": date(2026, 7, 17),
            },
            {
                "source": "tiingo",
                "dataset_name": "equity_price_daily",
                "ticker": "NEWONE",
                "security_id": "tiingo:NEWONE",
                "request_start_date": date(2026, 6, 23),
                "request_end_date": date(2026, 7, 17),
            },
        ]
    )

    existing_results = pd.DataFrame(
        [
            {
                "source": "tiingo",
                "dataset_name": "equity_price_daily",
                "ticker": "AAPL",
                "security_id": "tiingo:AAPL",
                "request_start_date": date(2026, 6, 23),
                "request_end_date": date(2026, 7, 17),
                "status": "success",
                "action": "downloaded",
                "row_count": 18,
                "completed_at_utc": "2026-07-17T23:00:00Z",
            },
            {
                "source": "tiingo",
                "dataset_name": "equity_price_daily",
                "ticker": "EMPTY",
                "security_id": "tiingo:EMPTY",
                "request_start_date": date(2026, 6, 23),
                "request_end_date": date(2026, 7, 17),
                "status": "empty",
                "action": "empty",
                "row_count": 0,
                "completed_at_utc": "2026-07-17T23:01:00Z",
            },
            {
                "source": "tiingo",
                "dataset_name": "equity_price_daily",
                "ticker": "FAILME",
                "security_id": "tiingo:FAILME",
                "request_start_date": date(2026, 6, 23),
                "request_end_date": date(2026, 7, 17),
                "status": "failed",
                "action": "failed",
                "row_count": None,
                "completed_at_utc": "2026-07-17T23:02:00Z",
            },
        ]
    )

    pending, completed = split_tasks_for_run_resume(
        tasks,
        existing_results,
    )

    assert set(pending["ticker"]) == {"FAILME", "NEWONE"}
    assert set(completed["ticker"]) == {"AAPL", "EMPTY"}

    by_ticker = completed.set_index("ticker")

    assert by_ticker.loc["AAPL", "prior_action"] == "downloaded"
    assert by_ticker.loc["EMPTY", "prior_action"] == "empty"

def test_split_tasks_for_run_resume_rejects_duplicate_existing_results():
    tasks = pd.DataFrame(
        [
            {
                "source": "tiingo",
                "dataset_name": "equity_price_daily",
                "ticker": "AAPL",
                "security_id": "tiingo:AAPL",
                "request_start_date": date(2026, 6, 23),
                "request_end_date": date(2026, 7, 17),
            }
        ]
    )

    existing_results = pd.DataFrame(
        [
            {
                "source": "tiingo",
                "dataset_name": "equity_price_daily",
                "ticker": "AAPL",
                "security_id": "tiingo:AAPL",
                "request_start_date": date(2026, 6, 23),
                "request_end_date": date(2026, 7, 17),
                "status": "success",
                "action": "downloaded",
                "row_count": 18,
                "completed_at_utc": "2026-07-17T23:00:00Z",
            },
            {
                "source": "tiingo",
                "dataset_name": "equity_price_daily",
                "ticker": "AAPL",
                "security_id": "tiingo:AAPL",
                "request_start_date": date(2026, 6, 23),
                "request_end_date": date(2026, 7, 17),
                "status": "success",
                "action": "existing",
                "row_count": 18,
                "completed_at_utc": "2026-07-17T23:01:00Z",
            },
        ]
    )

    with pytest.raises(ValueError, match="duplicate task keys"):
        split_tasks_for_run_resume(
            tasks,
            existing_results,
        )