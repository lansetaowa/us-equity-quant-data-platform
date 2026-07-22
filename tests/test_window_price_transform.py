from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from quant_platform.prices.normalize import (
    normalize_tiingo_price_rows,
)
from quant_platform.prices.transform import (
    write_year_month_partitions,
)
from quant_platform.prices.window_transform import (
    load_download_report,
    prepare_windowed_dwd_update,
    promote_windowed_dwd_update,
    normalize_window_files,
)
from quant_platform.storage.local_json import (
    write_json_rows,
)

from datetime import datetime, timezone



def raw_row(
    row_date: str,
    close: float,
) -> dict:
    return {
        "date": row_date,
        "open": close - 1,
        "high": close + 1,
        "low": close - 2,
        "close": close,
        "volume": 1_000,
        "adjOpen": close - 1,
        "adjHigh": close + 1,
        "adjLow": close - 2,
        "adjClose": close,
        "adjVolume": 1_000,
        "divCash": 0.0,
        "splitFactor": 1.0,
    }


def existing_frame() -> pd.DataFrame:
    return normalize_tiingo_price_rows(
        raw_rows=[
            raw_row(
                "2026-06-11T00:00:00Z",
                100.0,
            )
        ],
        ticker="AAPL",
        security_id="tiingo:AAPL",
        load_id="bootstrap",
        loaded_at="2026-06-12T00:00:00Z",
    )


def build_report(
    tmp_path: Path,
) -> Path:
    raw_path = (
        tmp_path
        / "data"
        / "ods"
        / "source=tiingo"
        / "dataset=equity_price_daily"
        / "symbol=AAPL"
        / "request_start=2026-06-12"
        / "request_end=2026-06-12"
        / "prices.json"
    )

    write_json_rows(
        raw_path,
        [
            raw_row(
                "2026-06-12T00:00:00Z",
                101.0,
            )
        ],
    )

    report_path = (
        tmp_path
        / "reports"
        / "price_download_test.csv"
    )
    report_path.parent.mkdir(
        parents=True
    )

    pd.DataFrame(
        [
            {
                "source": "tiingo",
                "dataset_name": (
                    "equity_price_daily"
                ),
                "ticker": "AAPL",
                "security_id": "tiingo:AAPL",
                "request_start_date": (
                    "2026-06-12"
                ),
                "request_end_date": (
                    "2026-06-12"
                ),
                "status": "downloaded",
                "row_count": 1,
                "local_path": (
                    raw_path.as_posix()
                ),
            }
        ]
    ).to_csv(
        report_path,
        index=False,
    )

    return report_path


def test_load_download_report_rejects_failed(
    tmp_path,
):
    report_path = tmp_path / "failed.csv"

    pd.DataFrame(
        [
            {
                "source": "tiingo",
                "dataset_name": (
                    "equity_price_daily"
                ),
                "ticker": "AAPL",
                "security_id": "tiingo:AAPL",
                "request_start_date": (
                    "2026-06-12"
                ),
                "request_end_date": (
                    "2026-06-12"
                ),
                "status": "failed",
                "row_count": 0,
                "local_path": "missing.json",
            }
        ]
    ).to_csv(
        report_path,
        index=False,
    )

    with pytest.raises(
        ValueError,
        match="non-terminal statuses",
    ):
        load_download_report(report_path)


def test_prepare_merges_existing_and_new_rows(
    tmp_path,
):
    dwd_root = (
        tmp_path
        / "data"
        / "dwd"
        / "equity_price_daily"
    )

    write_year_month_partitions(
        existing_frame(),
        dwd_root,
    )

    report_path = build_report(tmp_path)

    paths = prepare_windowed_dwd_update(
        report_path,
        dwd_root=dwd_root,
        staging_base=(
            tmp_path / "data" / "_tmp"
        ),
        archive_base=(
            tmp_path / "data" / "_archive"
        ),
        report_base=(
            tmp_path / "reports" / "transform"
        ),
    )

    staged_path = (
        paths.staging_root
        / "year=2026"
        / "month=06"
        / "part-000.parquet"
    )

    staged = pd.read_parquet(
        staged_path
    )

    assert len(staged) == 2
    assert set(
        pd.to_datetime(
            staged["date"]
        ).dt.date
    ) == {
        pd.Timestamp("2026-06-11").date(),
        pd.Timestamp("2026-06-12").date(),
    }

    manifest = pd.read_csv(
        paths.report_dir
        / "partition_manifest.csv"
    )

    assert manifest.loc[
        0,
        "existing_row_count",
    ] == 1
    assert manifest.loc[
        0,
        "inserted_key_count",
    ] == 1
    assert manifest.loc[
        0,
        "final_row_count",
    ] == 2


def test_promote_archives_and_replaces_partition(
    tmp_path,
):
    dwd_root = (
        tmp_path
        / "data"
        / "dwd"
        / "equity_price_daily"
    )

    write_year_month_partitions(
        existing_frame(),
        dwd_root,
    )

    report_path = build_report(tmp_path)

    staging_base = (
        tmp_path / "data" / "_tmp"
    )
    archive_base = (
        tmp_path / "data" / "_archive"
    )
    report_base = (
        tmp_path / "reports" / "transform"
    )

    paths = prepare_windowed_dwd_update(
        report_path,
        dwd_root=dwd_root,
        staging_base=staging_base,
        archive_base=archive_base,
        report_base=report_base,
    )

    promote_windowed_dwd_update(
        report_path,
        dwd_root=dwd_root,
        staging_base=staging_base,
        archive_base=archive_base,
        report_base=report_base,
    )

    final_path = (
        dwd_root
        / "year=2026"
        / "month=06"
        / "part-000.parquet"
    )
    archive_path = (
        paths.archive_root
        / "year=2026"
        / "month=06"
        / "part-000.parquet"
    )

    final = pd.read_parquet(
        final_path
    )
    archived = pd.read_parquet(
        archive_path
    )

    assert len(final) == 2
    assert len(archived) == 1

    assert (
        paths.report_dir
        / "promotion_complete.json"
    ).exists()

def test_normalize_window_files_accepts_skipped_without_raw_file():
    report = pd.DataFrame(
        [
            {
                "source": "tiingo",
                "dataset_name": "equity_price_daily",
                "ticker": "ATLN",
                "security_id": "tiingo:ATLN",
                "request_start_date": pd.Timestamp(
                    "2026-06-23"
                ).date(),
                "request_end_date": pd.Timestamp(
                    "2026-07-17"
                ).date(),
                "status": "skipped",
                "row_count": 0,
                "local_path": None,
            }
        ]
    )

    normalized, audit = normalize_window_files(
        report,
        load_id="test-skipped",
        loaded_at=datetime.now(timezone.utc),
    )

    assert normalized.empty
    assert len(audit) == 1
    assert audit.loc[0, "ticker"] == "ATLN"
    assert audit.loc[0, "status"] == "skipped"
    assert audit.loc[0, "raw_row_count"] == 0
    assert audit.loc[0, "normalized_row_count"] == 0

def test_normalize_window_files_rejects_skipped_nonzero_row_count():
    report = pd.DataFrame(
        [
            {
                "source": "tiingo",
                "dataset_name": "equity_price_daily",
                "ticker": "ATLN",
                "security_id": "tiingo:ATLN",
                "request_start_date": pd.Timestamp(
                    "2026-06-23"
                ).date(),
                "request_end_date": pd.Timestamp(
                    "2026-07-17"
                ).date(),
                "status": "skipped",
                "row_count": 1,
                "local_path": None,
            }
        ]
    )

    with pytest.raises(
        ValueError,
        match="Skipped window has non-zero row_count",
    ):
        normalize_window_files(
            report,
            load_id="test-skipped-invalid",
            loaded_at=datetime.now(timezone.utc),
        )