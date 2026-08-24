from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd

from quant_platform.research.operation import (
    build_partition_manifest,
    default_operation_id,
    default_run_id,
    write_build_reports,
)


def test_default_run_id_has_prefix():
    run_id = default_run_id("market_context_price")

    assert run_id.startswith("market_context_price_")


def test_default_operation_id_has_prefix():
    operation_id = default_operation_id()

    assert operation_id.startswith("daily_data_")


def test_build_partition_manifest():
    frame = pd.DataFrame(
        {
            "date": [
                date(2026, 7, 1),
                date(2026, 7, 2),
                date(2026, 8, 1),
            ],
            "ticker": ["SPY", "QQQ", "SPY"],
        }
    )

    paths = [
        Path(
            "data/dwd/market_context_price_daily/"
            "context_set=core_v1/year=2026/month=07/part-000.parquet"
        ),
        Path(
            "data/dwd/market_context_price_daily/"
            "context_set=core_v1/year=2026/month=08/part-000.parquet"
        ),
    ]

    manifest = build_partition_manifest(
        frame=frame,
        date_column="date",
        output_paths=paths,
    )

    assert manifest["year"].tolist() == [2026, 2026]
    assert manifest["month"].tolist() == [7, 8]
    assert manifest["row_count"].tolist() == [2, 1]
    assert manifest["output_path"].str.contains("part-000.parquet").all()


def test_build_partition_manifest_empty():
    manifest = build_partition_manifest(
        frame=pd.DataFrame(columns=["date"]),
        date_column="date",
        output_paths=[],
    )

    assert manifest.empty
    assert manifest.columns.tolist() == [
        "year",
        "month",
        "row_count",
        "output_path",
    ]


def test_write_build_reports(tmp_path):
    manifest = pd.DataFrame(
        {
            "year": [2026],
            "month": [8],
            "row_count": [17],
            "output_path": ["data/dwd/example.parquet"],
        }
    )

    report_dir = write_build_reports(
        report_root=tmp_path / "reports",
        run_id="market_context_price_test",
        summary={
            "run_id": "market_context_price_test",
            "operation_id": "daily_data_test",
            "status": "success",
        },
        partition_manifest=manifest,
    )

    summary_path = report_dir / "summary.json"
    manifest_path = report_dir / "partition_manifest.csv"

    assert summary_path.exists()
    assert manifest_path.exists()

    summary = json.loads(summary_path.read_text(encoding="utf-8"))

    assert summary["run_id"] == "market_context_price_test"
    assert summary["status"] == "success"

    loaded_manifest = pd.read_csv(manifest_path)

    assert loaded_manifest["row_count"].tolist() == [17]