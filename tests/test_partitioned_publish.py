from __future__ import annotations

from pathlib import Path

import pandas as pd

from quant_platform.warehouse.partitioned_publish import (
    PartitionedTableSpec,
    build_staging_table_id,
    build_table_id,
    discover_local_partition_files,
    parse_month_start,
)


def test_parse_month_start():
    assert parse_month_start(
        "2026-07",
        field_name="month",
    ) == pd.Timestamp("2026-07-01").date()


def test_build_table_ids():
    assert build_table_id(
        project_id="project",
        dataset_id="dataset",
        table_name="table",
    ) == "project.dataset.table"

    assert build_staging_table_id(
        project_id="project",
        dataset_id="dataset",
        table_name="table",
        suffix="2026-07",
    ) == "project.dataset.table__stg_run_2026_07"


def test_discover_local_partition_files_maps_to_gcs(tmp_path, monkeypatch):
    root = tmp_path / "data" / "dws" / "equity_liquidity_monthly"
    partition = root / "year=2026" / "month=07"
    partition.mkdir(parents=True)

    local_path = partition / "part-000.parquet"

    df = pd.DataFrame(
        {
            "metric_month": [pd.Timestamp("2026-07-01").date()],
            "security_id": ["tiingo:AAPL"],
            "ticker": ["AAPL"],
        }
    )
    df.to_parquet(local_path, index=False)

    monkeypatch.chdir(tmp_path)

    files = discover_local_partition_files(
        local_root=Path("data/dws/equity_liquidity_monthly"),
        bucket_name="bucket",
        partition_column="metric_month",
        start_month="2026-07",
        end_month="2026-07",
    )

    assert len(files) == 1
    assert files[0].local_path == Path(
        "data/dws/equity_liquidity_monthly/year=2026/month=07/part-000.parquet"
    )
    assert files[0].gcs_uri == (
        "gs://bucket/dws/equity_liquidity_monthly/"
        "year=2026/month=07/part-000.parquet"
    )
    assert files[0].row_count == 1


def test_partitioned_table_spec_is_generic():
    spec = PartitionedTableSpec(
        table_name="dws_equity_liquidity_monthly",
        partition_column="metric_month",
        key_columns=("metric_month", "security_id"),
        clustering_fields=("ticker", "security_id"),
    )

    assert spec.partition_column == "metric_month"