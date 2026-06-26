from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from quant_platform.prices.cloud_publish import (
    build_dwd_partition_publish_items,
    build_gcs_sync_plan,
    load_partition_manifest,
    sync_dwd_partitions_to_gcs,
)


class FakeBlob:
    def __init__(
        self,
        bucket,
        name: str,
    ) -> None:
        self.bucket = bucket
        self.name = name

    @property
    def size(self):
        payload = self.bucket.objects.get(
            self.name
        )

        return (
            len(payload)
            if payload is not None
            else None
        )

    def upload_from_filename(
        self,
        filename: str,
    ) -> None:
        self.bucket.objects[
            self.name
        ] = Path(filename).read_bytes()

    def delete(self) -> None:
        self.bucket.objects.pop(
            self.name,
            None,
        )


class FakeBucket:
    def __init__(self) -> None:
        self.name = "test-bucket"
        self.objects: dict[str, bytes] = {}

    def blob(self, name: str) -> FakeBlob:
        return FakeBlob(self, name)

    def list_blobs(self, prefix: str):
        return [
            FakeBlob(self, name)
            for name in sorted(self.objects)
            if name.startswith(prefix)
        ]


def build_fixture(tmp_path):
    data_root = tmp_path / "data"

    partition_dir = (
        data_root
        / "dwd"
        / "equity_price_daily"
        / "year=2026"
        / "month=06"
    )
    partition_dir.mkdir(parents=True)

    parquet_path = (
        partition_dir / "part-000.parquet"
    )

    pd.DataFrame(
        {
            "ticker": ["AAPL", "MSFT"],
            "value": [1, 2],
        }
    ).to_parquet(
        parquet_path,
        index=False,
    )

    report_dir = tmp_path / "reports"
    report_dir.mkdir()

    pd.DataFrame(
        [
            {
                "year": 2026,
                "month": 6,
                "final_row_count": 2,
            }
        ]
    ).to_csv(
        report_dir / "partition_manifest.csv",
        index=False,
    )

    return data_root, report_dir


def test_build_partition_publish_items(
    tmp_path,
):
    data_root, report_dir = (
        build_fixture(tmp_path)
    )

    manifest = load_partition_manifest(
        report_dir
    )

    items = build_dwd_partition_publish_items(
        manifest,
        dwd_root=(
            data_root
            / "dwd"
            / "equity_price_daily"
        ),
        local_data_root=data_root,
        project_root=tmp_path,
    )

    assert len(items) == 1
    assert items[0].expected_rows == 2
    assert items[0].gcs_prefix == (
        "dwd/equity_price_daily/"
        "year=2026/month=06/"
    )
    assert items[0].object_names == (
        "dwd/equity_price_daily/"
        "year=2026/month=06/"
        "part-000.parquet",
    )


def test_partition_row_count_mismatch(
    tmp_path,
):
    data_root, report_dir = (
        build_fixture(tmp_path)
    )

    manifest = load_partition_manifest(
        report_dir
    )
    manifest.loc[0, "final_row_count"] = 3

    with pytest.raises(
        ValueError,
        match="row count mismatch",
    ):
        build_dwd_partition_publish_items(
            manifest,
            dwd_root=(
                data_root
                / "dwd"
                / "equity_price_daily"
            ),
            local_data_root=data_root,
            project_root=tmp_path,
        )


def test_exact_sync_uploads_and_deletes_extra(
    tmp_path,
):
    data_root, report_dir = (
        build_fixture(tmp_path)
    )

    manifest = load_partition_manifest(
        report_dir
    )

    items = build_dwd_partition_publish_items(
        manifest,
        dwd_root=(
            data_root
            / "dwd"
            / "equity_price_daily"
        ),
        local_data_root=data_root,
        project_root=tmp_path,
    )

    bucket = FakeBucket()

    bucket.objects[
        (
            "dwd/equity_price_daily/"
            "year=2026/month=06/"
            "old-file.parquet"
        )
    ] = b"old"

    before = build_gcs_sync_plan(
        bucket,
        items,
    )

    assert before.loc[0, "status"] == (
        "needs_sync"
    )

    result = sync_dwd_partitions_to_gcs(
        bucket,
        items,
    )

    assert result.loc[0, "status"] == "success"
    assert result.loc[
        0,
        "deleted_extra_count",
    ] == 1

    after = build_gcs_sync_plan(
        bucket,
        items,
    )

    assert after.loc[0, "status"] == "in_sync"