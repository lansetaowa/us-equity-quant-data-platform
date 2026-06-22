from __future__ import annotations

# from pathlib import Path

from quant_platform.storage.gcs_sync import (
    GcsUploadItem,
    build_upload_plan,
    execute_upload_plan,
    iter_files_to_upload,
    upload_file,
)


class FakeBlob:
    def __init__(self, name: str) -> None:
        self.name = name
        self.uploaded_filename: str | None = None

    def upload_from_filename(self, filename: str) -> None:
        self.uploaded_filename = filename


class FakeBucket:
    def __init__(self, name: str) -> None:
        self.name = name
        self.blobs: dict[str, FakeBlob] = {}

    def blob(self, object_name: str) -> FakeBlob:
        blob = FakeBlob(object_name)
        self.blobs[object_name] = blob
        return blob


def test_iter_files_to_upload_filters_suffixes(tmp_path):
    data_root = tmp_path / "data"
    root = data_root / "ods"

    json_path = root / "a.json"
    csv_path = root / "b.csv"
    parquet_path = root / "c.parquet"
    ignored_path = root / "notes.txt"

    root.mkdir(parents=True)

    json_path.write_text("[]", encoding="utf-8")
    csv_path.write_text("ticker\nAAPL\n", encoding="utf-8")
    parquet_path.write_bytes(b"placeholder")
    ignored_path.write_text("ignore", encoding="utf-8")

    files = list(
        iter_files_to_upload(
            [root],
            project_root=tmp_path,
        )
    )

    assert files == sorted(
        [
            json_path.resolve(),
            csv_path.resolve(),
            parquet_path.resolve(),
        ]
    )


def test_build_upload_plan(tmp_path):
    data_root = tmp_path / "data"
    file_path = (
        data_root
        / "ods"
        / "source=tiingo"
        / "dataset=equity_price_daily"
        / "symbol=AAPL"
        / "prices.json"
    )

    file_path.parent.mkdir(parents=True)
    file_path.write_text("[]", encoding="utf-8")

    plan = build_upload_plan(
        local_roots=[file_path],
        local_data_root=data_root,
        project_root=tmp_path,
    )

    assert len(plan) == 1
    assert plan[0].local_path == file_path.resolve()
    assert plan[0].object_name == (
        "ods/source=tiingo/dataset=equity_price_daily/"
        "symbol=AAPL/prices.json"
    )


def test_upload_file_uses_existing_bucket(tmp_path):
    data_root = tmp_path / "data"
    file_path = data_root / "ods" / "prices.json"

    file_path.parent.mkdir(parents=True)
    file_path.write_text("[]", encoding="utf-8")

    bucket = FakeBucket("test-bucket")

    uri = upload_file(
        bucket=bucket,
        local_path=file_path,
        object_name="ods/prices.json",
        local_data_root=data_root,
        project_root=tmp_path,
    )

    assert uri == "gs://test-bucket/ods/prices.json"

    blob = bucket.blobs["ods/prices.json"]

    assert blob.uploaded_filename == str(file_path.resolve())


def test_execute_upload_plan(tmp_path):
    file_a = tmp_path / "a.json"
    file_b = tmp_path / "b.json"

    file_a.write_text("[]", encoding="utf-8")
    file_b.write_text("[]", encoding="utf-8")

    plan = [
        GcsUploadItem(
            local_path=file_a,
            object_name="ods/a.json",
        ),
        GcsUploadItem(
            local_path=file_b,
            object_name="ods/b.json",
        ),
    ]

    bucket = FakeBucket("test-bucket")

    uris = execute_upload_plan(
        bucket=bucket,
        upload_plan=plan,
    )

    assert uris == [
        "gs://test-bucket/ods/a.json",
        "gs://test-bucket/ods/b.json",
    ]