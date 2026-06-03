from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Iterable

from dotenv import load_dotenv
from google.cloud import storage


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOCAL_DATA_ROOT = PROJECT_ROOT / "data"

DEFAULT_LOCAL_ROOTS = [
    PROJECT_ROOT / "data" / "ods" / "source=tiingo" / "dataset=equity_price_daily",
    PROJECT_ROOT / "data" / "dwd" / "equity_price_daily",
]

ALLOWED_SUFFIXES = {".json", ".csv", ".parquet"}


def gcs_object_name_from_local_path(
    local_path: Path,
    local_data_root: Path = LOCAL_DATA_ROOT,
) -> str:
    """
    Convert a local data-lake path to a GCS object name.

    Example:
        data/ods/source=tiingo/file.json
        -> ods/source=tiingo/file.json

        data/dwd/equity_price_daily/year=2025/part-000.parquet
        -> dwd/equity_price_daily/year=2025/part-000.parquet

    The local 'data/' prefix should never appear in GCS.
    """
    path = Path(local_path)

    if not path.is_absolute():
        path = PROJECT_ROOT / path

    path = path.resolve()
    local_data_root = local_data_root.resolve()

    try:
        relative_path = path.relative_to(local_data_root)
    except ValueError as exc:
        raise ValueError(
            f"Local file must be under {local_data_root}, got {path}"
        ) from exc

    return relative_path.as_posix()


def upload_file(
    bucket: storage.Bucket,
    local_path: Path,
    dry_run: bool = False,
) -> None:
    """Upload one local data file to the corresponding GCS layer path."""
    object_name = gcs_object_name_from_local_path(local_path)

    if dry_run:
        print(f"[DRY RUN] Would upload {local_path} to gs://{bucket.name}/{object_name}")
        return

    blob = bucket.blob(object_name)
    blob.upload_from_filename(str(local_path))
    print(f"Uploaded {local_path} to gs://{bucket.name}/{object_name}")


def iter_files_to_upload(local_roots: Iterable[Path]) -> Iterable[Path]:
    """Yield allowed files under configured local roots."""
    for root in local_roots:
        root = Path(root)

        if not root.is_absolute():
            root = PROJECT_ROOT / root

        if not root.exists():
            print(f"Skipping missing root: {root}")
            continue

        for file_path in root.rglob("*"):
            if file_path.is_file() and file_path.suffix in ALLOWED_SUFFIXES:
                yield file_path


def sync_data_to_gcs(
    local_roots: Iterable[Path] | None = None,
    dry_run: bool = False,
) -> None:
    """
    Sync selected local data-lake files to GCS.

    Local paths keep the local 'data/' prefix.
    GCS object names intentionally strip that prefix.
    """
    load_dotenv()

    bucket_name = os.environ.get("GCS_BUCKET")
    project_id = os.environ.get("GCP_PROJECT_ID")

    if not bucket_name:
        raise RuntimeError("GCS_BUCKET is missing. Check your .env file.")

    if not project_id:
        raise RuntimeError("GCP_PROJECT_ID is missing. Check your .env file.")

    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)

    roots = list(local_roots) if local_roots is not None else DEFAULT_LOCAL_ROOTS

    uploaded_count = 0

    for file_path in iter_files_to_upload(roots):
        upload_file(bucket=bucket, local_path=file_path, dry_run=dry_run)
        uploaded_count += 1

    if dry_run:
        print(f"[DRY RUN] Planned uploads: {uploaded_count}")
    else:
        print(f"Uploaded files: {uploaded_count}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync local data files to GCS.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned uploads without uploading files.",
    )
    args = parser.parse_args()

    sync_data_to_gcs(dry_run=args.dry_run)


if __name__ == "__main__":
    main()