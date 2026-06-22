from __future__ import annotations

import argparse
import os
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from google.cloud import storage

from quant_platform.storage.gcs_sync import (
    ALLOWED_SUFFIXES,
    build_upload_plan,
    execute_upload_plan,
    gcs_object_name_from_local_path,
    upload_file as upload_gcs_file,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"

LOCAL_DATA_ROOT = PROJECT_ROOT / "data"

DEFAULT_LOCAL_ROOTS = [
    (
        PROJECT_ROOT
        / "data"
        / "ods"
        / "source=tiingo"
        / "dataset=equity_price_daily"
    ),
    PROJECT_ROOT / "data" / "dwd" / "equity_price_daily",
]


def upload_file(
    bucket: Any,
    local_path: Path,
    dry_run: bool = False,
) -> None:
    """
    Backward-compatible wrapper for the old script-level upload function.
    """
    object_name = gcs_object_name_from_local_path(
        local_path=local_path,
        local_data_root=LOCAL_DATA_ROOT,
        project_root=PROJECT_ROOT,
    )

    if dry_run:
        print(
            f"[DRY RUN] Would upload {local_path} "
            f"to gs://{bucket.name}/{object_name}"
        )
        return

    uri = upload_gcs_file(
        bucket=bucket,
        local_path=local_path,
        object_name=object_name,
        local_data_root=LOCAL_DATA_ROOT,
        project_root=PROJECT_ROOT,
    )

    print(f"Uploaded {local_path} to {uri}")


def sync_data_to_gcs(
    local_roots: Iterable[Path] | None = None,
    dry_run: bool = False,
) -> int:
    """
    Sync selected local data-lake files to GCS.

    In dry-run mode, credentials are not loaded and no GCP client is created.
    """
    roots = (
        list(local_roots)
        if local_roots is not None
        else DEFAULT_LOCAL_ROOTS
    )

    upload_plan = build_upload_plan(
        local_roots=roots,
        local_data_root=LOCAL_DATA_ROOT,
        allowed_suffixes=ALLOWED_SUFFIXES,
        project_root=PROJECT_ROOT,
    )

    if dry_run:
        for item in upload_plan:
            print(
                "[DRY RUN] Would upload "
                f"{item.local_path} to "
                f"gs://<configured-bucket>/{item.object_name}"
            )

        print(f"[DRY RUN] Planned uploads: {len(upload_plan)}")
        return len(upload_plan)

    load_dotenv(dotenv_path=ENV_PATH)

    bucket_name = os.getenv("GCS_BUCKET")
    project_id = os.getenv("GCP_PROJECT_ID")

    if not bucket_name:
        raise RuntimeError(
            "GCS_BUCKET is missing. Check your .env file."
        )

    if not project_id:
        raise RuntimeError(
            "GCP_PROJECT_ID is missing. Check your .env file."
        )

    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)

    uploaded_uris = execute_upload_plan(
        bucket=bucket,
        upload_plan=upload_plan,
    )

    for uri in uploaded_uris:
        print(f"Uploaded: {uri}")

    print(f"Uploaded files: {len(uploaded_uris)}")

    return len(uploaded_uris)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync local data files to GCS."
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned uploads without uploading files.",
    )

    parser.add_argument(
        "--local-root",
        action="append",
        type=Path,
        default=None,
        help=(
            "Optional local file or directory to sync. "
            "May be specified multiple times."
        ),
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    sync_data_to_gcs(
        local_roots=args.local_root,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()