from __future__ import annotations

from pathlib import Path
import os

from dotenv import load_dotenv
from google.cloud import storage


LOCAL_ROOTS = [
    Path("data/ods/source=tiingo/dataset=equity_price_daily"),
    Path("data/dwd/equity_price_daily"),
]


def upload_file(bucket: storage.Bucket, local_path: Path) -> None:
    object_name = local_path.as_posix()

    blob = bucket.blob(object_name)
    blob.upload_from_filename(str(local_path))

    print(f"Uploaded {local_path} to gs://{bucket.name}/{object_name}")


def main() -> None:
    load_dotenv()

    bucket_name = os.environ.get("GCS_BUCKET")
    project_id = os.environ.get("GCP_PROJECT_ID")

    if not bucket_name:
        raise RuntimeError("GCS_BUCKET is missing. Check your .env file.")
    if not project_id:
        raise RuntimeError("GCP_PROJECT_ID is missing. Check your .env file.")

    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)

    allowed_suffixes = {".json", ".csv", ".parquet"}

    for root in LOCAL_ROOTS:
        if not root.exists():
            print(f"Skipping missing root: {root}")
            continue

        for file_path in root.rglob("*"):
            if file_path.is_file() and file_path.suffix in allowed_suffixes:
                upload_file(bucket, file_path)


if __name__ == "__main__":
    main()