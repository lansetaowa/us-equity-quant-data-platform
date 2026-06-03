from pathlib import Path
import os

from dotenv import load_dotenv
from google.cloud import storage


LOCAL_FILE = Path(
    "data/dwd/equity_price_daily/year=2025/month=01/equity_price_daily.parquet"
)

GCS_OBJECT_NAME = (
    "dwd/equity_price_daily/year=2025/month=01/equity_price_daily.parquet"
)


def main() -> None:
    load_dotenv()

    bucket_name = os.environ.get("GCS_BUCKET")
    project_id = os.environ.get("GCP_PROJECT_ID")

    if not bucket_name:
        raise RuntimeError("GCS_BUCKET is missing. Check your .env file.")
    if not project_id:
        raise RuntimeError("GCP_PROJECT_ID is missing. Check your .env file.")
    if not LOCAL_FILE.exists():
        raise FileNotFoundError(
            f"{LOCAL_FILE} not found. Run: python scripts/create_sample_prices.py"
        )

    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(GCS_OBJECT_NAME)

    blob.upload_from_filename(str(LOCAL_FILE))

    print(f"Uploaded {LOCAL_FILE} to gs://{bucket_name}/{GCS_OBJECT_NAME}")


if __name__ == "__main__":
    main()