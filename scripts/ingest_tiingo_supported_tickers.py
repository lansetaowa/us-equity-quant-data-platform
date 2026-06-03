from __future__ import annotations

import argparse
import io
import os
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

import pandas as pd
import yaml
from dotenv import load_dotenv
from google.cloud import storage


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"
CONFIG_PATH = PROJECT_ROOT / "configs" / "security_master.yml"

SUPPORTED_TICKERS_URL = (
    "https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip"
)


def load_config(config_path: Path = CONFIG_PATH) -> dict[str, Any]:
    """Load security master configuration."""
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    raw_text = config_path.read_text(encoding="utf-8")
    expanded_text = os.path.expandvars(raw_text)
    config = yaml.safe_load(expanded_text)

    if not isinstance(config, dict):
        raise ValueError("security_master.yml must contain a YAML mapping.")

    return config


def get_local_ods_root(config: dict[str, Any]) -> Path:
    """Resolve local ODS output directory for supported tickers."""
    try:
        local_ods_root = config["security_master"]["local_ods_root"]
    except KeyError as exc:
        raise KeyError(
            "Missing config value: security_master.local_ods_root"
        ) from exc

    return PROJECT_ROOT / local_ods_root


def download_zip_bytes(url: str = SUPPORTED_TICKERS_URL) -> bytes:
    """Download Tiingo supported_tickers.zip."""
    request = Request(
        url,
        headers={
            "User-Agent": "us-equity-quant-data-platform/1.0",
        },
    )

    with urlopen(request, timeout=60) as response:
        if response.status != 200:
            raise RuntimeError(
                f"Failed to download supported tickers. "
                f"HTTP status={response.status}"
            )
        return response.read()


def extract_supported_tickers_csv(zip_bytes: bytes) -> bytes:
    """Extract the first CSV file from Tiingo supported_tickers.zip."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csv_names = [
            name
            for name in zf.namelist()
            if name.lower().endswith(".csv")
        ]

        if not csv_names:
            raise FileNotFoundError(
                "No CSV file found inside supported_tickers.zip"
            )

        # Tiingo currently publishes one CSV in the zip.
        csv_name = csv_names[0]
        return zf.read(csv_name)


def write_local_csv(csv_bytes: bytes, output_path: Path) -> None:
    """Write extracted supported tickers CSV locally."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(csv_bytes)


def upload_to_gcs(
    local_path: Path,
    bucket_name: str,
    destination_blob_name: str,
    dry_run: bool = False,
) -> None:
    """Upload local file to GCS."""
    if not bucket_name:
        raise ValueError(
            "GCS_BUCKET is not set. Add it to .env or environment variables."
        )

    if dry_run:
        print(
            "[DRY RUN] Would upload "
            f"{local_path} to gs://{bucket_name}/{destination_blob_name}"
        )
        return

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(str(local_path))
    print(f"Uploaded to gs://{bucket_name}/{destination_blob_name}")


def summarize_csv(csv_path: Path, sample_rows: int = 5) -> None:
    """Print row count, columns, and sample rows."""
    df = pd.read_csv(csv_path)

    print("\nSupported tickers ingestion summary")
    print("-----------------------------------")
    print(f"Local path: {csv_path}")
    print(f"Rows: {len(df):,}")
    print(f"Columns: {list(df.columns)}")

    print("\nSample rows:")
    print(df.head(sample_rows).to_string(index=False))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download Tiingo supported tickers to local ODS and GCS."
    )
    parser.add_argument(
        "--no-gcs",
        action="store_true",
        help="Skip GCS upload.",
    )
    parser.add_argument(
        "--dry-run-gcs",
        action="store_true",
        help="Print planned GCS upload without uploading.",
    )
    args = parser.parse_args()

    load_dotenv(ENV_PATH)

    config = load_config()
    local_ods_root = get_local_ods_root(config)

    local_output_path = local_ods_root / "supported_tickers.csv"
    gcs_destination = (
        "ods/source=tiingo/dataset=supported_tickers/supported_tickers.csv"
    )

    print(f"Downloading Tiingo supported tickers from: {SUPPORTED_TICKERS_URL}")
    zip_bytes = download_zip_bytes()
    csv_bytes = extract_supported_tickers_csv(zip_bytes)

    write_local_csv(csv_bytes, local_output_path)
    print(f"Saved local CSV: {local_output_path}")

    summarize_csv(local_output_path)

    if not args.no_gcs:
        bucket_name = os.getenv("GCS_BUCKET", "")
        upload_to_gcs(
            local_path=local_output_path,
            bucket_name=bucket_name,
            destination_blob_name=gcs_destination,
            dry_run=args.dry_run_gcs,
        )

    loaded_at = datetime.now(timezone.utc).isoformat()
    print(f"\nCompleted supported tickers ingestion at {loaded_at}")


if __name__ == "__main__":
    main()