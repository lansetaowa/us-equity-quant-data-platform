from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv
from google.cloud import bigquery

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CLOUD_CONFIG_PATH = PROJECT_ROOT / "configs" / "cloud.yml"
ENV_PATH = PROJECT_ROOT / ".env"


def load_cloud_config(config_path: Path = CLOUD_CONFIG_PATH) -> dict[str, Any]:
    """Load cloud configuration and expand environment variables."""
    load_dotenv(ENV_PATH)

    if not config_path.exists():
        raise FileNotFoundError(f"Cloud config not found: {config_path}")

    raw_text = config_path.read_text(encoding="utf-8")
    expanded_text = os.path.expandvars(raw_text)
    config = yaml.safe_load(expanded_text)

    if not isinstance(config, dict):
        raise ValueError("cloud.yml must contain a YAML mapping.")

    return config


def require_value(value: str | None, name: str) -> str:
    """Validate required config values."""
    if value is None or value == "" or value.startswith("${"):
        raise ValueError(
            f"Missing required config value: {name}. "
            f"Set it in .env or environment variables."
        )
    return value


def create_dataset(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    location: str,
    description: str,
    dry_run: bool = False,
) -> None:
    """Create a BigQuery dataset if it does not already exist."""
    full_dataset_id = f"{project_id}.{dataset_id}"

    if dry_run:
        print(f"[DRY RUN] Would create dataset: {full_dataset_id} ({location})")
        return

    dataset = bigquery.Dataset(full_dataset_id)
    dataset.location = location
    dataset.description = description

    client.create_dataset(dataset, exists_ok=True)
    print(f"Dataset ready: {full_dataset_id}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create BigQuery datasets for the quant data platform."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned BigQuery dataset creation without calling GCP.",
    )
    args = parser.parse_args()

    config = load_cloud_config()

    project_id = require_value(config["gcp"].get("project_id"), "GCP_PROJECT_ID")
    location = require_value(config["gcp"].get("location"), "GCP_LOCATION")
    dwh_dataset = require_value(
        config["bigquery"].get("dwh_dataset"),
        "BIGQUERY_DWH_DATASET",
    )
    metadata_dataset = require_value(
        config["bigquery"].get("metadata_dataset"),
        "BIGQUERY_METADATA_DATASET",
    )

    client = bigquery.Client(project=project_id)

    create_dataset(
        client=client,
        project_id=project_id,
        dataset_id=dwh_dataset,
        location=location,
        description="Analytical warehouse tables for the US equity quant platform.",
        dry_run=args.dry_run,
    )

    create_dataset(
        client=client,
        project_id=project_id,
        dataset_id=metadata_dataset,
        location=location,
        description="Pipeline, ingestion, data quality, and model metadata.",
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()