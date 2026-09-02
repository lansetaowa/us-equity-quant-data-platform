from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"


@dataclass(frozen=True)
class ResearchOutputSpec:
    dataset_key: str
    table_name: str
    gcs_prefix: str
    partition_field: str | None
    clustering_fields: tuple[str, ...]


DATASETS: dict[str, ResearchOutputSpec] = {
    "dim_market_context_symbol": ResearchOutputSpec(
        dataset_key="dim_market_context_symbol",
        table_name="dim_market_context_symbol",
        gcs_prefix="dwd/security_master/dim_market_context_symbol/",
        partition_field=None,
        clustering_fields=("context_set", "ticker"),
    ),
    "market_context_price_daily": ResearchOutputSpec(
        dataset_key="market_context_price_daily",
        table_name="dwd_market_context_price_daily",
        gcs_prefix="dwd/market_context_price_daily/",
        partition_field="date",
        clustering_fields=("context_set", "ticker", "security_id"),
    ),
    "market_context_features_daily": ResearchOutputSpec(
        dataset_key="market_context_features_daily",
        table_name="dws_market_context_features_daily",
        gcs_prefix="dws/market_context_features_daily/",
        partition_field="date",
        clustering_fields=("context_set", "ticker", "security_id"),
    ),
    "equity_features_daily": ResearchOutputSpec(
        dataset_key="equity_features_daily",
        table_name="dws_equity_features_daily",
        gcs_prefix="dws/equity_features_daily/",
        partition_field="date",
        clustering_fields=("factor_set", "ticker", "security_id"),
    ),
    "equity_forward_returns_daily": ResearchOutputSpec(
        dataset_key="equity_forward_returns_daily",
        table_name="dws_equity_forward_returns_daily",
        gcs_prefix="dws/equity_forward_returns_daily/",
        partition_field="date",
        clustering_fields=("label_set", "ticker", "security_id"),
    ),
    "equity_research_panel_daily": ResearchOutputSpec(
        dataset_key="equity_research_panel_daily",
        table_name="ads_equity_research_panel_daily",
        gcs_prefix="ads/equity_research_panel_daily/",
        partition_field="date",
        clustering_fields=("universe_name", "factor_set", "ticker"),
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish research data products from GCS Parquet to BigQuery."
    )
    parser.add_argument(
        "--dataset",
        choices=["all", *DATASETS.keys()],
        default="all",
        help="Dataset key to publish. Defaults to all research outputs.",
    )
    parser.add_argument(
        "--mode",
        choices=["plan", "apply"],
        required=True,
        help="Use plan to print inputs, or apply to load BigQuery tables.",
    )
    parser.add_argument(
        "--recreate",
        action="store_true",
        help=(
            "Drop target tables before loading. Useful if a previous "
            "first-time publish created the table with the wrong schema, "
            "partitioning, or clustering."
        ),
    )
    return parser.parse_args()


def require_env(name: str) -> str:
    value = os.getenv(name)

    if not value:
        raise RuntimeError(f"{name} is missing from .env")

    return value


def selected_specs(dataset_key: str) -> list[ResearchOutputSpec]:
    if dataset_key == "all":
        return list(DATASETS.values())

    return [DATASETS[dataset_key]]


def build_table_id(
    *,
    project_id: str,
    dataset_id: str,
    table_name: str,
) -> str:
    return f"{project_id}.{dataset_id}.{table_name}"


def list_parquet_uris(
    *,
    storage_client: storage.Client,
    bucket_name: str,
    prefix: str,
) -> list[str]:
    bucket = storage_client.bucket(bucket_name)

    return sorted(
        f"gs://{bucket_name}/{blob.name}"
        for blob in bucket.list_blobs(prefix=prefix)
        if blob.name.endswith(".parquet")
    )


def build_load_job_config(
    spec: ResearchOutputSpec,
) -> bigquery.LoadJobConfig:
    time_partitioning = None

    if spec.partition_field is not None:
        time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=spec.partition_field,
        )

    return bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        autodetect=True,
        time_partitioning=time_partitioning,
        clustering_fields=list(spec.clustering_fields),
    )


def print_existing_table(
    *,
    client: bigquery.Client,
    destination: str,
) -> None:
    try:
        table = client.get_table(destination)
    except NotFound:
        print("existing_table: missing")
        return

    partition_field = (
        table.time_partitioning.field
        if table.time_partitioning is not None
        else None
    )

    print("existing_table: present")
    print("existing_rows:", table.num_rows)
    print("existing_schema_fields:", len(table.schema))
    print("existing_partition_field:", partition_field)
    print("existing_clustering_fields:", table.clustering_fields)


def load_dataset(
    *,
    bigquery_client: bigquery.Client,
    storage_client: storage.Client,
    project_id: str,
    dataset_id: str,
    bucket_name: str,
    location: str,
    spec: ResearchOutputSpec,
    apply: bool,
    recreate: bool,
) -> None:
    uris = list_parquet_uris(
        storage_client=storage_client,
        bucket_name=bucket_name,
        prefix=spec.gcs_prefix,
    )

    destination = build_table_id(
        project_id=project_id,
        dataset_id=dataset_id,
        table_name=spec.table_name,
    )

    print("\n" + "=" * 100)
    print("dataset:", spec.dataset_key)
    print("table:", destination)
    print("gcs_prefix:", f"gs://{bucket_name}/{spec.gcs_prefix}")
    print("parquet_uri_count:", len(uris))
    print("partition_field:", spec.partition_field)
    print("clustering_fields:", spec.clustering_fields)

    print_existing_table(
        client=bigquery_client,
        destination=destination,
    )

    if not uris:
        raise RuntimeError(f"No parquet URIs found for {spec.dataset_key}")

    print("sample_uris:")
    for uri in uris[:10]:
        print(" ", uri)

    if not apply:
        return

    if recreate:
        print("dropping_existing_table:", destination)
        bigquery_client.delete_table(
            destination,
            not_found_ok=True,
        )

    job_config = build_load_job_config(spec)

    job = bigquery_client.load_table_from_uri(
        uris,
        destination,
        job_config=job_config,
        location=location,
    )

    job.result()

    table = bigquery_client.get_table(destination)

    print("load_job_id:", job.job_id)
    print("output_rows:", table.num_rows)
    print("schema_fields:", len(table.schema))


def main() -> None:
    args = parse_args()

    load_dotenv(dotenv_path=ENV_PATH.resolve())

    project_id = require_env("GCP_PROJECT_ID")
    bucket_name = require_env("GCS_BUCKET")
    dataset_id = require_env("BIGQUERY_DWH_DATASET")
    location = os.getenv("GCP_LOCATION", "US")

    bigquery_client = bigquery.Client(
        project=project_id,
        location=location,
    )
    storage_client = storage.Client(project=project_id)

    print("Research output BigQuery publish")
    print("--------------------------------")
    print("mode:", args.mode)
    print("recreate:", args.recreate)
    print("project_id:", project_id)
    print("dataset_id:", dataset_id)
    print("bucket_name:", bucket_name)
    print("location:", location)

    for spec in selected_specs(args.dataset):
        load_dataset(
            bigquery_client=bigquery_client,
            storage_client=storage_client,
            project_id=project_id,
            dataset_id=dataset_id,
            bucket_name=bucket_name,
            location=location,
            spec=spec,
            apply=args.mode == "apply",
            recreate=args.recreate,
        )

    print("\nResearch BigQuery publish complete.")


if __name__ == "__main__":
    main()
