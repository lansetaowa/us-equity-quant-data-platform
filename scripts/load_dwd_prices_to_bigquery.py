from __future__ import annotations

import argparse
import os
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud import storage


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"

DEFAULT_SOURCE_PREFIX = "dwd/equity_price_daily/"
DEFAULT_TABLE_NAME = "dwd_equity_price_daily"


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def normalize_prefix(prefix: str) -> str:
    return prefix.strip("/").replace("\\", "/") + "/"


def list_parquet_uris(bucket_name: str, source_prefix: str) -> list[str]:
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    prefix = normalize_prefix(source_prefix)
    blobs = client.list_blobs(bucket, prefix=prefix)

    uris = [
        f"gs://{bucket_name}/{blob.name}"
        for blob in blobs
        if blob.name.endswith(".parquet")
    ]

    return sorted(uris)


def build_table_id(project_id: str, dataset_id: str, table_name: str) -> str:
    return f"{project_id}.{dataset_id}.{table_name}"


def rows_to_dataframe(rows):
    """Convert BigQuery RowIterator to pandas only when available."""
    return rows.to_dataframe()


def load_parquet_to_bigquery(
    project_id: str,
    dataset_id: str,
    table_name: str,
    gcs_uris: list[str],
    location: str,
    dry_run: bool,
) -> None:
    if not gcs_uris:
        raise ValueError("No parquet files found in the requested GCS prefix.")

    if len(gcs_uris) > 10_000:
        raise ValueError(
            f"Too many source URIs for one load job: {len(gcs_uris):,}. "
            "Use a staging/chunked load plan."
        )

    table_id = build_table_id(
        project_id=project_id,
        dataset_id=dataset_id,
        table_name=table_name,
    )

    print("\nBigQuery DWD load plan")
    print("----------------------")
    print(f"Project: {project_id}")
    print(f"Dataset: {dataset_id}")
    print(f"Table:   {table_id}")
    print(f"Location: {location}")
    print(f"Source parquet files: {len(gcs_uris):,}")

    print("\nSample source files:")
    for uri in gcs_uris[:20]:
        print(f"  {uri}")

    if dry_run:
        print("\n[DRY RUN] BigQuery load job was not submitted.")
        return

    client = bigquery.Client(project=project_id, location=location)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date",
        ),
        clustering_fields=["ticker", "security_id"],
    )

    load_job = client.load_table_from_uri(
        gcs_uris,
        table_id,
        job_config=job_config,
        location=location,
    )

    print(f"\nStarted BigQuery load job: {load_job.job_id}")
    load_job.result()

    table = client.get_table(table_id)

    print("\nBigQuery load completed")
    print("-----------------------")
    print(f"Loaded rows according to table metadata: {table.num_rows:,}")
    print(f"Schema fields: {[field.name + ':' + field.field_type for field in table.schema]}")
    print(
        "Partitioning field:",
        table.time_partitioning.field if table.time_partitioning else None,
    )
    print("Clustering fields:", table.clustering_fields)

    validation_sql = f"""
    SELECT
      COUNT(*) AS n_rows,
      COUNT(DISTINCT ticker) AS n_tickers,
      COUNT(DISTINCT security_id) AS n_security_ids,
      MIN(date) AS min_date,
      MAX(date) AS max_date
    FROM `{table_id}`
    """

    validation = client.query(validation_sql, location=location).result().to_dataframe()

    print("\nBigQuery validation summary")
    print("---------------------------")
    print(validation.to_string(index=False))

    duplicate_sql = f"""
    SELECT
      security_id,
      date,
      COUNT(*) AS n
    FROM `{table_id}`
    GROUP BY security_id, date
    HAVING COUNT(*) > 1
    LIMIT 20
    """

    duplicates = client.query(duplicate_sql, location=location).result().to_dataframe()

    print("\nDuplicate key check")
    print("-------------------")
    print(f"duplicate key rows returned: {len(duplicates)}")
    if not duplicates.empty:
        print(duplicates.to_string(index=False))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load formal DWD equity price parquet files from GCS into BigQuery."
    )
    parser.add_argument(
        "--source-prefix",
        default=DEFAULT_SOURCE_PREFIX,
        help="GCS prefix containing DWD parquet files.",
    )
    parser.add_argument(
        "--table-name",
        default=DEFAULT_TABLE_NAME,
        help="BigQuery destination table name.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List planned files and destination without submitting BigQuery load job.",
    )
    args = parser.parse_args()

    load_dotenv(ENV_PATH)

    project_id = require_env("GCP_PROJECT_ID")
    dataset_id = require_env("BIGQUERY_DWH_DATASET")
    location = os.getenv("GCP_LOCATION", "US")
    bucket_name = require_env("GCS_BUCKET")

    gcs_uris = list_parquet_uris(
        bucket_name=bucket_name,
        source_prefix=args.source_prefix,
    )

    load_parquet_to_bigquery(
        project_id=project_id,
        dataset_id=dataset_id,
        table_name=args.table_name,
        gcs_uris=gcs_uris,
        location=location,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()