from __future__ import annotations

import argparse
import os
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import bigquery

from quant_platform.warehouse.partitioned_publish import (
    PartitionedTableSpec,
    build_staging_table_id,
    build_table_id,
    discover_local_partition_files,
    drop_table,
    load_gcs_uris_to_table,
    parse_month_start,
    replace_target_months_from_staging,
    table_exists,
    validate_partitioned_table_range,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"


DATASET_SPECS = {
    "liquidity_monthly": {
        "local_root": Path("data/dws/equity_liquidity_monthly"),
        "spec": PartitionedTableSpec(
            table_name="dws_equity_liquidity_monthly",
            partition_column="metric_month",
            key_columns=("metric_month", "security_id"),
            clustering_fields=("ticker", "security_id"),
        ),
    },
    "universe_membership": {
        "local_root": Path("data/dwd/universe_membership_monthly"),
        "spec": PartitionedTableSpec(
            table_name="dim_universe_membership_monthly",
            partition_column="membership_month",
            key_columns=(
                "universe_name",
                "membership_month",
                "security_id",
            ),
            clustering_fields=(
                "universe_name",
                "ticker",
                "security_id",
            ),
        ),
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish universe outputs to BigQuery."
    )
    parser.add_argument(
        "--dataset",
        choices=sorted(DATASET_SPECS),
        required=True,
    )
    parser.add_argument(
        "--mode",
        choices=["plan", "full-replace", "replace-months"],
        default="plan",
    )
    parser.add_argument(
        "--start-month",
        type=str,
        required=True,
        help="Inclusive month start, e.g. 2026-07.",
    )
    parser.add_argument(
        "--end-month",
        type=str,
        required=True,
        help="Inclusive month end, e.g. 2026-07.",
    )
    parser.add_argument(
        "--keep-staging",
        action="store_true",
        help="Keep staging table after replace-months mode.",
    )
    return parser.parse_args()


def require_env(name: str) -> str:
    value = os.getenv(name)

    if not value:
        raise RuntimeError(f"{name} is missing from .env")

    return value


def main() -> None:
    args = parse_args()

    load_dotenv(dotenv_path=ENV_PATH.resolve())

    project_id = require_env("GCP_PROJECT_ID")
    dataset_id = require_env("BIGQUERY_DWH_DATASET")
    bucket_name = require_env("GCS_BUCKET")
    location = os.getenv("GCP_LOCATION", "US")

    dataset_config = DATASET_SPECS[args.dataset]
    local_root = dataset_config["local_root"]
    spec = dataset_config["spec"]

    start_month = parse_month_start(
        args.start_month,
        field_name="start_month",
    )
    end_month = parse_month_start(
        args.end_month,
        field_name="end_month",
    )

    if start_month is None or end_month is None:
        raise ValueError("start-month and end-month are required")

    if start_month > end_month:
        raise ValueError("start-month must be <= end-month")

    files = discover_local_partition_files(
        local_root=local_root,
        bucket_name=bucket_name,
        partition_column=spec.partition_column,
        start_month=start_month,
        end_month=end_month,
    )

    gcs_uris = [item.gcs_uri for item in files]
    expected_rows = sum(item.row_count for item in files)

    target_table_id = build_table_id(
        project_id=project_id,
        dataset_id=dataset_id,
        table_name=spec.table_name,
    )

    staging_table_id = build_staging_table_id(
        project_id=project_id,
        dataset_id=dataset_id,
        table_name=spec.table_name,
        suffix=f"{args.dataset}_{start_month:%Y%m}_{end_month:%Y%m}",
    )

    client = bigquery.Client(
        project=project_id,
        location=location,
    )

    target_exists = table_exists(
        client,
        target_table_id,
    )

    print("Universe BigQuery publish")
    print("-------------------------")
    print("dataset:", args.dataset)
    print("mode:", args.mode)
    print("target:", target_table_id)
    print("staging:", staging_table_id)
    print("location:", location)
    print("target exists:", target_exists)
    print("local root:", local_root)
    print("month range:", start_month, "->", end_month)
    print("selected files:", len(files))
    print("expected rows:", expected_rows)

    print("\nGCS sources:")
    for uri in gcs_uris:
        print(uri)

    if args.mode == "plan":
        print("\n[PLAN] No BigQuery tables were changed.")
        return

    if args.mode == "full-replace":
        load_gcs_uris_to_table(
            client=client,
            destination_table=target_table_id,
            gcs_uris=gcs_uris,
            spec=spec,
            location=location,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

    elif args.mode == "replace-months":
        if not target_exists:
            raise RuntimeError(
                f"Target table does not exist: {target_table_id}. "
                "Run --mode full-replace first."
            )

        load_gcs_uris_to_table(
            client=client,
            destination_table=staging_table_id,
            gcs_uris=gcs_uris,
            spec=spec,
            location=location,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        validate_partitioned_table_range(
            client=client,
            table_id=staging_table_id,
            spec=spec,
            start_month=start_month,
            end_month=end_month,
            expected_rows=expected_rows,
            location=location,
        )

        replace_target_months_from_staging(
            client=client,
            target_table_id=target_table_id,
            staging_table_id=staging_table_id,
            spec=spec,
            start_month=start_month,
            end_month=end_month,
            location=location,
        )

        if not args.keep_staging:
            drop_table(
                client,
                staging_table_id,
            )

    validation = validate_partitioned_table_range(
        client=client,
        table_id=target_table_id,
        spec=spec,
        start_month=start_month,
        end_month=end_month,
        expected_rows=expected_rows,
        location=location,
    )

    print("\nValidation:")
    for key, value in validation.items():
        print(f"{key}: {value}")

    print("\nUniverse BigQuery publish passed.")


if __name__ == "__main__":
    main()