from __future__ import annotations

import argparse
import os
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import bigquery, storage

from quant_platform.paths.data_lake import (
    DATA_ROOT,
    DWD_PRICE_ROOT,
)
from quant_platform.prices.cloud_publish import (
    build_dwd_partition_publish_items,
    build_gcs_sync_plan,
    load_partition_manifest,
)
from quant_platform.storage.local_json import (
    write_json,
)
from quant_platform.warehouse.price_incremental import (
    apply_staging_to_target,
    build_staging_table_id,
    build_table_id,
    classify_target_state,
    drop_staging_table,
    get_affected_table_summary,
    get_table_summary,
    load_staging_table,
    month_ranges_from_manifest,
    summarize_manifest,
    validate_staging_table,
    validate_target_after_update,
    validate_target_table_definition,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"
DEFAULT_TABLE_NAME = "dwd_equity_price_daily"


def require_env(name: str) -> str:
    value = os.getenv(name)

    if not value:
        raise RuntimeError(
            f"{name} is missing from .env"
        )

    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Stage and atomically replace affected "
            "BigQuery DWD price dates."
        )
    )

    parser.add_argument(
        "--mode",
        choices=["plan", "stage", "apply"],
        default="plan",
    )
    parser.add_argument(
        "--transform-report-dir",
        type=Path,
        required=True,
    )
    parser.add_argument(
        "--dwd-root",
        type=Path,
        default=DWD_PRICE_ROOT,
    )
    parser.add_argument(
        "--table-name",
        default=DEFAULT_TABLE_NAME,
    )
    parser.add_argument(
        "--keep-staging",
        action="store_true",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    load_dotenv(
        dotenv_path=ENV_PATH.resolve()
    )

    project_id = require_env("GCP_PROJECT_ID")
    dataset_id = require_env(
        "BIGQUERY_DWH_DATASET"
    )
    bucket_name = require_env("GCS_BUCKET")
    location = os.getenv(
        "GCP_LOCATION",
        "US",
    )

    manifest = load_partition_manifest(
        args.transform_report_dir
    )
    counts = summarize_manifest(manifest)
    ranges = month_ranges_from_manifest(
        manifest
    )

    items = build_dwd_partition_publish_items(
        manifest,
        dwd_root=args.dwd_root,
        local_data_root=PROJECT_ROOT / DATA_ROOT,
        project_root=PROJECT_ROOT,
    )

    storage_client = storage.Client(
        project=project_id
    )
    bucket = storage_client.bucket(
        bucket_name
    )

    gcs_plan = build_gcs_sync_plan(
        bucket,
        items,
    )

    if not (
        gcs_plan["status"] == "in_sync"
    ).all():
        print(gcs_plan.to_string(index=False))
        raise RuntimeError(
            "Canonical GCS partitions do not "
            "match local DWD"
        )

    gcs_uris = sorted(
        f"gs://{bucket_name}/{object_name}"
        for item in items
        for object_name in item.object_names
    )

    run_id = args.transform_report_dir.name

    target_table_id = build_table_id(
        project_id,
        dataset_id,
        args.table_name,
    )
    staging_table_id = (
        build_staging_table_id(
            project_id,
            dataset_id,
            args.table_name,
            run_id,
        )
    )

    client = bigquery.Client(
        project=project_id,
        location=location,
    )

    target = client.get_table(
        target_table_id
    )
    validate_target_table_definition(target)

    affected_before = (
        get_affected_table_summary(
            client,
            target_table_id,
            ranges,
            location=location,
        )
    )

    target_state = classify_target_state(
        affected_before["n_rows"],
        expected_existing_rows=counts[
            "expected_existing_rows"
        ],
        expected_final_rows=counts[
            "expected_final_rows"
        ],
    )

    print("BigQuery incremental DWD plan")
    print("-----------------------------")
    print("mode:", args.mode)
    print("target:", target_table_id)
    print("staging:", staging_table_id)
    print("location:", location)
    print("GCS files:", len(gcs_uris))
    print("target state:", target_state)
    print("affected before:", affected_before)
    print("manifest counts:", counts)

    print("\nGCS sources:")
    for uri in gcs_uris:
        print(" ", uri)

    if affected_before["duplicate_groups"] != 0:
        raise RuntimeError(
            "Existing affected target range has "
            "duplicate keys"
        )

    if target_state == "unexpected":
        raise RuntimeError(
            "Affected BigQuery target row count "
            "matches neither the pre-update nor "
            "post-update local DWD state"
        )

    if args.mode == "plan":
        print(
            "\nRead-only plan complete. "
            "No BigQuery tables were changed."
        )
        return

    if args.mode == "stage":
        staging = load_staging_table(
            client,
            target_table_id=target_table_id,
            staging_table_id=staging_table_id,
            gcs_uris=gcs_uris,
            location=location,
        )

        staging_summary = (
            validate_staging_table(
                client,
                staging_table_id=(
                    staging_table_id
                ),
                target_table_id=(
                    target_table_id
                ),
                ranges=ranges,
                expected_rows=counts[
                    "expected_final_rows"
                ],
                location=location,
            )
        )

        report = {
            "run_id": run_id,
            "target_table_id": target_table_id,
            "staging_table_id": (
                staging_table_id
            ),
            "staging_metadata_rows": int(
                staging.num_rows
            ),
            "staging_validation": (
                staging_summary
            ),
        }

        output_path = (
            args.transform_report_dir
            / "bigquery_stage_summary.json"
        )
        write_json(output_path, report)

        print("\nStaging validation passed:")
        print(staging_summary)
        print("Report:", output_path)
        return

    staging_summary = validate_staging_table(
        client,
        staging_table_id=staging_table_id,
        target_table_id=target_table_id,
        ranges=ranges,
        expected_rows=counts[
            "expected_final_rows"
        ],
        location=location,
    )

    global_before = get_table_summary(
        client,
        target_table_id,
        location=location,
    )

    applied_transaction = False

    if target_state == "pre_update":
        apply_staging_to_target(
            client,
            target_table_id=target_table_id,
            staging_table_id=staging_table_id,
            ranges=ranges,
            location=location,
        )
        applied_transaction = True

    target_validation = (
        validate_target_after_update(
            client,
            target_table_id=target_table_id,
            staging_table_id=staging_table_id,
            ranges=ranges,
            expected_rows=counts[
                "expected_final_rows"
            ],
            location=location,
        )
    )

    global_after = get_table_summary(
        client,
        target_table_id,
        location=location,
    )

    if applied_transaction:
        actual_delta = (
            global_after["n_rows"]
            - global_before["n_rows"]
        )

        expected_delta = counts[
            "expected_inserted_rows"
        ]

        if actual_delta != expected_delta:
            raise RuntimeError(
                "BigQuery global row-count delta "
                f"mismatch: expected={expected_delta}, "
                f"actual={actual_delta}"
            )

    report = {
        "run_id": run_id,
        "target_table_id": target_table_id,
        "staging_table_id": staging_table_id,
        "target_state_before": target_state,
        "applied_transaction": (
            applied_transaction
        ),
        "manifest_counts": counts,
        "staging_validation": (
            staging_summary
        ),
        "target_validation": (
            target_validation
        ),
        "global_before": global_before,
        "global_after": global_after,
    }

    output_path = (
        args.transform_report_dir
        / "bigquery_apply_summary.json"
    )
    write_json(output_path, report)

    if not args.keep_staging:
        drop_staging_table(
            client,
            staging_table_id,
        )

    print("\nBigQuery update passed")
    print("----------------------")
    print("Target validation:", target_validation)
    print("Global before:", global_before)
    print("Global after:", global_after)
    print("Report:", output_path)

    if args.keep_staging:
        print("Staging retained:", staging_table_id)
    else:
        print("Staging table dropped.")


if __name__ == "__main__":
    main()