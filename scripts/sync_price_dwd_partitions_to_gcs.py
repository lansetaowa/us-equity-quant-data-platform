from __future__ import annotations

import argparse
import os
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import storage

from quant_platform.paths.data_lake import (
    DATA_ROOT,
    DWD_PRICE_ROOT,
)
from quant_platform.prices.cloud_publish import (
    build_dwd_partition_publish_items,
    build_gcs_sync_plan,
    load_partition_manifest,
    sync_dwd_partitions_to_gcs,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"


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
            "Exact-sync affected local DWD price "
            "partitions to canonical GCS prefixes."
        )
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
        "--apply",
        action="store_true",
        help=(
            "Upload local partition files and remove "
            "extra remote objects. Without this flag, "
            "the command is read-only."
        ),
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    load_dotenv(
        dotenv_path=ENV_PATH.resolve()
    )

    project_id = require_env("GCP_PROJECT_ID")
    bucket_name = require_env("GCS_BUCKET")

    manifest = load_partition_manifest(
        args.transform_report_dir
    )

    items = build_dwd_partition_publish_items(
        manifest,
        dwd_root=args.dwd_root,
        local_data_root=PROJECT_ROOT / DATA_ROOT,
        project_root=PROJECT_ROOT,
    )

    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)

    plan = build_gcs_sync_plan(
        bucket,
        items,
    )

    plan_path = (
        args.transform_report_dir
        / "gcs_sync_plan.csv"
    )
    plan.to_csv(plan_path, index=False)

    print("GCS DWD partition sync plan")
    print("---------------------------")
    print(plan.to_string(index=False))
    print("\nPlan report:", plan_path)

    if not args.apply:
        print(
            "\nRead-only plan complete. "
            "No GCS objects were changed."
        )
        return

    result = sync_dwd_partitions_to_gcs(
        bucket,
        items,
    )

    result_path = (
        args.transform_report_dir
        / "gcs_sync_result.csv"
    )
    result.to_csv(result_path, index=False)

    post_sync = build_gcs_sync_plan(
        bucket,
        items,
    )

    post_path = (
        args.transform_report_dir
        / "gcs_post_sync_validation.csv"
    )
    post_sync.to_csv(post_path, index=False)

    print("\nGCS sync result")
    print("----------------")
    print(result.to_string(index=False))

    print("\nPost-sync validation")
    print("--------------------")
    print(post_sync.to_string(index=False))

    if not (
        post_sync["status"] == "in_sync"
    ).all():
        raise SystemExit(
            "GCS post-sync validation failed"
        )

    print("\nGCS partition sync passed.")


if __name__ == "__main__":
    main()