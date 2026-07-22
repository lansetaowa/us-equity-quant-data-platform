from __future__ import annotations

import argparse
import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv

from quant_platform.metadata.price_update import (
    export_price_update_window_results,
)
from quant_platform.paths.data_lake import (
    DWD_PRICE_ROOT,
    DWD_PRICE_UPDATE_ARCHIVE_ROOT,
    DWD_PRICE_UPDATE_STAGING_ROOT,
    PRICE_UPDATE_METADATA_EXPORT_ROOT,
    PRICE_UPDATE_TRANSFORM_REPORT_ROOT,
)
from quant_platform.prices.window_transform import (
    prepare_windowed_dwd_update,
    preview_windowed_dwd_update,
    promote_windowed_dwd_update,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Incrementally merge windowed Tiingo "
            "raw files into affected DWD partitions."
        )
    )

    parser.add_argument(
        "--download-report",
        type=Path,
        default=None,
        help="Legacy CSV bridge report from the completed Day 3 run.",
    )

    parser.add_argument(
        "--run-id",
        type=str,
        default=None,
        help="Postgres run_id to use as the transform source of truth.",
    )

    parser.add_argument(
        "--dwd-root",
        type=Path,
        default=DWD_PRICE_ROOT,
    )

    parser.add_argument(
        "--staging-base",
        type=Path,
        default=(
            DWD_PRICE_UPDATE_STAGING_ROOT
        ),
    )

    parser.add_argument(
        "--archive-base",
        type=Path,
        default=(
            DWD_PRICE_UPDATE_ARCHIVE_ROOT
        ),
    )

    parser.add_argument(
        "--report-base",
        type=Path,
        default=(
            PRICE_UPDATE_TRANSFORM_REPORT_ROOT
        ),
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Validate inputs and print affected "
            "partitions without writing."
        ),
    )

    parser.add_argument(
        "--promote",
        action="store_true",
        help=(
            "Promote an already prepared staging "
            "run into final DWD."
        ),
    )

    parser.add_argument(
        "--overwrite-staging",
        action="store_true",
        help=(
            "Replace an existing staging/report "
            "directory for this run."
        ),
    )

    return parser.parse_args()


def resolve_transform_input(args: argparse.Namespace) -> Path:
    """Return a CSV-compatible input path.

    If --run-id is supplied, the file is generated from Postgres into data/_tmp.
    The operational fact source is Postgres, not reports/.
    """
    if bool(args.download_report) == bool(args.run_id):
        raise ValueError(
            "Specify exactly one of --download-report or --run-id"
        )

    if args.download_report is not None:
        return args.download_report

    load_dotenv(dotenv_path=ENV_PATH.resolve())

    dsn = os.getenv("POSTGRES_DSN")
    if not dsn:
        raise RuntimeError("POSTGRES_DSN is missing from .env")

    export_path = (
        PRICE_UPDATE_METADATA_EXPORT_ROOT
        / f"{args.run_id}.csv"
    )

    with psycopg.connect(dsn) as conn:
        export_price_update_window_results(
            conn,
            run_id=args.run_id,
            output_path=export_path,
        )

    return export_path


def main() -> None:
    args = parse_args()

    transform_input = resolve_transform_input(args)

    if args.dry_run and args.promote:
        raise ValueError(
            "--dry-run and --promote cannot "
            "be used together"
        )

    if args.dry_run:
        summary = preview_windowed_dwd_update(
            transform_input
        )

        print("Windowed DWD transform preview")
        print("------------------------------")

        for key, value in summary.items():
            print(f"{key}: {value}")

        print(
            "\nNo staging or final DWD files "
            "were written."
        )
        return

    if args.promote:
        paths = promote_windowed_dwd_update(
            transform_input,
            dwd_root=args.dwd_root,
            staging_base=args.staging_base,
            archive_base=args.archive_base,
            report_base=args.report_base,
        )

        print("Promotion complete")
        print("Final DWD root:", args.dwd_root)
        print("Archive root:", paths.archive_root)
        print("Report directory:", paths.report_dir)
        return

    paths = prepare_windowed_dwd_update(
        transform_input,
        dwd_root=args.dwd_root,
        staging_base=args.staging_base,
        archive_base=args.archive_base,
        report_base=args.report_base,
        overwrite=args.overwrite_staging,
    )

    print("Staging preparation complete")
    print("Staging root:", paths.staging_root)
    print("Future archive root:", paths.archive_root)
    print("Report directory:", paths.report_dir)
    print(
        "\nFinal DWD has not been modified. "
        "Inspect staging before --promote."
    )


if __name__ == "__main__":
    main()