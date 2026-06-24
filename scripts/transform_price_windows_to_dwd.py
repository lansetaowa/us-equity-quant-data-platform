from __future__ import annotations

import argparse
from pathlib import Path

from quant_platform.paths.data_lake import (
    DWD_PRICE_ROOT,
    DWD_PRICE_UPDATE_ARCHIVE_ROOT,
    DWD_PRICE_UPDATE_STAGING_ROOT,
    PRICE_UPDATE_TRANSFORM_REPORT_ROOT,
)
from quant_platform.prices.window_transform import (
    prepare_windowed_dwd_update,
    preview_windowed_dwd_update,
    promote_windowed_dwd_update,
)


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
        required=True,
        help=(
            "Day 3 price download CSV report."
        ),
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


def main() -> None:
    args = parse_args()

    if args.dry_run and args.promote:
        raise ValueError(
            "--dry-run and --promote cannot "
            "be used together"
        )

    if args.dry_run:
        summary = preview_windowed_dwd_update(
            args.download_report
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
            args.download_report,
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
        args.download_report,
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