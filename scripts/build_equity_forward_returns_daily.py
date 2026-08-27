from __future__ import annotations

import argparse
from pathlib import Path

from quant_platform.research.config import load_research_panel_config
from quant_platform.research.features import (
    filter_to_security_ids,
    load_ever_member_security_ids,
)
from quant_platform.research.labels import (
    build_forward_return_labels,
    expand_label_output_start_month_for_price_report,
    read_equity_prices_for_label_build,
    summarize_label_coverage,
    write_label_partitions,
)
from quant_platform.research.operation import (
    build_partition_manifest,
    default_operation_id,
    default_run_id,
    resolve_output_month_range,
    write_build_reports,
)

DEFAULT_CONFIG_PATH = Path("configs/research_panel.yml")
DEFAULT_REPORT_ROOT = Path("reports/equity_label_build")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build daily equity forward-return label table."
    )
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--run-id", type=str, default=None)
    parser.add_argument("--operation-id", type=str, default=None)
    parser.add_argument("--source-price-run-id", type=str, default=None)
    parser.add_argument(
        "--price-transform-report-dir",
        type=Path,
        default=None,
        help=(
            "Price transform report directory. If supplied, output months are "
            "derived from partition_manifest.csv and expanded backward by "
            "the max label horizon."
        ),
    )
    parser.add_argument("--start-month", type=str, default=None)
    parser.add_argument("--end-month", type=str, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--replace-existing-partitions", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    config = load_research_panel_config(args.config)

    run_id = args.run_id or default_run_id("equity_labels")
    operation_id = args.operation_id or default_operation_id()

    source_start_month, source_end_month, source_manifest = (
        resolve_output_month_range(
            start_month=args.start_month,
            end_month=args.end_month,
            report_dir=args.price_transform_report_dir,
            report_arg_name="--price-transform-report-dir",
        )
    )

    if args.price_transform_report_dir is not None and source_start_month is not None:
        output_start_month = expand_label_output_start_month_for_price_report(
            source_start_month,
            horizons=config.label_horizons,
        )
    else:
        output_start_month = source_start_month

    output_end_month = source_end_month

    prices = read_equity_prices_for_label_build(
        price_root=config.price_dwd_root,
        start_month=output_start_month,
        end_month=output_end_month,
        horizons=config.label_horizons,
    )

    print("Equity forward-return label build")
    print("---------------------------------")
    print("run_id:", run_id)
    print("operation_id:", operation_id)
    print("source_price_run_id:", args.source_price_run_id)
    print("price_transform_report_dir:", args.price_transform_report_dir)
    print("label_set:", config.label_set)
    print("source_start_month:", source_start_month)
    print("source_end_month:", source_end_month)
    print("output_start_month:", output_start_month)
    print("output_end_month:", output_end_month)
    print("price rows before universe filter:", len(prices))

    feature_scope = config.feature_scope

    if bool(feature_scope.get("use_ever_members", False)):
        universe_name = str(
            feature_scope.get(
                "universe_name",
                config.default_universe_name,
            )
        )
        security_ids = load_ever_member_security_ids(
            membership_root=config.universe_membership_root,
            universe_name=universe_name,
        )

        prices = filter_to_security_ids(
            prices,
            security_ids,
        )

        print("label universe:", universe_name)
        print("ever-member security_ids:", len(security_ids))
        print("price rows after universe filter:", len(prices))

    labels = build_forward_return_labels(
        prices,
        label_set=config.label_set,
        horizons=config.label_horizons,
    )

    duplicate_count = int(
        labels.duplicated(["date", "security_id", "label_set"]).sum()
    )

    coverage_summary = summarize_label_coverage(
        labels,
        horizons=config.label_horizons,
    )

    print("\nLabel rows:", len(labels))
    print("tickers:", labels["ticker"].nunique())
    print("security_ids:", labels["security_id"].nunique())
    print("min date:", labels["date"].min())
    print("max date:", labels["date"].max())
    print("duplicate date/security_id/label_set:", duplicate_count)

    print("\nCoverage summary:")
    for key, value in coverage_summary.items():
        print(f"{key}: {value}")

    if duplicate_count != 0:
        raise SystemExit("Duplicate label keys found")

    if args.dry_run:
        print("\n[DRY RUN] No label partitions written.")
        return

    written, selected = write_label_partitions(
        labels,
        config=config,
        start_month=output_start_month,
        end_month=output_end_month,
        overwrite=args.overwrite,
        replace_existing_partitions=args.replace_existing_partitions,
    )

    partition_manifest = build_partition_manifest(
        frame=selected,
        date_column="date",
        output_paths=written,
    )

    selected_coverage_summary = summarize_label_coverage(
        selected,
        horizons=config.label_horizons,
    )

    report_dir = write_build_reports(
        report_root=DEFAULT_REPORT_ROOT,
        run_id=run_id,
        summary={
            "run_id": run_id,
            "operation_id": operation_id,
            "source_price_run_id": args.source_price_run_id,
            "price_transform_report_dir": (
                args.price_transform_report_dir.as_posix()
                if args.price_transform_report_dir is not None
                else None
            ),
            "label_set": config.label_set,
            "label_horizons": list(config.label_horizons),
            "source_start_month": source_start_month,
            "source_end_month": source_end_month,
            "output_start_month": output_start_month,
            "output_end_month": output_end_month,
            "price_rows_read_after_filter": len(prices),
            "label_rows_total": len(labels),
            "label_rows_written": len(selected),
            "written_partition_count": len(written),
            "written_partitions": [path.as_posix() for path in written],
            "duplicate_key_count": duplicate_count,
            "source_transform_manifest_rows": (
                len(source_manifest) if source_manifest is not None else None
            ),
            "coverage_summary_total": coverage_summary,
            "coverage_summary_written": selected_coverage_summary,
        },
        partition_manifest=partition_manifest,
    )

    print("\nWritten partitions:", len(written))
    for path in written[-20:]:
        print(path)

    print("\nReport dir:", report_dir)


if __name__ == "__main__":
    main()