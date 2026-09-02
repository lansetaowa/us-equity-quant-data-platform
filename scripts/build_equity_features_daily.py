from __future__ import annotations

import argparse
from pathlib import Path

from quant_platform.research.config import load_research_panel_config
from quant_platform.research.features import (
    build_equity_core_features,
    filter_to_security_ids,
    load_ever_member_security_ids,
    read_equity_prices_for_feature_build,
    write_equity_feature_partitions,
)
from quant_platform.research.operation import (
    build_partition_manifest,
    default_operation_id,
    default_run_id,
    resolve_output_month_range,
    write_build_reports,
)

DEFAULT_CONFIG_PATH = Path("configs/research_panel.yml")
DEFAULT_REPORT_ROOT = Path("reports/equity_feature_build")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build daily equity core feature table."
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
            "derived from partition_manifest.csv."
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

    run_id = args.run_id or default_run_id("equity_features")
    operation_id = args.operation_id or default_operation_id()

    output_start_month, output_end_month, source_manifest = (
        resolve_output_month_range(
            start_month=args.start_month,
            end_month=args.end_month,
            report_dir=args.price_transform_report_dir,
            report_arg_name="--price-transform-report-dir",
        )
    )

    prices = read_equity_prices_for_feature_build(
        price_root=config.price_dwd_root,
        start_month=output_start_month,
        end_month=output_end_month,
        rolling_windows=config.rolling_windows,
    )

    print("Equity feature build")
    print("--------------------")
    print("run_id:", run_id)
    print("operation_id:", operation_id)
    print("source_price_run_id:", args.source_price_run_id)
    print("price_transform_report_dir:", args.price_transform_report_dir)
    print("factor_set:", config.factor_set)
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

        print("feature universe:", universe_name)
        print("ever-member security_ids:", len(security_ids))
        print("price rows after universe filter:", len(prices))

    features = build_equity_core_features(
        prices,
        config=config,
    )

    print("\nFeature rows:", len(features))
    print("tickers:", features["ticker"].nunique())
    print("security_ids:", features["security_id"].nunique())
    print("min date:", features["date"].min())
    print("max date:", features["date"].max())
    print(
        "duplicate date/security_id/factor_set:",
        int(features.duplicated(["date", "security_id", "factor_set"]).sum()),
    )

    if args.dry_run:
        print("\n[DRY RUN] No feature partitions written.")
        return

    written, selected = write_equity_feature_partitions(
        features,
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
            "factor_set": config.factor_set,
            "output_start_month": output_start_month,
            "output_end_month": output_end_month,
            "price_rows_read_after_filter": len(prices),
            "feature_rows_total": len(features),
            "feature_rows_written": len(selected),
            "written_partition_count": len(written),
            "written_partitions": [path.as_posix() for path in written],
            "source_transform_manifest_rows": (
                len(source_manifest) if source_manifest is not None else None
            ),
        },
        partition_manifest=partition_manifest,
    )

    print("\nWritten partitions:", len(written))
    for path in written[-20:]:
        print(path)

    print("\nReport dir:", report_dir)


if __name__ == "__main__":
    main()