from __future__ import annotations

import argparse
from pathlib import Path

from quant_platform.research.config import (
    load_market_context_config,
    load_research_panel_config,
)
from quant_platform.research.features import (
    build_market_context_core_features,
    read_market_context_prices_for_feature_build,
    write_market_context_feature_partitions,
)
from quant_platform.research.operation import (
    build_partition_manifest,
    default_operation_id,
    default_run_id,
    resolve_output_month_range,
    write_build_reports,
)

DEFAULT_RESEARCH_CONFIG_PATH = Path("configs/research_panel.yml")
DEFAULT_MARKET_CONTEXT_CONFIG_PATH = Path("configs/market_context.yml")
DEFAULT_REPORT_ROOT = Path("reports/market_context_feature_build")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build daily market-context core feature table."
    )
    parser.add_argument(
        "--research-config",
        type=Path,
        default=DEFAULT_RESEARCH_CONFIG_PATH,
    )
    parser.add_argument(
        "--market-context-config",
        type=Path,
        default=DEFAULT_MARKET_CONTEXT_CONFIG_PATH,
    )
    parser.add_argument("--run-id", type=str, default=None)
    parser.add_argument("--operation-id", type=str, default=None)
    parser.add_argument("--source-market-context-price-run-id", type=str, default=None)
    parser.add_argument(
        "--market-context-price-report-dir",
        type=Path,
        default=None,
        help=(
            "Market context price build report directory. If supplied, "
            "output months are derived from partition_manifest.csv."
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

    research_config = load_research_panel_config(args.research_config)
    market_config = load_market_context_config(args.market_context_config)

    run_id = args.run_id or default_run_id("market_context_features")
    operation_id = args.operation_id or default_operation_id()

    output_start_month, output_end_month, source_manifest = (
        resolve_output_month_range(
            start_month=args.start_month,
            end_month=args.end_month,
            report_dir=args.market_context_price_report_dir,
            report_arg_name="--market-context-price-report-dir",
        )
    )

    prices = read_market_context_prices_for_feature_build(
        price_root=market_config.price_dwd_root,
        context_set=market_config.context_set,
        start_month=output_start_month,
        end_month=output_end_month,
        rolling_windows=research_config.rolling_windows,
    )

    print("Market context feature build")
    print("----------------------------")
    print("run_id:", run_id)
    print("operation_id:", operation_id)
    print(
        "source_market_context_price_run_id:",
        args.source_market_context_price_run_id,
    )
    print("market_context_price_report_dir:", args.market_context_price_report_dir)
    print("context_set:", market_config.context_set)
    print("output_start_month:", output_start_month)
    print("output_end_month:", output_end_month)
    print("price rows:", len(prices))

    features = build_market_context_core_features(
        prices,
        context_set=market_config.context_set,
        rolling_windows=research_config.rolling_windows,
    )

    print("\nFeature rows:", len(features))
    print("tickers:", features["ticker"].nunique())
    print("security_ids:", features["security_id"].nunique())
    print("min date:", features["date"].min())
    print("max date:", features["date"].max())
    print(
        "duplicate date/context/security:",
        int(features.duplicated(["date", "context_set", "security_id"]).sum()),
    )

    if args.dry_run:
        print("\n[DRY RUN] No feature partitions written.")
        return

    written, selected = write_market_context_feature_partitions(
        features,
        output_root=research_config.market_context_feature_root,
        context_set=market_config.context_set,
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
            "source_market_context_price_run_id": (
                args.source_market_context_price_run_id
            ),
            "market_context_price_report_dir": (
                args.market_context_price_report_dir.as_posix()
                if args.market_context_price_report_dir is not None
                else None
            ),
            "context_set": market_config.context_set,
            "output_start_month": output_start_month,
            "output_end_month": output_end_month,
            "price_rows_read": len(prices),
            "feature_rows_total": len(features),
            "feature_rows_written": len(selected),
            "written_partition_count": len(written),
            "written_partitions": [path.as_posix() for path in written],
            "source_manifest_rows": (
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