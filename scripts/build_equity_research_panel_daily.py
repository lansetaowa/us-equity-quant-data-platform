from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from quant_platform.research.config import (
    load_market_context_config,
    load_research_panel_config,
)
from quant_platform.research.operation import (
    build_partition_manifest,
    default_operation_id,
    default_run_id,
    month_range_from_partition_manifest_report,
    parse_month_start,
    write_build_reports,
)
from quant_platform.research.panel import (
    build_equity_research_panel,
    read_equity_features_for_panel_build,
    read_labels_for_panel_build,
    read_market_context_features_for_panel_build,
    read_universe_membership_for_panel_build,
    summarize_panel,
    write_panel_partitions,
)

DEFAULT_RESEARCH_CONFIG_PATH = Path("configs/research_panel.yml")
DEFAULT_MARKET_CONTEXT_CONFIG_PATH = Path("configs/market_context.yml")
DEFAULT_REPORT_ROOT = Path("reports/equity_research_panel_build")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build point-in-time daily equity research panel."
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
    parser.add_argument("--source-feature-run-id", type=str, default=None)
    parser.add_argument("--source-label-run-id", type=str, default=None)
    parser.add_argument("--source-market-context-feature-run-id", type=str, default=None)
    parser.add_argument("--feature-report-dir", type=Path, default=None)
    parser.add_argument("--label-report-dir", type=Path, default=None)
    parser.add_argument("--market-context-feature-report-dir", type=Path, default=None)
    parser.add_argument("--start-month", type=str, default=None)
    parser.add_argument("--end-month", type=str, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--replace-existing-partitions", action="store_true")
    return parser.parse_args()


def _resolve_month_range(args: argparse.Namespace):
    report_dirs = [
        path
        for path in [
            args.feature_report_dir,
            args.label_report_dir,
            args.market_context_feature_report_dir,
        ]
        if path is not None
    ]

    manual_supplied = bool(args.start_month or args.end_month)
    report_supplied = bool(report_dirs)

    if manual_supplied and report_supplied:
        raise ValueError(
            "Use either report-dir arguments or --start-month/--end-month, not both"
        )

    if bool(args.start_month) != bool(args.end_month):
        raise ValueError("--start-month and --end-month must be provided together")

    if report_dirs:
        starts = []
        ends = []
        manifests = []

        for report_dir in report_dirs:
            try:
                start, end, manifest = month_range_from_partition_manifest_report(
                    report_dir
                )
            except ValueError:
                # no-op upstream reports may have an empty manifest; skip them.
                continue

            starts.append(start)
            ends.append(end)
            manifests.append(manifest)

        if not starts:
            raise ValueError("No non-empty upstream report manifests found")

        return min(starts), max(ends), pd.concat(manifests, ignore_index=True)

    if args.start_month is None and args.end_month is None:
        return None, None, None

    return (
        parse_month_start(args.start_month, field_name="start_month"),
        parse_month_start(args.end_month, field_name="end_month"),
        None,
    )


def main() -> None:
    args = parse_args()

    research_config = load_research_panel_config(args.research_config)
    market_context_config = load_market_context_config(args.market_context_config)

    run_id = args.run_id or default_run_id("equity_research_panel")
    operation_id = args.operation_id or default_operation_id()

    universe_name = str(
        research_config.feature_scope.get(
            "universe_name",
            research_config.default_universe_name,
        )
    )

    output_start_month, output_end_month, source_manifest = _resolve_month_range(args)

    print("Equity research panel build")
    print("---------------------------")
    print("run_id:", run_id)
    print("operation_id:", operation_id)
    print("universe_name:", universe_name)
    print("factor_set:", research_config.factor_set)
    print("label_set:", research_config.label_set)
    print("context_set:", market_context_config.context_set)
    print("output_start_month:", output_start_month)
    print("output_end_month:", output_end_month)
    print("source_feature_run_id:", args.source_feature_run_id)
    print("source_label_run_id:", args.source_label_run_id)
    print(
        "source_market_context_feature_run_id:",
        args.source_market_context_feature_run_id,
    )

    features = read_equity_features_for_panel_build(
        config=research_config,
        start_month=output_start_month,
        end_month=output_end_month,
    )
    labels = read_labels_for_panel_build(
        config=research_config,
        start_month=output_start_month,
        end_month=output_end_month,
    )
    market_context_features = read_market_context_features_for_panel_build(
        config=research_config,
        context_set=market_context_config.context_set,
        start_month=output_start_month,
        end_month=output_end_month,
    )
    membership = read_universe_membership_for_panel_build(
        membership_root=research_config.universe_membership_root,
        universe_name=universe_name,
        start_month=output_start_month,
        end_month=output_end_month,
    )

    print("\nInput rows:")
    print("features:", len(features))
    print("labels:", len(labels))
    print("market_context_features:", len(market_context_features))
    print("membership:", len(membership))

    panel = build_equity_research_panel(
        features=features,
        labels=labels,
        market_context_features=market_context_features,
        membership=membership,
        universe_name=universe_name,
        factor_set=research_config.factor_set,
        label_set=research_config.label_set,
    )

    summary = summarize_panel(panel)

    print("\nPanel summary:")
    for key, value in summary.items():
        print(f"{key}: {value}")

    if summary["duplicate_key_count"] != 0:
        raise SystemExit("Duplicate panel keys found")

    if args.dry_run:
        print("\n[DRY RUN] No panel partitions written.")
        return

    written, selected = write_panel_partitions(
        panel,
        config=research_config,
        universe_name=universe_name,
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

    selected_summary = summarize_panel(selected)

    report_dir = write_build_reports(
        report_root=DEFAULT_REPORT_ROOT,
        run_id=run_id,
        summary={
            "run_id": run_id,
            "operation_id": operation_id,
            "universe_name": universe_name,
            "factor_set": research_config.factor_set,
            "label_set": research_config.label_set,
            "context_set": market_context_config.context_set,
            "output_start_month": output_start_month,
            "output_end_month": output_end_month,
            "source_feature_run_id": args.source_feature_run_id,
            "source_label_run_id": args.source_label_run_id,
            "source_market_context_feature_run_id": (
                args.source_market_context_feature_run_id
            ),
            "feature_report_dir": (
                args.feature_report_dir.as_posix()
                if args.feature_report_dir is not None
                else None
            ),
            "label_report_dir": (
                args.label_report_dir.as_posix()
                if args.label_report_dir is not None
                else None
            ),
            "market_context_feature_report_dir": (
                args.market_context_feature_report_dir.as_posix()
                if args.market_context_feature_report_dir is not None
                else None
            ),
            "source_manifest_rows": (
                len(source_manifest) if source_manifest is not None else None
            ),
            "input_rows": {
                "features": len(features),
                "labels": len(labels),
                "market_context_features": len(market_context_features),
                "membership": len(membership),
            },
            "panel_summary_total": summary,
            "panel_summary_written": selected_summary,
            "written_partition_count": len(written),
            "written_partitions": [path.as_posix() for path in written],
        },
        partition_manifest=partition_manifest,
    )

    print("\nWritten partitions:", len(written))
    for path in written[-20:]:
        print(path)

    print("\nReport dir:", report_dir)


if __name__ == "__main__":
    main()