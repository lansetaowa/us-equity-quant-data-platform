from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from quant_platform.universe.liquidity import (
    build_monthly_liquidity_metrics,
    filter_liquidity_metrics_by_month,
    load_liquidity_build_config,
    read_dwd_price_frame,
    summarize_liquidity_metrics,
    write_liquidity_metrics,
)

DEFAULT_CONFIG_PATH = Path("configs/liquid_universe.yml")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build monthly equity liquidity metrics from DWD prices."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
    )
    parser.add_argument(
        "--dwd-root",
        type=Path,
        default=None,
        help="Override source DWD price root.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=None,
        help="Override liquidity metrics output root.",
    )
    parser.add_argument(
        "--include-incomplete-months",
        action="store_true",
        help="Include incomplete months such as the current partial month.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace existing output root.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build and summarize metrics without writing files.",
    )
    parser.add_argument(
        "--start-month",
        type=str,
        default=None,
        help="Inclusive metric month start, e.g. 2026-07.",
    )
    parser.add_argument(
        "--end-month",
        type=str,
        default=None,
        help="Inclusive metric month end, e.g. 2026-07.",
    )
    parser.add_argument(
        "--missing-only",
        action="store_true",
        help="Write only metric-month partitions that do not already exist.",
    )
    parser.add_argument(
        "--replace-existing-partitions",
        action="store_true",
        help=(
            "Replace only selected metric-month partitions without deleting "
            "the full output root."
        ),
    )
    return parser.parse_args()


def print_month_summary(metrics: pd.DataFrame) -> None:
    month_summary = (
        metrics.groupby("metric_month")
        .agg(
            rows=("ticker", "size"),
            tickers=("ticker", "nunique"),
            security_ids=("security_id", "nunique"),
            passing=("passes_liquidity_filters", "sum"),
            complete=("is_complete_month", "first"),
            median_dollar_volume_median=("median_dollar_volume", "median"),
        )
        .reset_index()
    )

    print("\nMonthly summary tail:")
    print(month_summary.tail(18).to_string(index=False))


def print_latest_complete_month_top(metrics: pd.DataFrame) -> None:
    complete = metrics[metrics["is_complete_month"]].copy()

    if complete.empty:
        print("\nNo complete months found.")
        return

    latest_month = max(complete["metric_month"])
    latest = complete[
        (complete["metric_month"] == latest_month)
        & complete["passes_liquidity_filters"]
    ].copy()

    print(f"\nLatest complete metric month: {latest_month}")
    print("Passing rows:", len(latest))

    print("\nTop 40 by liquidity_score:")
    print(
        latest.sort_values("liquidity_score", ascending=False)
        .head(40)[
            [
                "ticker",
                "security_id",
                "trading_day_count",
                "expected_trading_days",
                "trading_day_coverage",
                "median_close",
                "median_dollar_volume",
                "liquidity_score",
            ]
        ]
        .to_string(index=False)
    )


def main() -> None:
    args = parse_args()

    config = load_liquidity_build_config(args.config)

    dwd_root = args.dwd_root or config.source_price_dwd_root
    output_root = (
        args.output_root
        or config.liquidity_monthly_output_root
    )

    include_incomplete = (
        config.include_incomplete_months
        or args.include_incomplete_months
    )

    print("Monthly equity liquidity build")
    print("------------------------------")
    print("config:", args.config)
    print("DWD root:", dwd_root)
    print("output root:", output_root)
    print("calendar:", config.calendar)
    print("include incomplete months:", include_incomplete)

    price_frame = read_dwd_price_frame(dwd_root)

    metrics = build_monthly_liquidity_metrics(
        price_frame,
        calendar_name=config.calendar,
        filters=config.filters,
        include_incomplete_months=include_incomplete,
    )

    metrics = filter_liquidity_metrics_by_month(
        metrics,
        start_month=args.start_month,
        end_month=args.end_month,
    )

    if metrics.empty:
        raise ValueError(
            "No liquidity metrics remain after month filtering"
        )

    summary = summarize_liquidity_metrics(metrics)

    print("\nSummary:")
    for key, value in summary.items():
        print(f"{key}: {value}")

    print_month_summary(metrics)
    print_latest_complete_month_top(metrics)

    if args.dry_run:
        print("\n[DRY RUN] No liquidity metric files were written.")
        return

    written = write_liquidity_metrics(
        metrics,
        output_root,
        overwrite=args.overwrite,
        missing_only=args.missing_only,
        replace_existing_partitions=args.replace_existing_partitions,
    )

    if written:
        print("\nWrote liquidity metric partitions:")
        for path in written[-20:]:
            print(path)
    else:
        print("\nNo liquidity metric partitions were written.")

    print(f"\nPartition count: {len(written)}")


if __name__ == "__main__":
    main()