from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from quant_platform.universe.membership import (
    build_universe_membership,
    filter_membership_by_month,
    load_universe_membership_config,
    read_liquidity_metrics,
    summarize_universe_membership,
    write_universe_membership,
)

DEFAULT_CONFIG_PATH = Path("configs/liquid_universe.yml")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build point-in-time monthly liquid universe membership "
            "from monthly liquidity metrics."
        )
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
    )
    parser.add_argument(
        "--metrics-root",
        type=Path,
        default=None,
        help="Override monthly liquidity metrics root.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=None,
        help="Override universe membership output root.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace existing output root.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build and summarize membership without writing files.",
    )
    parser.add_argument(
        "--start-membership-month",
        type=str,
        default=None,
        help="Inclusive membership month start, e.g. 2026-08.",
    )
    parser.add_argument(
        "--end-membership-month",
        type=str,
        default=None,
        help="Inclusive membership month end, e.g. 2026-08.",
    )
    parser.add_argument(
        "--missing-only",
        action="store_true",
        help="Write only membership partitions that do not already exist.",
    )
    parser.add_argument(
        "--replace-existing-partitions",
        action="store_true",
        help=(
            "Replace only selected membership partitions without deleting "
            "the full output root."
        ),
    )
    return parser.parse_args()


def print_month_universe_summary(membership: pd.DataFrame) -> None:
    summary = (
        membership.groupby(["membership_month", "universe_name"])
        .agg(
            rows=("security_id", "size"),
            min_rank=("rank", "min"),
            max_rank=("rank", "max"),
            min_score=("liquidity_score", "min"),
            max_score=("liquidity_score", "max"),
            source_metric_month=("source_metric_month", "first"),
        )
        .reset_index()
    )

    print("\nMembership month/universe summary tail:")
    print(summary.tail(20).to_string(index=False))


def print_latest_membership_samples(membership: pd.DataFrame) -> None:
    latest_month = max(membership["membership_month"])

    latest = membership[
        membership["membership_month"] == latest_month
    ].copy()

    print(f"\nLatest membership month: {latest_month}")

    for universe_name in sorted(latest["universe_name"].unique()):
        subset = latest[latest["universe_name"] == universe_name].copy()

        print("\n" + "=" * 100)
        print(universe_name)
        print("=" * 100)
        print("rows:", len(subset))
        print("source metric month:", subset["source_metric_month"].iloc[0])

        print("\nTop 30:")
        print(
            subset.sort_values("rank")
            .head(30)[
                [
                    "rank",
                    "ticker",
                    "security_id",
                    "liquidity_score",
                    "score_observation_count",
                    "source_median_close",
                    "source_median_dollar_volume",
                ]
            ]
            .to_string(index=False)
        )


def print_known_tickers(membership: pd.DataFrame) -> None:
    latest_month = max(membership["membership_month"])
    latest = membership[
        membership["membership_month"] == latest_month
    ].copy()

    tickers = [
        "AAPL",
        "MSFT",
        "NVDA",
        "AMZN",
        "META",
        "GOOG",
        "GOOGL",
        "TSLA",
        "ATLN",
        "SLAI",
    ]

    print("\nKnown ticker latest membership:")
    for ticker in tickers:
        rows = latest[latest["ticker"] == ticker].copy()

        print("\n" + ticker)

        if rows.empty:
            print("not present")
            continue

        print(
            rows[
                [
                    "universe_name",
                    "membership_month",
                    "rank",
                    "liquidity_score",
                    "source_metric_month",
                    "source_trading_day_coverage",
                    "source_median_dollar_volume",
                ]
            ].to_string(index=False)
        )


def main() -> None:
    args = parse_args()

    config = load_universe_membership_config(args.config)

    metrics_root = (
        args.metrics_root
        or config.liquidity_monthly_output_root
    )
    output_root = (
        args.output_root
        or config.universe_membership_output_root
    )

    print("Liquid universe membership build")
    print("--------------------------------")
    print("config:", args.config)
    print("metrics root:", metrics_root)
    print("output root:", output_root)
    print("score method:", config.liquidity_score_method)
    print("score aggregation:", config.score_aggregation)
    print("lookback months:", config.lookback_months)
    print("universes:", config.universes)

    metrics = read_liquidity_metrics(metrics_root)

    membership = build_universe_membership(
        metrics,
        config=config,
    )

    membership = filter_membership_by_month(
        membership,
        start_membership_month=args.start_membership_month,
        end_membership_month=args.end_membership_month,
    )

    if membership.empty:
        raise ValueError(
            "No universe membership rows remain after month filtering"
        )

    summary = summarize_universe_membership(membership)

    print("\nSummary:")
    for key, value in summary.items():
        print(f"{key}: {value}")

    print_month_universe_summary(membership)
    print_latest_membership_samples(membership)
    print_known_tickers(membership)

    if args.dry_run:
        print("\n[DRY RUN] No universe membership files were written.")
        return

    written = write_universe_membership(
        membership,
        output_root,
        overwrite=args.overwrite,
        missing_only=args.missing_only,
        replace_existing_partitions=args.replace_existing_partitions,
    )

    if written:
        print("\nWrote universe membership partitions:")
        for path in written[-20:]:
            print(path)
    else:
        print("\nNo universe membership partitions were written.")

    print(f"\nPartition count: {len(written)}")


if __name__ == "__main__":
    main()