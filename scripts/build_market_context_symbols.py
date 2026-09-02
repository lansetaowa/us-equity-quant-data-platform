from __future__ import annotations

import argparse
from pathlib import Path

from quant_platform.research.config import load_market_context_config
from quant_platform.research.market_context import (
    build_dim_market_context_symbol,
    load_dim_security_for_market_context,
    write_dim_market_context_symbol,
)

DEFAULT_CONFIG_PATH = Path("configs/market_context.yml")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build resolved market-context symbol dimension."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print output without writing Parquet.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    config = load_market_context_config(args.config)
    dim_security = load_dim_security_for_market_context(
        config.dim_security_path
    )

    output = build_dim_market_context_symbol(
        dim_security=dim_security,
        config=config,
    )

    print("Market context symbol dimension")
    print("-------------------------------")
    print("context_set:", config.context_set)
    print("output_path:", config.symbol_output_path)
    print("rows:", len(output))
    print("groups:")
    print(output["context_group"].value_counts().to_string())
    print("\nSymbols:")
    print(
        output[
            [
                "context_group",
                "ticker",
                "security_id",
                "asset_type",
                "exchange",
                "end_date",
                "is_active",
                "is_primary_benchmark",
            ]
        ].to_string(index=False)
    )

    if args.dry_run:
        print("\n[DRY RUN] No file written.")
        return

    written = write_dim_market_context_symbol(
        output,
        config.symbol_output_path,
    )

    print(f"\nWrote: {written}")


if __name__ == "__main__":
    main()