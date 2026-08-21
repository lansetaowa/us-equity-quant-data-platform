from __future__ import annotations

import argparse
import os
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from quant_platform.calendar.eod import (
    EodResolutionConfig,
    resolve_latest_complete_eod_date,
)
from quant_platform.clients.tiingo import TiingoClientConfig
from quant_platform.research.config import load_market_context_config
from quant_platform.research.market_context_prices import (
    build_market_context_price_tasks,
    combine_market_context_prices,
    download_market_context_price_tasks,
    price_months_in_frame,
    read_existing_market_context_prices,
    write_market_context_price_partitions,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"
DEFAULT_CONFIG_PATH = Path("configs/market_context.yml")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build DWD daily market-context ETF price data."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help=(
            "Ignore existing DWD market-context prices and rebuild from "
            "price_start_date. This replaces the full context_set output root."
        ),
    )
    parser.add_argument(
        "--symbols-path",
        type=Path,
        default=None,
        help="Override dim_market_context_symbol parquet path.",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="Override request end date, YYYY-MM-DD.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Plan tasks but do not call Tiingo or write files.",
    )
    parser.add_argument(
        "--replace-existing",
        action="store_true",
        help="Replace the full context_set DWD output root before writing.",
    )
    return parser.parse_args()


def require_env(name: str) -> str:
    value = os.getenv(name)

    if not value:
        raise RuntimeError(f"{name} is missing from .env")

    return value


def main() -> None:
    args = parse_args()

    load_dotenv(dotenv_path=ENV_PATH.resolve())

    config = load_market_context_config(args.config)
    symbols_path = args.symbols_path or config.symbol_output_path

    if not symbols_path.exists():
        raise FileNotFoundError(
            f"Market context symbol dimension not found: {symbols_path}. "
            "Run `python -m scripts.build_market_context_symbols` first."
        )

    symbols = pd.read_parquet(symbols_path)

    if args.end_date:
        request_end_date = pd.Timestamp(
            args.end_date
        ).date()
    else:
        request_end_date = resolve_latest_complete_eod_date(
            EodResolutionConfig(
                manual_latest_complete_eod_date=None,
                market_calendar="XNYS",
                market_timezone="America/New_York",
                market_close_buffer_minutes=90,
            )
        )

    full_refresh = args.full_refresh or args.replace_existing

    if full_refresh:
        existing_prices = pd.DataFrame()
    else:
        existing_prices = read_existing_market_context_prices(
            dwd_root=config.price_dwd_root,
            context_set=config.context_set,
        )

    tasks = build_market_context_price_tasks(
        symbols=symbols,
        config=config,
        request_end_date=request_end_date,
        existing_prices=existing_prices,
    )

    print("Market context daily price build")
    print("--------------------------------")
    print("context_set:", config.context_set)
    print("price_start_date:", config.price_start_date)
    print("request_end_date:", request_end_date)
    print("symbols:", len(symbols))
    print("existing rows:", len(existing_prices))
    if not existing_prices.empty:
        existing_dates = pd.to_datetime(existing_prices["date"]).dt.date
        print("existing min date:", existing_dates.min())
        print("existing max date:", existing_dates.max())
        print("existing tickers:", existing_prices["ticker"].nunique())
    print("tasks:", len(tasks))
    print("ODS root:", config.price_ods_root)
    print("DWD root:", config.price_dwd_root)

    if tasks:
        print("\nPlanned tasks:")
        for task in tasks:
            print(
                task.ticker,
                task.request_start_date,
                "->",
                task.request_end_date,
                task.local_path,
            )
    else:
        print("\nNo missing market-context prices.")

    if args.dry_run:
        print("\n[DRY RUN] No Tiingo calls or file writes.")
        return

    if not tasks:
        return

    api_token = require_env("TIINGO_API_TOKEN")

    loaded_at = datetime.now(UTC)
    load_id = (
        f"market_context_price_{config.context_set}_"
        f"{loaded_at:%Y%m%dT%H%M%SZ}"
    )

    client_config = TiingoClientConfig(
        api_token=api_token,
        timeout_seconds=60,
        max_attempts=3,
        retry_sleep_seconds=5,
    )

    results, new_prices = download_market_context_price_tasks(
        tasks=tasks,
        client_config=client_config,
        load_id=load_id,
        loaded_at=loaded_at,
        source=config.source,
    )

    print("\nDownload results:")
    print(results.to_string(index=False))

    failed = results[results["status"] == "failed"]

    if not failed.empty:
        raise SystemExit("Market context price download failed")

    combined = combine_market_context_prices(
        existing_prices=existing_prices,
        new_prices=new_prices,
        symbols=symbols,
        context_set=config.context_set,
    )

    affected_months = None if full_refresh else price_months_in_frame(new_prices)

    written = write_market_context_price_partitions(
        prices=combined,
        dwd_root=config.price_dwd_root,
        context_set=config.context_set,
        replace_existing=full_refresh,
        months_to_write=affected_months,
    )

    print("\nCombined rows:", len(combined))

    if affected_months is None:
        print("affected months: full refresh")
    else:
        print(
            "affected months:",
            ", ".join(sorted(month.isoformat() for month in affected_months)),
        )
        
    print("Written partitions:", len(written))

    for path in written[-20:]:
        print(path)


if __name__ == "__main__":
    main()