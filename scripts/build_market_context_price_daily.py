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
from quant_platform.research.operation import (
    build_partition_manifest,
    default_operation_id,
    default_run_id,
    write_build_reports,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"

DEFAULT_CONFIG_PATH = Path("configs/market_context.yml")
DEFAULT_REPORT_ROOT = Path("reports/market_context_price_build")


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
        "--run-id",
        type=str,
        default=None,
        help="Component run ID for this market context price build.",
    )
    parser.add_argument(
        "--operation-id",
        type=str,
        default=None,
        help="Shared operation ID for the broader daily data operation.",
    )
    parser.add_argument(
        "--report-root",
        type=Path,
        default=DEFAULT_REPORT_ROOT,
        help="Root directory for market context price build reports.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Plan tasks but do not call Tiingo or write files.",
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
        "--replace-existing",
        action="store_true",
        help=(
            "Deprecated alias for --full-refresh. Use --full-refresh for "
            "intentional full rebuilds."
        ),
    )
    return parser.parse_args()


def require_env(name: str) -> str:
    value = os.getenv(name)

    if not value:
        raise RuntimeError(f"{name} is missing from .env")

    return value


def _month_filtered_frame(
    frame: pd.DataFrame,
    *,
    months: set | None,
) -> pd.DataFrame:
    if months is None:
        return frame.reset_index(drop=True)

    if frame.empty or not months:
        return frame.iloc[0:0].copy()

    parsed = pd.to_datetime(frame["date"], errors="coerce")

    if parsed.isna().any():
        raise ValueError("Market context price frame contains invalid dates")

    month_start = parsed.dt.to_period("M").dt.to_timestamp().dt.date

    return frame[month_start.isin(months)].reset_index(drop=True)


def _empty_download_results() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "context_set",
            "context_group",
            "ticker",
            "security_id",
            "request_start_date",
            "request_end_date",
            "status",
            "row_count",
            "local_path",
            "error_message",
        ]
    )


def main() -> None:
    args = parse_args()

    load_dotenv(dotenv_path=ENV_PATH.resolve())

    config = load_market_context_config(args.config)
    run_id = args.run_id or default_run_id("market_context_price")
    operation_id = args.operation_id or default_operation_id()

    symbols_path = args.symbols_path or config.symbol_output_path

    if not symbols_path.exists():
        raise FileNotFoundError(
            f"Market context symbol dimension not found: {symbols_path}. "
            "Run `python -m scripts.build_market_context_symbols` first."
        )

    symbols = pd.read_parquet(symbols_path)

    if args.end_date:
        request_end_date = pd.Timestamp(args.end_date).date()
    else:
        request_end_date = resolve_latest_complete_eod_date(
            EodResolutionConfig(
                manual_latest_complete_eod_date=None,
                market_calendar="XNYS",
                market_timezone="America/New_York",
                market_close_buffer_minutes=90,
            )
        )

    full_refresh = bool(args.full_refresh or args.replace_existing)

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
    print("run_id:", run_id)
    print("operation_id:", operation_id)
    print("context_set:", config.context_set)
    print("price_start_date:", config.price_start_date)
    print("request_end_date:", request_end_date)
    print("full_refresh:", full_refresh)
    print("symbols:", len(symbols))
    print("existing rows:", len(existing_prices))
    print("tasks:", len(tasks))
    print("ODS root:", config.price_ods_root)
    print("DWD root:", config.price_dwd_root)
    print("report root:", args.report_root)

    if not existing_prices.empty:
        existing_dates = pd.to_datetime(existing_prices["date"]).dt.date
        print("existing min date:", existing_dates.min())
        print("existing max date:", existing_dates.max())
        print("existing tickers:", existing_prices["ticker"].nunique())

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
        print("\n[DRY RUN] No Tiingo calls, data writes, or reports.")
        return

    if not tasks:
        empty_manifest = build_partition_manifest(
            frame=pd.DataFrame(columns=["date"]),
            date_column="date",
            output_paths=[],
        )

        report_dir = write_build_reports(
            report_root=args.report_root,
            run_id=run_id,
            summary={
                "run_id": run_id,
                "operation_id": operation_id,
                "status": "no_op",
                "context_set": config.context_set,
                "request_end_date": request_end_date,
                "full_refresh": full_refresh,
                "symbol_count": len(symbols),
                "task_count": 0,
                "existing_row_count": len(existing_prices),
                "downloaded_row_count": 0,
                "combined_row_count": len(existing_prices),
                "affected_months": [],
                "written_partition_count": 0,
                "written_partitions": [],
            },
            partition_manifest=empty_manifest,
        )

        _empty_download_results().to_csv(
            report_dir / "download_results.csv",
            index=False,
        )

        print("\nNo-op report dir:", report_dir)
        return

    api_token = require_env("TIINGO_API_TOKEN")

    loaded_at = datetime.now(UTC)
    load_id = f"{run_id}_{loaded_at:%Y%m%dT%H%M%SZ}"

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
        report_dir = write_build_reports(
            report_root=args.report_root,
            run_id=run_id,
            summary={
                "run_id": run_id,
                "operation_id": operation_id,
                "status": "failed",
                "context_set": config.context_set,
                "request_end_date": request_end_date,
                "full_refresh": full_refresh,
                "symbol_count": len(symbols),
                "task_count": len(tasks),
                "existing_row_count": len(existing_prices),
                "downloaded_row_count": len(new_prices),
                "failed_count": len(failed),
            },
            partition_manifest=build_partition_manifest(
                frame=pd.DataFrame(columns=["date"]),
                date_column="date",
                output_paths=[],
            ),
        )

        results.to_csv(
            report_dir / "download_results.csv",
            index=False,
        )

        raise SystemExit("Market context price download failed")

    combined = combine_market_context_prices(
        existing_prices=existing_prices,
        new_prices=new_prices,
        symbols=symbols,
        context_set=config.context_set,
    )

    affected_months = None if full_refresh else price_months_in_frame(new_prices)

    if affected_months is None:
        affected_months_text = ["full_refresh"]
    else:
        affected_months_text = sorted(
            month.isoformat() for month in affected_months
        )

    written = write_market_context_price_partitions(
        prices=combined,
        dwd_root=config.price_dwd_root,
        context_set=config.context_set,
        replace_existing=full_refresh,
        months_to_write=affected_months,
    )

    written_frame = _month_filtered_frame(
        combined,
        months=affected_months,
    )

    partition_manifest = build_partition_manifest(
        frame=written_frame,
        date_column="date",
        output_paths=written,
    )

    status = (
        "no_new_rows"
        if len(new_prices) == 0 and not full_refresh
        else "success"
    )

    report_dir = write_build_reports(
        report_root=args.report_root,
        run_id=run_id,
        summary={
            "run_id": run_id,
            "operation_id": operation_id,
            "status": status,
            "context_set": config.context_set,
            "source": config.source,
            "dataset_name": config.dataset_name,
            "request_end_date": request_end_date,
            "full_refresh": full_refresh,
            "symbol_count": len(symbols),
            "task_count": len(tasks),
            "existing_row_count": len(existing_prices),
            "downloaded_row_count": len(new_prices),
            "combined_row_count": len(combined),
            "download_result_counts": results["status"].value_counts().to_dict(),
            "affected_months": affected_months_text,
            "written_partition_count": len(written),
            "written_partitions": [path.as_posix() for path in written],
            "load_id": load_id,
            "loaded_at": loaded_at,
        },
        partition_manifest=partition_manifest,
    )

    results.to_csv(
        report_dir / "download_results.csv",
        index=False,
    )

    print("\nCombined rows:", len(combined))
    print("New downloaded rows:", len(new_prices))
    print("Affected months:", affected_months_text)
    print("Written partitions:", len(written))

    for path in written[-20:]:
        print(path)

    print("\nReport dir:", report_dir)


if __name__ == "__main__":
    main()