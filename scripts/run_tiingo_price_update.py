from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import storage

from quant_platform.clients.tiingo import (
    TiingoClientConfig,
)
from quant_platform.paths.data_lake import (
    ODS_ROOT,
    PRICE_GAP_TASK_LIST_PATH,
    PRICE_UPDATE_CONFIG_PATH,
    PRICE_UPDATE_DOWNLOAD_REPORT_ROOT,
)
from quant_platform.prices.download import (
    build_price_download_plan,
    load_price_download_settings,
    load_price_gap_tasks,
    parse_ticker_csv,
    print_price_download_summary,
    run_price_download_tasks,
    save_price_download_results,
    select_price_download_tasks,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download windowed Tiingo EOD price updates "
            "from a generated gap task list."
        )
    )

    parser.add_argument(
        "--task-list",
        type=Path,
        default=PRICE_GAP_TASK_LIST_PATH,
        help="Path to price gap task-list Parquet.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=PRICE_UPDATE_CONFIG_PATH,
        help="Path to price update YAML config.",
    )
    parser.add_argument(
        "--tickers",
        type=str,
        default=None,
        help=(
            "Optional comma-separated ticker subset, "
            "for example AAPL,MSFT,NVDA."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "Optional CLI-only task limit for controlled "
            "testing."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Print the download plan without API calls, "
            "writes, or uploads."
        ),
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help=(
            "Refetch and replace existing window files. "
            "Use only for explicit repair or retry."
        ),
    )
    parser.add_argument(
        "--upload-gcs",
        action="store_true",
        help=(
            "Upload valid local window files to GCS. "
            "Existing local files are uploaded without "
            "calling Tiingo again."
        ),
    )
    parser.add_argument(
        "--report-root",
        type=Path,
        default=PRICE_UPDATE_DOWNLOAD_REPORT_ROOT,
        help="Directory for download-run CSV reports.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    settings = load_price_download_settings(
        args.config
    )
    tasks = load_price_gap_tasks(args.task_list)

    selected = select_price_download_tasks(
        tasks,
        tickers=parse_ticker_csv(args.tickers),
        limit=args.limit,
    )

    plan = build_price_download_plan(
        selected,
        ods_root=ODS_ROOT,
        filename=settings.filename,
        overwrite=args.overwrite,
    )

    print("Windowed Tiingo price update")
    print("----------------------------")
    print(f"task list: {args.task_list}")
    print(f"selected tasks: {len(selected)}")
    print(f"tickers: {args.tickers or 'all'}")
    print(f"limit: {args.limit}")
    print(f"overwrite: {args.overwrite}")
    print(f"upload GCS: {args.upload_gcs}")
    print(
        "files already present:",
        int(plan["file_exists"].sum()),
    )
    print(
        "planned API calls:",
        int(plan["would_call_api"].sum()),
    )

    print("\nSelected plan:")
    print(
        plan.head(30).to_string(index=False)
    )

    if args.dry_run:
        print(
            "\n[DRY RUN] No API calls, local writes, "
            "or GCS uploads were performed."
        )
        return

    load_dotenv(
        dotenv_path=ENV_PATH.resolve()
    )

    api_token = os.getenv("TIINGO_API_TOKEN")

    if not api_token:
        raise RuntimeError(
            "TIINGO_API_TOKEN is missing from .env"
        )

    client_config = TiingoClientConfig(
        api_token=api_token,
        timeout_seconds=(
            settings.request_timeout_seconds
        ),
        max_attempts=settings.max_attempts,
        retry_sleep_seconds=(
            settings.retry_sleep_seconds
        ),
    )

    bucket = None

    if args.upload_gcs:
        project_id = os.getenv("GCP_PROJECT_ID")
        bucket_name = os.getenv("GCS_BUCKET")

        if not project_id:
            raise RuntimeError(
                "GCP_PROJECT_ID is missing from .env"
            )

        if not bucket_name:
            raise RuntimeError(
                "GCS_BUCKET is missing from .env"
            )

        client = storage.Client(
            project=project_id
        )
        bucket = client.bucket(bucket_name)

    results = run_price_download_tasks(
        selected,
        client_config=client_config,
        settings=settings,
        ods_root=ODS_ROOT,
        overwrite=args.overwrite,
        bucket=bucket,
    )

    run_id = datetime.now(
        timezone.utc
    ).strftime("%Y%m%dT%H%M%SZ")

    report_path = (
        args.report_root
        / f"price_download_{run_id}.csv"
    )

    save_price_download_results(
        results,
        report_path,
    )

    print_price_download_summary(results)
    print(f"\nReport: {report_path}")

    failed_count = int(
        (results["status"] == "failed").sum()
    )

    if failed_count:
        raise SystemExit(
            f"{failed_count} price-download task(s) failed"
        )


if __name__ == "__main__":
    main()