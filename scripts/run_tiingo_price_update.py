from __future__ import annotations

import argparse
import os
from datetime import UTC, datetime
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from google.cloud import storage

from quant_platform.clients.tiingo import TiingoClientConfig
from quant_platform.metadata.price_update import (
    export_price_update_window_results,
    fetch_existing_price_update_window_results,
    fetch_pipeline_run,
    persist_price_update_result,
    split_tasks_for_run_resume,
    start_price_update_pipeline_run,
    update_price_update_pipeline_run_after_download,
)
from quant_platform.paths.data_lake import (
    ODS_ROOT,
    PRICE_GAP_TASK_LIST_PATH,
    PRICE_UPDATE_AUDIT_REPORT_ROOT,
    PRICE_UPDATE_CONFIG_PATH,
)
from quant_platform.prices.download import (
    build_price_download_plan,
    load_price_download_settings,
    load_price_gap_tasks,
    parse_ticker_csv,
    print_price_download_summary,
    run_price_download_tasks,
    select_price_download_tasks,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"


def default_run_id() -> str:
    return "price_update_" + datetime.now(UTC).strftime(
        "%Y%m%dT%H%M%SZ"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download windowed Tiingo EOD price updates and persist "
            "operational results directly to Postgres."
        )
    )

    parser.add_argument(
        "--task-list",
        type=Path,
        default=PRICE_GAP_TASK_LIST_PATH,
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=PRICE_UPDATE_CONFIG_PATH,
    )
    parser.add_argument(
        "--run-id",
        type=str,
        default=None,
        help="Optional explicit run_id. Defaults to price_update_<UTC timestamp>.",
    )
    parser.add_argument(
        "--tickers",
        type=str,
        default=None,
        help="Optional comma-separated ticker subset, for example AAPL,MSFT,NVDA.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional CLI-only task limit for controlled testing.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the plan without API calls, writes, uploads, or Postgres changes.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Refetch and replace existing window files.",
    )
    parser.add_argument(
        "--upload-gcs",
        action="store_true",
        help="Upload valid local window files to GCS.",
    )
    parser.add_argument(
        "--write-report-export",
        action="store_true",
        help=(
            "Write a CSV audit export after the run. This is not operational "
            "truth; Postgres is."
        ),
    )
    parser.add_argument(
        "--audit-root",
        type=Path,
        default=PRICE_UPDATE_AUDIT_REPORT_ROOT,
    )

    return parser.parse_args()


def export_window_results_audit(
    *,
    dsn: str,
    run_id: str,
    audit_root: Path,
) -> Path:
    """Export Postgres window results as an audit CSV."""
    audit_dir = audit_root / run_id
    audit_dir.mkdir(parents=True, exist_ok=True)
    export_path = audit_dir / "window_results_export.csv"

    with psycopg.connect(dsn) as conn:
        export_price_update_window_results(
            conn,
            run_id=run_id,
            output_path=export_path,
        )

    return export_path


def main() -> None:
    args = parse_args()

    run_id = args.run_id or default_run_id()

    settings = load_price_download_settings(args.config)
    tasks = load_price_gap_tasks(args.task_list)

    selected = select_price_download_tasks(
        tasks,
        tickers=parse_ticker_csv(args.tickers),
        limit=args.limit,
    )

    load_dotenv(dotenv_path=ENV_PATH.resolve())

    dsn = os.getenv("POSTGRES_DSN")
    if not dsn:
        raise RuntimeError("POSTGRES_DSN is missing from .env")

    with psycopg.connect(dsn) as conn:
        existing_results = fetch_existing_price_update_window_results(
            conn,
            run_id=run_id,
        )

        try:
            pipeline_run = fetch_pipeline_run(conn, run_id)
        except ValueError:
            pipeline_run = None

    pending, already_completed = split_tasks_for_run_resume(
        selected,
        existing_results,
    )

    if pending.empty:
        plan = None
    else:
        plan = build_price_download_plan(
            pending,
            ods_root=ODS_ROOT,
            filename=settings.filename,
            overwrite=args.overwrite,
        )

    data_start_date = min(selected["request_start_date"])
    data_end_date = max(selected["request_end_date"])

    source_values = sorted(selected["source"].unique())
    dataset_values = sorted(selected["dataset_name"].unique())

    if len(source_values) != 1:
        raise ValueError(f"Expected one source, got {source_values}")

    if len(dataset_values) != 1:
        raise ValueError(f"Expected one dataset, got {dataset_values}")

    print("Windowed Tiingo price update")
    print("----------------------------")
    print(f"run_id: {run_id}")
    print(f"task list: {args.task_list}")
    print(f"already completed for run_id: {len(already_completed)}")
    print(f"pending tasks: {len(pending)}")
    print(f"selected tasks: {len(selected)}")
    print(f"tickers: {args.tickers or 'all'}")
    print(f"limit: {args.limit}")
    print(f"overwrite: {args.overwrite}")
    print(f"upload GCS: {args.upload_gcs}")
    if plan is not None:
        print("files already present:", int(plan["file_exists"].sum()))
        print("planned API calls:", int(plan["would_call_api"].sum()))

        print("\nPending plan:")
        print(plan.head(30).to_string(index=False))
    else:
        print("files already present: 0")
        print("planned API calls: 0")
        print("\nNo pending tasks for this run_id.")

    if not already_completed.empty:
        print("\nAlready completed sample:")
        print(
            already_completed[
                [
                    "ticker",
                    "request_start_date",
                    "request_end_date",
                    "prior_status",
                    "prior_action",
                    "prior_row_count",
                ]
            ]
            .head(20)
            .to_string(index=False)
        )

    if args.dry_run:
        print(
            "\n[DRY RUN] No API calls, local writes, GCS uploads, "
            "or Postgres changes were performed."
        )
        return

    if pending.empty:
        print(
            "\nNo pending tasks remain for this run_id. "
            "No API calls or new window-result writes will be performed."
        )

        if pipeline_run and pipeline_run.get("status") == "running":
            with psycopg.connect(dsn) as conn:
                update_price_update_pipeline_run_after_download(
                    conn,
                    run_id=run_id,
                )
                conn.commit()

            print("Existing running pipeline row was refreshed from results.")

        if args.write_report_export:
            export_path = export_window_results_audit(
                dsn=dsn,
                run_id=run_id,
                audit_root=args.audit_root,
            )

            print("\nAudit export:", export_path)

        return

    api_token = os.getenv("TIINGO_API_TOKEN")
    if not api_token:
        raise RuntimeError("TIINGO_API_TOKEN is missing from .env")

    client_config = TiingoClientConfig(
        api_token=api_token,
        timeout_seconds=settings.request_timeout_seconds,
        max_attempts=settings.max_attempts,
        retry_sleep_seconds=settings.retry_sleep_seconds,
    )

    bucket = None

    if args.upload_gcs:
        project_id = os.getenv("GCP_PROJECT_ID")
        bucket_name = os.getenv("GCS_BUCKET")

        if not project_id:
            raise RuntimeError("GCP_PROJECT_ID is missing from .env")

        if not bucket_name:
            raise RuntimeError("GCS_BUCKET is missing from .env")

        client = storage.Client(project=project_id)
        bucket = client.bucket(bucket_name)

    with psycopg.connect(dsn) as conn:
        start_price_update_pipeline_run(
            conn,
            run_id=run_id,
            source=source_values[0],
            dataset_name=dataset_values[0],
            data_start_date=data_start_date,
            data_end_date=data_end_date,
            symbols_count=len(selected),
            notes=(
                "Windowed daily price update. Operational results are "
                "persisted directly to Postgres."
            ),
        )
        conn.commit()

        def persist_result(result: dict) -> None:
            persist_price_update_result(
                conn,
                run_id=run_id,
                result=result,
            )
            conn.commit()

        results = run_price_download_tasks(
            pending,
            client_config=client_config,
            settings=settings,
            ods_root=ODS_ROOT,
            overwrite=args.overwrite,
            bucket=bucket,
            result_callback=persist_result,
        )

        update_price_update_pipeline_run_after_download(
            conn,
            run_id=run_id,
        )
        conn.commit()

    print_price_download_summary(results)

    failed_count = int((results["status"] == "failed").sum())

    if args.write_report_export:
        export_path = export_window_results_audit(
            dsn=dsn,
            run_id=run_id,
            audit_root=args.audit_root,
        )

        print("\nAudit export:", export_path)

    if failed_count:
        raise SystemExit(f"{failed_count} price-download task(s) failed")

    print("\nPostgres-native download stage complete.")
    print(f"run_id: {run_id}")


if __name__ == "__main__":
    main()