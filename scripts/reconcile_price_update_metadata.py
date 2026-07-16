from __future__ import annotations

import argparse
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg
from dotenv import load_dotenv

from quant_platform.metadata.price_update import (
    build_price_update_run_summary,
    fetch_current_window_status_summary,
    fetch_pipeline_run,
    fetch_window_results_summary,
    load_end_to_end_artifact_summary,
    load_price_update_report,
    reconcile_price_update_metadata,
)
from quant_platform.paths.data_lake import (
    DIM_SECURITY_PATH,
    PRICE_UPDATE_AUDIT_REPORT_ROOT,
)
from quant_platform.storage.local_json import write_json

from quant_platform.metadata.price_update import (
    export_price_update_window_results,
)
from quant_platform.paths.data_lake import (
    PRICE_UPDATE_METADATA_EXPORT_ROOT,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Reconcile a completed price update into Postgres metadata."
    )

    parser.add_argument(
        "--download-report",
        type=Path,
        default=None,
        help="Legacy CSV bridge report.",
    )

    parser.add_argument(
        "--run-id",
        type=str,
        default=None,
        help="Postgres run_id to reconcile.",
    )
    parser.add_argument("--transform-report-dir", type=Path, required=True)
    parser.add_argument("--audit-report-uri", type=str, default=None)
    parser.add_argument(
        "--audit-root",
        type=Path,
        default=PRICE_UPDATE_AUDIT_REPORT_ROOT,
    )
    parser.add_argument("--dry-run", action="store_true")

    return parser.parse_args()


def _json_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}

    if isinstance(value, list):
        return [_json_value(item) for item in value]

    return value


def _empty_window_review(report: pd.DataFrame) -> pd.DataFrame:
    empty = report[report["persistent_status"] == "empty"].copy()

    if empty.empty:
        return empty

    dim = pd.read_parquet(DIM_SECURITY_PATH)

    dim["ticker"] = dim["ticker"].astype(str).str.strip().str.upper()
    dim["security_id"] = dim["security_id"].astype(str).str.strip()

    keep_columns = [
        column
        for column in [
            "ticker",
            "security_id",
            "end_date",
            "is_active",
            "exchange",
            "asset_type",
            "currency",
        ]
        if column in dim.columns
    ]

    return empty.merge(
        dim[keep_columns].drop_duplicates(["ticker", "security_id"]),
        on=["ticker", "security_id"],
        how="left",
    )


def _status_summary(df: pd.DataFrame) -> dict[str, int]:
    return {
        str(key): int(value)
        for key, value in df["persistent_status"].value_counts().to_dict().items()
    }


def _action_summary(df: pd.DataFrame) -> dict[str, int]:
    return {
        str(key): int(value)
        for key, value in df["status"].value_counts().to_dict().items()
    }


def _compare_metadata_to_report(
    report: pd.DataFrame,
    result_summary: pd.DataFrame,
    current_summary: pd.DataFrame,
) -> None:
    expected_action = _action_summary(report)
    expected_status = _status_summary(report)
    expected_api_calls = int(report["api_called"].sum())
    expected_uploads = int(report["uploaded_to_gcs"].sum())
    expected_rows = int(report["row_count"].fillna(0).sum())

    for name, summary in [
        ("price_update_window_results", result_summary),
        ("symbol_ingestion_status", current_summary),
    ]:
        actual_action = {
            str(row.action): int(row.n)
            for row in summary.itertuples(index=False)
        }

        if actual_action != expected_action:
            raise ValueError(
                f"{name} action counts mismatch: "
                f"expected={expected_action}, actual={actual_action}"
            )

        actual_status = summary.groupby("status")["n"].sum().astype(int).to_dict()

        if actual_status != expected_status:
            raise ValueError(
                f"{name} status counts mismatch: "
                f"expected={expected_status}, actual={actual_status}"
            )

        actual_api_calls = int(summary["api_calls"].sum())
        if actual_api_calls != expected_api_calls:
            raise ValueError(f"{name} API-call count mismatch")

        actual_uploads = int(summary["gcs_uploads"].sum())
        if actual_uploads != expected_uploads:
            raise ValueError(f"{name} GCS-upload count mismatch")

        actual_rows = int(summary["row_count"].fillna(0).sum())
        if actual_rows != expected_rows:
            raise ValueError(f"{name} row count mismatch")

def resolve_result_input(args: argparse.Namespace) -> Path:
    """Return a CSV-compatible result file generated from the fact source."""
    if bool(args.download_report) == bool(args.run_id):
        raise ValueError(
            "Specify exactly one of --download-report or --run-id"
        )

    if args.download_report is not None:
        return args.download_report

    load_dotenv(dotenv_path=ENV_PATH.resolve())

    dsn = os.getenv("POSTGRES_DSN")
    if not dsn:
        raise RuntimeError("POSTGRES_DSN is missing from .env")

    export_path = (
        PRICE_UPDATE_METADATA_EXPORT_ROOT
        / f"{args.run_id}.csv"
    )

    with psycopg.connect(dsn) as conn:
        export_price_update_window_results(
            conn,
            run_id=args.run_id,
            output_path=export_path,
        )

    return export_path

def main() -> None:
    args = parse_args()

    result_input = resolve_result_input(args)
    result_input = resolve_result_input(args)

    report = load_price_update_report(result_input)
    artifacts = load_end_to_end_artifact_summary(args.transform_report_dir)

    run_id = args.run_id or Path(result_input).stem
    audit_dir = args.audit_root / run_id

    summary = build_price_update_run_summary(
        report,
        report_path=result_input,
        artifact_summary=artifacts,
        audit_report_path=audit_dir,
    )

    print("Price update metadata reconciliation")
    print("------------------------------------")
    print("run_id:", summary.run_id)
    print("pipeline status:", summary.pipeline_status)
    print("symbols:", summary.symbols_count)
    print("ODS records:", summary.ods_records)
    print("DWD records:", summary.dwd_records)
    print("start date:", summary.data_start_date)
    print("end date:", summary.data_end_date)
    print("audit dir:", audit_dir)
    print("audit URI:", args.audit_report_uri)

    print("\nAction counts:")
    print(report["status"].value_counts().to_string())

    print("\nPersistent status counts:")
    print(report["persistent_status"].value_counts().to_string())

    print("\nAPI calls:", int(report["api_called"].sum()))
    print("GCS uploads:", int(report["uploaded_to_gcs"].sum()))

    if args.dry_run:
        print("\n[DRY RUN] No Postgres rows were written.")
        return

    load_dotenv(dotenv_path=ENV_PATH.resolve())

    dsn = os.getenv("POSTGRES_DSN")
    if not dsn:
        raise RuntimeError("POSTGRES_DSN is missing from .env")

    with psycopg.connect(dsn) as conn:
        reconcile_price_update_metadata(
            conn,
            report,
            summary,
            audit_report_uri=args.audit_report_uri,
        )

        result_summary = fetch_window_results_summary(conn, summary.run_id)
        current_summary = fetch_current_window_status_summary(conn, summary.run_id)
        pipeline_run = fetch_pipeline_run(conn, summary.run_id)

    _compare_metadata_to_report(report, result_summary, current_summary)

    if int(pipeline_run["symbols_count"]) != summary.symbols_count:
        raise ValueError("pipeline_runs symbols_count mismatch")

    if int(pipeline_run["ods_records"]) != summary.ods_records:
        raise ValueError("pipeline_runs ods_records mismatch")

    if int(pipeline_run["dwd_records"]) != summary.dwd_records:
        raise ValueError("pipeline_runs dwd_records mismatch")

    audit_dir.mkdir(parents=True, exist_ok=True)

    report_status_summary = (
        report.groupby(["persistent_status", "status"], dropna=False)
        .agg(
            n=("ticker", "size"),
            api_calls=("api_called", "sum"),
            gcs_uploads=("uploaded_to_gcs", "sum"),
            row_count=("row_count", "sum"),
        )
        .reset_index()
    )

    empty_review = _empty_window_review(report)

    report_status_summary.to_csv(
        audit_dir / "report_status_summary.csv",
        index=False,
    )
    result_summary.to_csv(
        audit_dir / "window_results_status_summary.csv",
        index=False,
    )
    current_summary.to_csv(
        audit_dir / "current_status_summary.csv",
        index=False,
    )
    empty_review.to_csv(
        audit_dir / "empty_windows.csv",
        index=False,
    )

    write_json(
        audit_dir / "run_summary.json",
        {
            "run_id": summary.run_id,
            "pipeline_status": summary.pipeline_status,
            "source": summary.source,
            "dataset_name": summary.dataset_name,
            "data_start_date": summary.data_start_date.isoformat(),
            "data_end_date": summary.data_end_date.isoformat(),
            "symbols_count": summary.symbols_count,
            "ods_records": summary.ods_records,
            "dwd_records": summary.dwd_records,
            "audit_report_path": summary.audit_report_path,
            "audit_report_uri": args.audit_report_uri,
            "metrics": _json_value(summary.metrics),
        },
    )

    write_json(
        audit_dir / "pipeline_run.json",
        _json_value(pipeline_run),
    )

    print("\nPostgres reconciliation passed.")
    print("Audit directory:", audit_dir)
    print("Empty windows:", len(empty_review))


if __name__ == "__main__":
    main()