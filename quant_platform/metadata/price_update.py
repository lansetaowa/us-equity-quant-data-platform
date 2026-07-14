from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg
from psycopg.types.json import Jsonb


REPORT_REQUIRED_COLUMNS = {
    "source",
    "dataset_name",
    "ticker",
    "security_id",
    "request_start_date",
    "request_end_date",
    "status",
    "row_count",
    "first_price_date",
    "last_price_date",
    "api_called",
    "uploaded_to_gcs",
    "local_path",
    "gcs_uri",
    "error_message",
    "completed_at_utc",
}

ALLOWED_ACTIONS = {
    "downloaded",
    "existing",
    "empty",
    "existing_empty",
    "failed",
    "skipped",
}

ACTION_TO_STATUS = {
    "downloaded": "success",
    "existing": "success",
    "empty": "empty",
    "existing_empty": "empty",
    "failed": "failed",
    "skipped": "skipped",
}


@dataclass(frozen=True)
class PriceUpdateRunSummary:
    run_id: str
    pipeline_status: str
    source: str
    dataset_name: str
    data_start_date: date
    data_end_date: date
    symbols_count: int
    ods_records: int
    dwd_records: int
    started_at: datetime
    ended_at: datetime
    audit_report_path: str
    metrics: dict[str, Any]
    notes: str


def derive_run_id(report_path: str | Path) -> str:
    run_id = Path(report_path).stem.strip()

    if not run_id:
        raise ValueError("Could not derive run_id from report path")

    return run_id


def _nullable_text(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None

    text = str(value).strip()

    return text or None


def _nullable_date(value: Any) -> date | None:
    if value is None or pd.isna(value):
        return None

    return pd.Timestamp(value).date()


def _nullable_int(value: Any) -> int | None:
    if value is None or pd.isna(value):
        return None

    return int(value)


def _parse_boolean_series(series: pd.Series, column_name: str) -> pd.Series:
    normalized = series.astype("string").str.strip().str.lower()

    true_values = {"true", "1", "yes", "y"}
    false_values = {"false", "0", "no", "n"}

    valid = normalized.isin(true_values) | normalized.isin(false_values)
    invalid = normalized.notna() & ~valid

    if invalid.any():
        examples = normalized[invalid].drop_duplicates().head(10).tolist()
        raise ValueError(
            f"Invalid boolean values in {column_name}: {examples}"
        )

    return normalized.isin(true_values)


def load_price_update_report(path: str | Path) -> pd.DataFrame:
    """Load and validate the completed Day 3 download report."""
    report_path = Path(path)

    if not report_path.exists():
        raise FileNotFoundError(
            f"Price update report not found: {report_path}"
        )

    report = pd.read_csv(report_path)

    missing = sorted(REPORT_REQUIRED_COLUMNS - set(report.columns))
    if missing:
        raise ValueError(f"Price update report missing columns: {missing}")

    output = report.copy()

    for column in [
        "source",
        "dataset_name",
        "ticker",
        "security_id",
        "status",
    ]:
        output[column] = output[column].astype(str).str.strip()

    output["ticker"] = output["ticker"].str.upper()
    output["status"] = output["status"].str.lower()

    unexpected = sorted(set(output["status"]) - ALLOWED_ACTIONS)
    if unexpected:
        raise ValueError(f"Unexpected download actions: {unexpected}")

    for column in ["request_start_date", "request_end_date"]:
        parsed = pd.to_datetime(output[column], errors="coerce")
        if parsed.isna().any():
            raise ValueError(f"Invalid {column} values")
        output[column] = parsed.dt.date

    invalid_windows = output["request_start_date"] > output["request_end_date"]
    if invalid_windows.any():
        raise ValueError("Report contains invalid request windows")

    for column in ["first_price_date", "last_price_date"]:
        parsed = pd.to_datetime(output[column], errors="coerce")
        output[column] = parsed.dt.date

    output["row_count"] = pd.to_numeric(
        output["row_count"],
        errors="coerce",
    ).astype("Int64")

    non_failed = output["status"] != "failed"
    if output.loc[non_failed, "row_count"].isna().any():
        raise ValueError("Non-failed report rows must have row_count")

    output["api_called"] = _parse_boolean_series(
        output["api_called"],
        "api_called",
    )
    output["uploaded_to_gcs"] = _parse_boolean_series(
        output["uploaded_to_gcs"],
        "uploaded_to_gcs",
    )

    completed_at = pd.to_datetime(
        output["completed_at_utc"],
        format="mixed",
        errors="coerce",
        utc=True,
    )
    if completed_at.isna().any():
        raise ValueError("Invalid completed_at_utc values")

    output["completed_at_utc"] = completed_at

    duplicate_windows = output.duplicated(
        [
            "source",
            "dataset_name",
            "ticker",
            "request_start_date",
            "request_end_date",
        ],
        keep=False,
    )
    if duplicate_windows.any():
        raise ValueError("Report contains duplicate request windows")

    empty_actions = output["status"].isin({"empty", "existing_empty"})
    if output.loc[empty_actions, "row_count"].fillna(-1).ne(0).any():
        raise ValueError("Empty actions must have row_count=0")

    success_actions = output["status"].isin({"downloaded", "existing"})
    if output.loc[success_actions, "row_count"].fillna(0).le(0).any():
        raise ValueError("Downloaded/existing actions must contain rows")

    if output.loc[success_actions, "last_price_date"].isna().any():
        raise ValueError("Successful rows require last_price_date")

    output["persistent_status"] = output["status"].map(ACTION_TO_STATUS)

    output["gcs_uri"] = output["gcs_uri"].map(_nullable_text)
    output["error_message"] = output["error_message"].map(_nullable_text)

    return output.sort_values(["ticker", "security_id"]).reset_index(drop=True)


def load_end_to_end_artifact_summary(
    transform_report_dir: str | Path,
) -> dict[str, Any]:
    """Validate Day 4/5 completion artifacts and summarize them."""
    root = Path(transform_report_dir)

    required = {
        "prepare": root / "prepare_summary.json",
        "promotion": root / "promotion_complete.json",
        "gcs": root / "gcs_post_sync_validation.csv",
        "bigquery": root / "bigquery_apply_summary.json",
    }

    missing = [path for path in required.values() if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Missing end-to-end artifacts: {missing}")

    with required["prepare"].open("r", encoding="utf-8") as file:
        prepare = json.load(file)

    with required["promotion"].open("r", encoding="utf-8") as file:
        promotion = json.load(file)

    gcs = pd.read_csv(required["gcs"])

    if gcs.empty or not (gcs["status"] == "in_sync").all():
        raise ValueError("GCS post-sync validation did not pass")

    with required["bigquery"].open("r", encoding="utf-8") as file:
        bigquery = json.load(file)

    target_validation = bigquery.get("target_validation", {})

    for field in [
        "duplicate_groups",
        "missing_keys",
        "extra_keys",
        "mismatched_rows",
    ]:
        if int(target_validation.get(field, -1)) != 0:
            raise ValueError(
                "BigQuery target validation failed: "
                f"{field}={target_validation.get(field)}"
            )

    return {
        "normalized_new_row_count": int(
            prepare["normalized_new_row_count"]
        ),
        "affected_partition_count": int(
            prepare["affected_partition_count"]
        ),
        "promotion_completed": True,
        "gcs_partition_count": len(gcs),
        "gcs_status": "in_sync",
        "bigquery_applied_transaction": bool(
            bigquery.get("applied_transaction", False)
        ),
        "bigquery_target_validation": target_validation,
        "bigquery_global_after": bigquery.get("global_after", {}),
        "promotion_summary": promotion,
    }


def build_price_update_run_summary(
    report: pd.DataFrame,
    *,
    report_path: str | Path,
    artifact_summary: dict[str, Any],
    audit_report_path: str | Path,
) -> PriceUpdateRunSummary:
    sources = sorted(report["source"].unique())
    datasets = sorted(report["dataset_name"].unique())

    if len(sources) != 1:
        raise ValueError(f"Expected one source, got {sources}")
    if len(datasets) != 1:
        raise ValueError(f"Expected one dataset, got {datasets}")

    action_counts = {
        str(key): int(value)
        for key, value in report["status"].value_counts().to_dict().items()
    }
    persistent_counts = {
        str(key): int(value)
        for key, value in report["persistent_status"]
        .value_counts()
        .to_dict()
        .items()
    }

    failed_count = action_counts.get("failed", 0)
    pipeline_status = "failed" if failed_count else "success"

    ods_records = int(report["row_count"].fillna(0).sum())
    dwd_records = int(artifact_summary["normalized_new_row_count"])

    if pipeline_status == "success" and ods_records != dwd_records:
        raise ValueError(
            "ODS/DWD new-row reconciliation failed: "
            f"ods={ods_records}, dwd={dwd_records}"
        )

    metrics = {
        "action_counts": action_counts,
        "persistent_status_counts": persistent_counts,
        "api_call_count": int(report["api_called"].sum()),
        "gcs_upload_count": int(report["uploaded_to_gcs"].sum()),
        "empty_window_count": int(
            report["persistent_status"].eq("empty").sum()
        ),
        "artifact_summary": artifact_summary,
        "source_report_path": Path(report_path).as_posix(),
    }

    return PriceUpdateRunSummary(
        run_id=derive_run_id(report_path),
        pipeline_status=pipeline_status,
        source=sources[0],
        dataset_name=datasets[0],
        data_start_date=min(report["request_start_date"]),
        data_end_date=max(report["request_end_date"]),
        symbols_count=len(report),
        ods_records=ods_records,
        dwd_records=dwd_records,
        started_at=report["completed_at_utc"].min().to_pydatetime(),
        ended_at=report["completed_at_utc"].max().to_pydatetime(),
        audit_report_path=Path(audit_report_path).as_posix(),
        metrics=metrics,
        notes=(
            "Windowed daily price update. The Day 3 CSV was used as a "
            "one-time bridge for this already-completed run; operational "
            "truth is persisted in Postgres."
        ),
    )


def _window_result_params(
    report: pd.DataFrame,
    run_id: str,
) -> list[tuple[Any, ...]]:
    params: list[tuple[Any, ...]] = []

    for row in report.itertuples(index=False):
        status = ACTION_TO_STATUS[row.status]

        params.append(
            (
                run_id,
                row.source,
                row.dataset_name,
                row.ticker,
                row.security_id,
                row.request_start_date,
                row.request_end_date,
                status,
                row.status,
                _nullable_int(row.row_count),
                _nullable_date(row.first_price_date),
                _nullable_date(row.last_price_date),
                bool(row.api_called),
                bool(row.uploaded_to_gcs),
                _nullable_text(row.local_path),
                _nullable_text(row.gcs_uri),
                _nullable_text(row.error_message),
                row.completed_at_utc.to_pydatetime(),
            )
        )

    return params


def upsert_price_update_window_results(
    conn: psycopg.Connection,
    report: pd.DataFrame,
    run_id: str,
) -> None:
    sql = """
    INSERT INTO metadata.price_update_window_results AS current (
        run_id,
        source,
        dataset_name,
        ticker,
        security_id,
        requested_start_date,
        requested_end_date,
        status,
        action,
        row_count,
        first_price_date,
        last_price_date,
        api_called,
        uploaded_to_gcs,
        local_path,
        gcs_uri,
        error_message,
        completed_at
    )
    VALUES (
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s
    )
    ON CONFLICT (
        run_id,
        source,
        dataset_name,
        ticker,
        requested_start_date,
        requested_end_date
    )
    DO UPDATE SET
        security_id = EXCLUDED.security_id,
        status = EXCLUDED.status,
        action = EXCLUDED.action,
        row_count = EXCLUDED.row_count,
        first_price_date = EXCLUDED.first_price_date,
        last_price_date = EXCLUDED.last_price_date,
        api_called = EXCLUDED.api_called,
        uploaded_to_gcs = EXCLUDED.uploaded_to_gcs,
        local_path = EXCLUDED.local_path,
        gcs_uri = EXCLUDED.gcs_uri,
        error_message = EXCLUDED.error_message,
        completed_at = EXCLUDED.completed_at,
        updated_at = now();
    """

    with conn.cursor() as cur:
        cur.executemany(sql, _window_result_params(report, run_id))


def _symbol_status_params(
    report: pd.DataFrame,
    run_id: str,
) -> list[tuple[Any, ...]]:
    params: list[tuple[Any, ...]] = []

    for row in report.itertuples(index=False):
        persistent_status = ACTION_TO_STATUS[row.status]

        last_successful_date = (
            _nullable_date(row.last_price_date)
            if persistent_status == "success"
            else None
        )
        error_message = (
            _nullable_text(row.error_message)
            if persistent_status == "failed"
            else None
        )

        params.append(
            (
                row.source,
                row.dataset_name,
                row.ticker,
                row.security_id,
                row.request_start_date,
                row.request_end_date,
                last_successful_date,
                persistent_status,
                int(bool(row.api_called)),
                error_message,
                run_id,
                row.status,
                _nullable_int(row.row_count),
                _nullable_date(row.first_price_date),
                _nullable_date(row.last_price_date),
                bool(row.api_called),
                bool(row.uploaded_to_gcs),
                _nullable_text(row.local_path),
                _nullable_text(row.gcs_uri),
                row.completed_at_utc.to_pydatetime(),
            )
        )

    return params


def upsert_symbol_window_statuses(
    conn: psycopg.Connection,
    report: pd.DataFrame,
    run_id: str,
) -> None:
    sql = """
    INSERT INTO metadata.symbol_ingestion_status AS current (
        source,
        dataset_name,
        ticker,
        security_id,
        requested_start_date,
        requested_end_date,
        last_successful_date,
        status,
        attempt_count,
        last_error_message,
        last_run_id,
        last_action,
        row_count,
        first_price_date,
        last_price_date,
        api_called,
        uploaded_to_gcs,
        local_path,
        gcs_uri,
        last_completed_at
    )
    VALUES (
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s
    )
    ON CONFLICT (
        source,
        dataset_name,
        ticker,
        requested_start_date,
        requested_end_date
    )
    DO UPDATE SET
        security_id = EXCLUDED.security_id,
        last_successful_date = EXCLUDED.last_successful_date,
        status = EXCLUDED.status,
        attempt_count =
            current.attempt_count
            + CASE
                WHEN EXCLUDED.api_called IS TRUE
                 AND current.last_run_id IS DISTINCT FROM EXCLUDED.last_run_id
                THEN 1
                ELSE 0
              END,
        last_error_message = EXCLUDED.last_error_message,
        last_run_id = EXCLUDED.last_run_id,
        last_action = EXCLUDED.last_action,
        row_count = EXCLUDED.row_count,
        first_price_date = EXCLUDED.first_price_date,
        last_price_date = EXCLUDED.last_price_date,
        api_called = EXCLUDED.api_called,
        uploaded_to_gcs = EXCLUDED.uploaded_to_gcs,
        local_path = EXCLUDED.local_path,
        gcs_uri = EXCLUDED.gcs_uri,
        last_completed_at = EXCLUDED.last_completed_at,
        updated_at = now()
    WHERE
        current.last_completed_at IS NULL
        OR EXCLUDED.last_completed_at >= current.last_completed_at;
    """

    with conn.cursor() as cur:
        cur.executemany(sql, _symbol_status_params(report, run_id))


def upsert_pipeline_run(
    conn: psycopg.Connection,
    summary: PriceUpdateRunSummary,
    *,
    audit_report_uri: str | None,
) -> None:
    error_message = (
        None
        if summary.pipeline_status == "success"
        else "One or more window update tasks failed."
    )

    sql = """
    INSERT INTO metadata.pipeline_runs (
        run_id,
        pipeline_name,
        status,
        started_at,
        ended_at,
        row_count,
        notes,
        source,
        dataset,
        mode,
        data_start_date,
        data_end_date,
        symbols_count,
        ods_records,
        dwd_records,
        error_message,
        audit_report_path,
        audit_report_uri,
        metrics
    )
    VALUES (
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s
    )
    ON CONFLICT (run_id)
    DO UPDATE SET
        pipeline_name = EXCLUDED.pipeline_name,
        status = EXCLUDED.status,
        started_at = EXCLUDED.started_at,
        ended_at = EXCLUDED.ended_at,
        row_count = EXCLUDED.row_count,
        notes = EXCLUDED.notes,
        source = EXCLUDED.source,
        dataset = EXCLUDED.dataset,
        mode = EXCLUDED.mode,
        data_start_date = EXCLUDED.data_start_date,
        data_end_date = EXCLUDED.data_end_date,
        symbols_count = EXCLUDED.symbols_count,
        ods_records = EXCLUDED.ods_records,
        dwd_records = EXCLUDED.dwd_records,
        error_message = EXCLUDED.error_message,
        audit_report_path = EXCLUDED.audit_report_path,
        audit_report_uri = EXCLUDED.audit_report_uri,
        metrics = EXCLUDED.metrics;
    """

    params = (
        summary.run_id,
        "daily_price_update",
        summary.pipeline_status,
        summary.started_at,
        summary.ended_at,
        summary.ods_records,
        summary.notes,
        summary.source,
        summary.dataset_name,
        "windowed_incremental",
        summary.data_start_date,
        summary.data_end_date,
        summary.symbols_count,
        summary.ods_records,
        summary.dwd_records,
        error_message,
        summary.audit_report_path,
        audit_report_uri,
        Jsonb(summary.metrics),
    )

    with conn.cursor() as cur:
        cur.execute(sql, params)


def reconcile_price_update_metadata(
    conn: psycopg.Connection,
    report: pd.DataFrame,
    summary: PriceUpdateRunSummary,
    *,
    audit_report_uri: str | None,
) -> None:
    """Persist the completed run atomically."""
    with conn.transaction():
        upsert_price_update_window_results(conn, report, summary.run_id)
        upsert_symbol_window_statuses(conn, report, summary.run_id)
        upsert_pipeline_run(conn, summary, audit_report_uri=audit_report_uri)


def fetch_window_results_summary(
    conn: psycopg.Connection,
    run_id: str,
) -> pd.DataFrame:
    sql = """
    SELECT
        status,
        action,
        COUNT(*) AS n,
        COUNT(*) FILTER (WHERE api_called IS TRUE) AS api_calls,
        COUNT(*) FILTER (WHERE uploaded_to_gcs IS TRUE) AS gcs_uploads,
        SUM(COALESCE(row_count, 0)) AS row_count
    FROM metadata.price_update_window_results
    WHERE run_id = %s
    GROUP BY status, action
    ORDER BY status, action;
    """

    with conn.cursor() as cur:
        cur.execute(sql, (run_id,))
        rows = cur.fetchall()

    return pd.DataFrame(
        rows,
        columns=[
            "status",
            "action",
            "n",
            "api_calls",
            "gcs_uploads",
            "row_count",
        ],
    )


def fetch_current_window_status_summary(
    conn: psycopg.Connection,
    run_id: str,
) -> pd.DataFrame:
    sql = """
    SELECT
        status,
        last_action AS action,
        COUNT(*) AS n,
        COUNT(*) FILTER (WHERE api_called IS TRUE) AS api_calls,
        COUNT(*) FILTER (WHERE uploaded_to_gcs IS TRUE) AS gcs_uploads,
        SUM(COALESCE(row_count, 0)) AS row_count
    FROM metadata.symbol_ingestion_status
    WHERE last_run_id = %s
    GROUP BY status, last_action
    ORDER BY status, last_action;
    """

    with conn.cursor() as cur:
        cur.execute(sql, (run_id,))
        rows = cur.fetchall()

    return pd.DataFrame(
        rows,
        columns=[
            "status",
            "action",
            "n",
            "api_calls",
            "gcs_uploads",
            "row_count",
        ],
    )


def fetch_pipeline_run(conn: psycopg.Connection, run_id: str) -> dict[str, Any]:
    sql = """
    SELECT
        run_id,
        pipeline_name,
        status,
        source,
        dataset,
        mode,
        data_start_date,
        data_end_date,
        symbols_count,
        ods_records,
        dwd_records,
        audit_report_path,
        audit_report_uri,
        metrics
    FROM metadata.pipeline_runs
    WHERE run_id = %s;
    """

    with conn.cursor() as cur:
        cur.execute(sql, (run_id,))
        row = cur.fetchone()

        if row is None:
            raise ValueError(f"Pipeline run not found: {run_id}")

        columns = [description.name for description in cur.description]

    return dict(zip(columns, row, strict=True))