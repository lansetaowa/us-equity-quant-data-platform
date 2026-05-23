from __future__ import annotations

from datetime import date, datetime, timezone
import os

from dotenv import load_dotenv
import psycopg


def get_postgres_dsn() -> str:
    load_dotenv()

    dsn = os.environ.get("POSTGRES_DSN")
    if not dsn:
        raise RuntimeError("POSTGRES_DSN is missing. Check your .env file.")

    return dsn


def log_pipeline_started(
    run_id: str,
    pipeline_name: str,
    source: str,
    dataset: str,
    mode: str,
    data_start_date: date,
    data_end_date: date,
    symbols_count: int,
    notes: str | None = None,
) -> None:
    dsn = get_postgres_dsn()

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO metadata.pipeline_runs (
                    run_id,
                    pipeline_name,
                    status,
                    started_at,
                    source,
                    dataset,
                    mode,
                    data_start_date,
                    data_end_date,
                    symbols_count,
                    notes
                )
                VALUES (
                    %s, %s, 'started', now(),
                    %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (run_id)
                DO UPDATE SET
                    status = 'started',
                    started_at = now(),
                    source = EXCLUDED.source,
                    dataset = EXCLUDED.dataset,
                    mode = EXCLUDED.mode,
                    data_start_date = EXCLUDED.data_start_date,
                    data_end_date = EXCLUDED.data_end_date,
                    symbols_count = EXCLUDED.symbols_count,
                    notes = EXCLUDED.notes;
                """,
                (
                    run_id,
                    pipeline_name,
                    source,
                    dataset,
                    mode,
                    data_start_date,
                    data_end_date,
                    symbols_count,
                    notes,
                ),
            )

        conn.commit()


def log_pipeline_success(
    run_id: str,
    row_count: int | None = None,
    ods_records: int | None = None,
    dwd_records: int | None = None,
    notes: str | None = None,
) -> None:
    dsn = get_postgres_dsn()
    ended_at = datetime.now(timezone.utc)

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE metadata.pipeline_runs
                SET
                    status = 'success',
                    ended_at = %s,
                    row_count = %s,
                    ods_records = %s,
                    dwd_records = %s,
                    notes = COALESCE(%s, notes),
                    error_message = NULL
                WHERE run_id = %s;
                """,
                (
                    ended_at,
                    row_count,
                    ods_records,
                    dwd_records,
                    notes,
                    run_id,
                ),
            )

        conn.commit()


def log_pipeline_failed(
    run_id: str,
    error_message: str,
    notes: str | None = None,
) -> None:
    dsn = get_postgres_dsn()
    ended_at = datetime.now(timezone.utc)

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE metadata.pipeline_runs
                SET
                    status = 'failed',
                    ended_at = %s,
                    error_message = %s,
                    notes = COALESCE(%s, notes)
                WHERE run_id = %s;
                """,
                (
                    ended_at,
                    error_message[:2000],
                    notes,
                    run_id,
                ),
            )

        conn.commit()