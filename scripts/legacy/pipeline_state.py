from __future__ import annotations

from datetime import date, datetime, timedelta
import os

from dotenv import load_dotenv
import psycopg


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def get_last_successful_data_end_date(
    pipeline_name: str,
) -> date | None:
    load_dotenv()

    dsn = os.environ.get("POSTGRES_DSN")
    if not dsn:
        raise RuntimeError("POSTGRES_DSN is missing. Check your .env file.")

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT data_end_date
                FROM metadata.pipeline_runs
                WHERE
                    pipeline_name = %s
                    AND status = 'success'
                    AND data_end_date IS NOT NULL
                ORDER BY data_end_date DESC
                LIMIT 1;
                """,
                (pipeline_name,),
            )

            row = cur.fetchone()

    if row is None:
        return None

    return row[0]


def compute_refresh_window(
    mode: str,
    backfill_start_date: str,
    default_lookback_days: int,
    last_successful_data_end_date: date | None,
    today: date | None = None,
) -> tuple[date, date]:
    if today is None:
        today = date.today()

    if mode not in {"backfill", "incremental"}:
        raise ValueError(f"Unsupported pipeline mode: {mode}")

    if default_lookback_days < 0:
        raise ValueError("default_lookback_days must be non-negative")

    if mode == "backfill":
        return parse_date(backfill_start_date), today

    if last_successful_data_end_date is None:
        return parse_date(backfill_start_date), today

    start_date = last_successful_data_end_date - timedelta(
        days=default_lookback_days
    )

    return start_date, today