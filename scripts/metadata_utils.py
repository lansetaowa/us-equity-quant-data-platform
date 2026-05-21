from __future__ import annotations

from datetime import datetime, timezone
import os

from dotenv import load_dotenv
import psycopg


def log_pipeline_run(
    run_id: str,
    pipeline_name: str,
    status: str,
    row_count: int | None = None,
    notes: str | None = None,
) -> None:
    load_dotenv()

    dsn = os.environ.get("POSTGRES_DSN")
    if not dsn:
        raise RuntimeError("POSTGRES_DSN is missing. Check your .env file.")

    ended_at = datetime.now(timezone.utc)

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO metadata.pipeline_runs (
                    run_id,
                    pipeline_name,
                    status,
                    ended_at,
                    row_count,
                    notes
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id)
                DO UPDATE SET
                    status = EXCLUDED.status,
                    ended_at = EXCLUDED.ended_at,
                    row_count = EXCLUDED.row_count,
                    notes = EXCLUDED.notes;
                """,
                (
                    run_id,
                    pipeline_name,
                    status,
                    ended_at,
                    row_count,
                    notes,
                ),
            )
        conn.commit()