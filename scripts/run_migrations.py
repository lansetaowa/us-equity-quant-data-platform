from __future__ import annotations

import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv

MIGRATIONS = [
    Path("sql/001_create_metadata_tables.sql"),
    Path("sql/002_alter_pipeline_runs.sql"),
    Path("sql/003_create_symbol_ingestion_status.sql"),
]

def main() -> None:
    load_dotenv()

    dsn = os.environ.get("POSTGRES_DSN")
    if not dsn:
        raise RuntimeError("POSTGRES_DSN is missing. Check your .env file.")

    print(f"Using POSTGRES_DSN: {dsn}")

    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT current_database(), current_user, "
            "inet_server_addr(), inet_server_port();"
        )
        db_info = cur.fetchone()
        print(f"Connected to database: {db_info}")

        for migration in MIGRATIONS:
            if not migration.exists():
                raise FileNotFoundError(f"Migration file not found: {migration}")

            sql = migration.read_text(encoding="utf-8")
            print(f"Running migration: {migration}")
            print(f"SQL length: {len(sql)} characters")

            if not sql.strip():
                raise RuntimeError(f"Migration file is empty: {migration}")

            cur.execute(sql)

        conn.commit()

        cur.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'metadata'
                AND table_name = 'pipeline_runs'
            ORDER BY ordinal_position;
            """
        )
        rows = cur.fetchall()

        print("Current metadata.pipeline_runs columns:")
        for row in rows:
            print(row)              

    print("All migrations completed.")


if __name__ == "__main__":
    main()