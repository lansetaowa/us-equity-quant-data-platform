from pathlib import Path
import os

import psycopg
from dotenv import load_dotenv


def main() -> None:
    load_dotenv()

    dsn = os.environ.get("POSTGRES_DSN")
    if not dsn:
        raise RuntimeError("POSTGRES_DSN is missing. Check your .env file.")

    sql = Path("sql/001_create_metadata_tables.sql").read_text()

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)

            cur.execute(
                """
                INSERT INTO metadata.datasets (
                    dataset_name,
                    layer,
                    storage_path,
                    description
                )
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (dataset_name)
                DO UPDATE SET
                    storage_path = EXCLUDED.storage_path,
                    description = EXCLUDED.description,
                    updated_at = now();
                """,
                (
                    "equity_price_daily_sample",
                    "dwd",
                    "data/dwd/equity_price_daily/",
                    "Synthetic daily equity price data for platform bootstrap.",
                ),
            )

        conn.commit()

    print("Metadata tables initialized.")


if __name__ == "__main__":
    main()