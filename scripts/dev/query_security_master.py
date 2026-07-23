from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SECURITY_MASTER_PATH = (
    PROJECT_ROOT / "data" / "dwd" / "security_master" / "dim_security.parquet"
)


def query_security_master(parquet_path: Path, limit: int = 20) -> None:
    """Print basic summaries from dim_security parquet."""
    if not parquet_path.exists():
        raise FileNotFoundError(
            f"Security master parquet not found: {parquet_path}. "
            "Run `python -m scripts.build_security_master` first."
        )

    con = duckdb.connect()

    print("\nSecurity master overview")
    print("------------------------")

    total_rows = con.execute(
        """
        SELECT COUNT(*) AS n_rows
        FROM read_parquet(?)
        """,
        [str(parquet_path)],
    ).fetchdf()

    print("\nTotal rows:")
    print(total_rows.to_string(index=False))

    by_asset_type = con.execute(
        """
        SELECT asset_type, COUNT(*) AS n
        FROM read_parquet(?)
        GROUP BY asset_type
        ORDER BY n DESC
        LIMIT 20
        """,
        [str(parquet_path)],
    ).fetchdf()

    print("\nCount by asset_type:")
    print(by_asset_type.to_string(index=False))

    by_exchange = con.execute(
        """
        SELECT exchange, COUNT(*) AS n
        FROM read_parquet(?)
        GROUP BY exchange
        ORDER BY n DESC
        LIMIT 30
        """,
        [str(parquet_path)],
    ).fetchdf()

    print("\nCount by exchange:")
    print(by_exchange.to_string(index=False))

    by_currency = con.execute(
        """
        SELECT price_currency, COUNT(*) AS n
        FROM read_parquet(?)
        GROUP BY price_currency
        ORDER BY n DESC
        LIMIT 20
        """,
        [str(parquet_path)],
    ).fetchdf()

    print("\nCount by price_currency:")
    print(by_currency.to_string(index=False))

    active_counts = con.execute(
        """
        SELECT is_active, COUNT(*) AS n
        FROM read_parquet(?)
        GROUP BY is_active
        ORDER BY is_active DESC
        """,
        [str(parquet_path)],
    ).fetchdf()

    print("\nActive counts:")
    print(active_counts.to_string(index=False))

    date_coverage = con.execute(
        """
        SELECT
            MIN(start_date) AS min_start_date,
            MAX(start_date) AS max_start_date,
            MIN(end_date) AS min_end_date,
            MAX(end_date) AS max_end_date
        FROM read_parquet(?)
        """,
        [str(parquet_path)],
    ).fetchdf()

    print("\nDate coverage:")
    print(date_coverage.to_string(index=False))

    sample = con.execute(
        """
        SELECT *
        FROM read_parquet(?)
        ORDER BY ticker
        LIMIT ?
        """,
        [str(parquet_path), limit],
    ).fetchdf()

    print("\nSample rows:")
    print(sample.to_string(index=False))


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect dim_security parquet.")
    parser.add_argument(
        "--path",
        type=str,
        default=str(DEFAULT_SECURITY_MASTER_PATH),
        help="Path to dim_security parquet.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Number of sample rows to print.",
    )
    args = parser.parse_args()

    query_security_master(Path(args.path), limit=args.limit)


if __name__ == "__main__":
    main()