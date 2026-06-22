from __future__ import annotations

from pathlib import Path

import duckdb


def run_price_quality_checks(
    parquet_glob: str,
    expected_symbols: list[str],
) -> dict:
    parquet_root = Path("data/dwd/equity_price_daily")

    if not list(parquet_root.rglob("*.parquet")):
        raise FileNotFoundError("No DWD Parquet files found.")

    con = duckdb.connect()

    duplicate_count = con.execute(
        f"""
        SELECT COUNT(*) AS duplicate_count
        FROM (
            SELECT
                security_id,
                date,
                COUNT(*) AS n
            FROM read_parquet('{parquet_glob}')
            GROUP BY security_id, date
            HAVING COUNT(*) > 1
        );
        """
    ).fetchone()[0]

    if duplicate_count > 0:
        raise ValueError(f"Found duplicate security_id/date rows: {duplicate_count}")

    invalid_count = con.execute(
        f"""
        SELECT COUNT(*) AS invalid_count
        FROM read_parquet('{parquet_glob}')
        WHERE
            close <= 0
            OR adj_close <= 0
            OR high < low
            OR volume < 0
            OR split_factor <= 0;
        """
    ).fetchone()[0]

    if invalid_count > 0:
        raise ValueError(f"Found invalid price rows: {invalid_count}")

    symbol_rows = con.execute(
        f"""
        SELECT
            ticker,
            COUNT(*) AS n_rows,
            MIN(date) AS min_date,
            MAX(date) AS max_date
        FROM read_parquet('{parquet_glob}')
        GROUP BY ticker
        ORDER BY ticker;
        """
    ).fetchdf()

    actual_symbols = set(symbol_rows["ticker"].tolist())
    missing_symbols = sorted(set(expected_symbols) - actual_symbols)

    if missing_symbols:
        raise ValueError(f"Missing expected symbols in DWD: {missing_symbols}")

    summary = con.execute(
        f"""
        SELECT
            COUNT(*) AS row_count,
            COUNT(DISTINCT ticker) AS symbols_count,
            MIN(date) AS min_date,
            MAX(date) AS max_date
        FROM read_parquet('{parquet_glob}');
        """
    ).fetchone()

    return {
        "passed": True,
        "row_count": int(summary[0]),
        "symbols_count": int(summary[1]),
        "min_date": str(summary[2]),
        "max_date": str(summary[3]),
    }