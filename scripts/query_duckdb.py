from pathlib import Path

import duckdb


PARQUET_GLOB = "data/dwd/equity_price_daily/**/*.parquet"


def main() -> None:
    parquet_root = Path("data/dwd/equity_price_daily")

    if not list(parquet_root.rglob("*.parquet")):
        raise FileNotFoundError(
            "No Parquet files found. Run: python scripts/create_sample_prices.py"
        )

    con = duckdb.connect()

    query = f"""
        SELECT
            ticker,
            COUNT(*) AS n_rows,
            ROUND(AVG(close), 2) AS avg_close,
            MIN(date) AS min_date,
            MAX(date) AS max_date
        FROM read_parquet('{PARQUET_GLOB}')
        GROUP BY ticker
        ORDER BY ticker;
    """

    result = con.execute(query).fetchdf()
    print(result.to_string(index=False))


if __name__ == "__main__":
    main()