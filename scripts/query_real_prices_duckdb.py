from pathlib import Path

import duckdb


PARQUET_GLOB = "data/dwd/equity_price_daily/**/*.parquet"


def main() -> None:
    parquet_root = Path("data/dwd/equity_price_daily")

    if not list(parquet_root.rglob("*.parquet")):
        raise FileNotFoundError(
            "No DWD Parquet files found. Run transform_tiingo_prices_to_dwd.py first."
        )

    con = duckdb.connect()

    summary_query = f"""
        SELECT
            ticker,
            COUNT(*) AS n_rows,
            MIN(date) AS min_date,
            MAX(date) AS max_date,
            ROUND(AVG(adj_close), 2) AS avg_adj_close,
            ROUND(MAX(adj_close), 2) AS max_adj_close,
            ROUND(MIN(adj_close), 2) AS min_adj_close
        FROM read_parquet('{PARQUET_GLOB}')
        GROUP BY ticker
        ORDER BY ticker;
    """

    summary = con.execute(summary_query).fetchdf()
    print("Summary by ticker:")
    print(summary.to_string(index=False))

    recent_query = f"""
        SELECT
            ticker,
            date,
            close,
            adj_close,
            volume,
            div_cash,
            split_factor
        FROM read_parquet('{PARQUET_GLOB}')
        WHERE date >= DATE '2024-01-01'
        ORDER BY date DESC, ticker
        LIMIT 20;
    """

    recent = con.execute(recent_query).fetchdf()
    print("\nRecent sample:")
    print(recent.to_string(index=False))


if __name__ == "__main__":
    main()