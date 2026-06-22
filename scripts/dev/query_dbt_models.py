from __future__ import annotations

from pathlib import Path

import duckdb


DBT_DUCKDB_PATH = Path("data/dbt/quant.duckdb")

"""
python -m scripts.query_dbt_models
"""

""" 
生成dbt docs
cd dbt_quant
dbt docs generate --profiles-dir .
cd ..
"""

"""
本地查看dbt docs
cd dbt_quant
dbt docs serve --profiles-dir .
"""

def main() -> None:
    if not DBT_DUCKDB_PATH.exists():
        raise FileNotFoundError(
            "dbt DuckDB database not found. Run: "
            "cd dbt_quant && dbt run --profiles-dir ."
        )

    con = duckdb.connect(str(DBT_DUCKDB_PATH))

    summary = con.execute(
        """
        SELECT
            ticker,
            COUNT(*) AS n_rows,
            MIN(date) AS min_date,
            MAX(date) AS max_date,
            ROUND(AVG(ret_20d), 6) AS avg_ret_20d,
            ROUND(AVG(fwd_ret_20d), 6) AS avg_fwd_ret_20d,
            ROUND(AVG(fwd_excess_ret_20d_vs_spy), 6)
                AS avg_fwd_excess_ret_20d_vs_spy
        FROM ads_ml_research_panel
        GROUP BY ticker
        ORDER BY ticker;
        """
    ).fetchdf()

    print("ADS ML research panel summary:")
    print(summary.to_string(index=False))

    sample = con.execute(
        """
        SELECT
            ticker,
            date,
            adj_close,
            ret_20d,
            volatility_20d_lagged,
            dollar_volume_zscore_20d_lagged,
            close_to_20d_high_lagged,
            fwd_ret_20d,
            fwd_excess_ret_20d_vs_spy
        FROM ads_ml_research_panel
        WHERE fwd_ret_20d IS NOT NULL
        ORDER BY date DESC, ticker
        LIMIT 20;
        """
    ).fetchdf()

    print("\nRecent ADS sample:")
    print(sample.to_string(index=False))


if __name__ == "__main__":
    main()