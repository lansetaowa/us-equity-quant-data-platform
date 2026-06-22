from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

"""
python -m scripts.inspect_data --parquet "data/dwd/equity_price_daily/**/part-*.parquet" --limit 20

python -m scripts.inspect_data --duckdb data/dbt/quant.duckdb --table ads_ml_research_panel --limit 20

python -m scripts.inspect_data --duckdb data/dbt/quant.duckdb --list-tables

python -m scripts.inspect_data --duckdb data/dbt/quant.duckdb --sql "SELECT ticker, COUNT(*) AS n_rows, MIN(date), MAX(date) FROM ads_ml_research_panel GROUP BY ticker ORDER BY ticker"
"""


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sql_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def sql_table_name(value: str) -> str:
    return ".".join(sql_identifier(part) for part in value.split("."))


def print_section(title: str) -> None:
    print("\n" + "=" * 100)
    print(title)
    print("=" * 100)


def inspect_parquet(parquet_glob: str, limit: int) -> None:
    con = duckdb.connect()

    relation = f"read_parquet({sql_string(parquet_glob)})"

    print_section("Parquet schema")
    schema = con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchdf()
    print(schema.to_string(index=False))

    print_section("Row count")
    row_count = con.execute(f"SELECT COUNT(*) AS row_count FROM {relation}").fetchdf()
    print(row_count.to_string(index=False))

    print_section(f"Sample rows, limit={limit}")
    sample = con.execute(f"SELECT * FROM {relation} LIMIT {limit}").fetchdf()
    print(sample.to_string(index=False))


def list_duckdb_tables(db_path: Path) -> None:
    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB database not found: {db_path}")

    con = duckdb.connect(str(db_path), read_only=True)

    print_section("DuckDB tables")
    tables = con.execute("SHOW TABLES").fetchdf()
    print(tables.to_string(index=False))


def inspect_duckdb_table(db_path: Path, table: str, limit: int) -> None:
    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB database not found: {db_path}")

    con = duckdb.connect(str(db_path), read_only=True)
    table_sql = sql_table_name(table)

    print_section(f"DuckDB table schema: {table}")
    schema = con.execute(f"DESCRIBE SELECT * FROM {table_sql}").fetchdf()
    print(schema.to_string(index=False))

    print_section("Row count")
    row_count = con.execute(
        f"SELECT COUNT(*) AS row_count FROM {table_sql}"
    ).fetchdf()
    print(row_count.to_string(index=False))

    print_section(f"Sample rows, limit={limit}")
    sample = con.execute(f"SELECT * FROM {table_sql} LIMIT {limit}").fetchdf()
    print(sample.to_string(index=False))


def run_duckdb_sql(db_path: Path, query: str) -> None:
    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB database not found: {db_path}")

    con = duckdb.connect(str(db_path), read_only=True)

    print_section("Query result")
    result = con.execute(query).fetchdf()
    print(result.to_string(index=False))


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect Parquet files or DuckDB tables.")

    parser.add_argument("--parquet", help="Parquet file path or glob pattern.")
    parser.add_argument("--duckdb", help="DuckDB database path.")
    parser.add_argument("--table", help="DuckDB table name to inspect.")
    parser.add_argument("--sql", help="SQL query to run against DuckDB.")
    parser.add_argument("--list-tables", action="store_true", help="List DuckDB tables.")
    parser.add_argument("--limit", type=int, default=20, help="Number of sample rows.")

    args = parser.parse_args()

    if args.parquet:
        inspect_parquet(args.parquet, args.limit)
        return

    if args.duckdb:
        db_path = Path(args.duckdb)

        if args.list_tables:
            list_duckdb_tables(db_path)
            return

        if args.sql:
            run_duckdb_sql(db_path, args.sql)
            return

        if args.table:
            inspect_duckdb_table(db_path, args.table, args.limit)
            return

        raise ValueError("When using --duckdb, provide --list-tables, --table, or --sql.")

    raise ValueError("Provide either --parquet or --duckdb.")


if __name__ == "__main__":
    main()