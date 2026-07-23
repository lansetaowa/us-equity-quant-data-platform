from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd
from google.cloud import bigquery

from quant_platform.prices.schema import (
    DWD_PRICE_COLUMNS,
    DWD_PRICE_KEY_COLUMNS,
)


@dataclass(frozen=True)
class MonthRange:
    year: int
    month: int
    start_date: date
    end_date_exclusive: date


def _next_month_start(
    year: int,
    month: int,
) -> date:
    if month == 12:
        return date(year + 1, 1, 1)

    return date(year, month + 1, 1)


def month_ranges_from_manifest(
    manifest: pd.DataFrame,
) -> list[MonthRange]:
    ranges: list[MonthRange] = []

    for row in manifest.to_dict("records"):
        year = int(row["year"])
        month = int(row["month"])

        ranges.append(
            MonthRange(
                year=year,
                month=month,
                start_date=date(year, month, 1),
                end_date_exclusive=(
                    _next_month_start(
                        year,
                        month,
                    )
                ),
            )
        )

    return sorted(
        ranges,
        key=lambda value: value.start_date,
    )


def build_date_predicate(
    column_sql: str,
    ranges: Sequence[MonthRange],
) -> str:
    if not ranges:
        raise ValueError(
            "At least one month range is required"
        )

    predicates = [
        (
            f"({column_sql} >= DATE "
            f"'{value.start_date.isoformat()}' "
            f"AND {column_sql} < DATE "
            f"'{value.end_date_exclusive.isoformat()}')"
        )
        for value in ranges
    ]

    return " OR ".join(predicates)


def sanitize_table_component(value: str) -> str:
    normalized = re.sub(
        r"[^A-Za-z0-9_]+",
        "_",
        str(value),
    ).strip("_")

    if not normalized:
        raise ValueError(
            "Table component must not be empty"
        )

    if normalized[0].isdigit():
        normalized = f"run_{normalized}"

    return normalized


def build_table_id(
    project_id: str,
    dataset_id: str,
    table_name: str,
) -> str:
    return (
        f"{project_id}.{dataset_id}.{table_name}"
    )


def build_staging_table_id(
    project_id: str,
    dataset_id: str,
    target_table_name: str,
    run_id: str,
) -> str:
    suffix = sanitize_table_component(run_id)

    table_name = (
        f"{target_table_name}__stg_{suffix}"
    )[:1024]

    return build_table_id(
        project_id,
        dataset_id,
        table_name,
    )


def summarize_manifest(
    manifest: pd.DataFrame,
) -> dict[str, int]:
    required = {
        "existing_row_count",
        "inserted_key_count",
        "final_row_count",
    }

    missing = sorted(
        required - set(manifest.columns)
    )

    if missing:
        raise ValueError(
            f"Manifest missing columns: {missing}"
        )

    return {
        "expected_existing_rows": int(
            manifest[
                "existing_row_count"
            ].sum()
        ),
        "expected_inserted_rows": int(
            manifest[
                "inserted_key_count"
            ].sum()
        ),
        "expected_final_rows": int(
            manifest["final_row_count"].sum()
        ),
    }


def validate_target_table_definition(
    table: Any,
) -> None:
    partition_field = (
        table.time_partitioning.field
        if table.time_partitioning
        else None
    )

    if partition_field != "date":
        raise ValueError(
            "Target BigQuery table must be "
            "partitioned by date"
        )

    clustering = list(
        table.clustering_fields or []
    )

    if clustering != [
        "ticker",
        "security_id",
    ]:
        raise ValueError(
            "Target BigQuery clustering must be "
            "['ticker', 'security_id']"
        )

    actual_columns = {
        field.name
        for field in table.schema
    }
    expected_columns = set(
        DWD_PRICE_COLUMNS
    )

    if actual_columns != expected_columns:
        raise ValueError(
            "Target BigQuery schema does not match "
            "the canonical DWD price schema"
        )


def _query_one(
    client: Any,
    sql: str,
    *,
    location: str,
) -> dict[str, Any]:
    rows = list(
        client.query(
            sql,
            location=location,
        ).result()
    )

    if len(rows) != 1:
        raise ValueError(
            f"Expected one query row, got {len(rows)}"
        )

    return dict(rows[0].items())


def _date_to_text(
    value: Any,
) -> str | None:
    if value is None:
        return None

    if hasattr(value, "isoformat"):
        return value.isoformat()

    return str(value)


def get_affected_table_summary(
    client: Any,
    table_id: str,
    ranges: Sequence[MonthRange],
    *,
    location: str,
) -> dict[str, Any]:
    predicate = build_date_predicate(
        "`date`",
        ranges,
    )

    summary = _query_one(
        client,
        f"""
        SELECT
          COUNT(*) AS n_rows,
          MIN(date) AS min_date,
          MAX(date) AS max_date
        FROM `{table_id}`
        WHERE {predicate}
        """,
        location=location,
    )

    duplicates = _query_one(
        client,
        f"""
        SELECT COUNT(*) AS duplicate_groups
        FROM (
          SELECT security_id, date
          FROM `{table_id}`
          WHERE {predicate}
          GROUP BY security_id, date
          HAVING COUNT(*) > 1
        )
        """,
        location=location,
    )

    return {
        "n_rows": int(summary["n_rows"]),
        "min_date": _date_to_text(
            summary["min_date"]
        ),
        "max_date": _date_to_text(
            summary["max_date"]
        ),
        "duplicate_groups": int(
            duplicates["duplicate_groups"]
        ),
    }


def classify_target_state(
    actual_rows: int,
    *,
    expected_existing_rows: int,
    expected_final_rows: int,
) -> str:
    if actual_rows == expected_existing_rows:
        return "pre_update"

    if actual_rows == expected_final_rows:
        return "already_updated"

    return "unexpected"


def load_staging_table(
    client: Any,
    *,
    target_table_id: str,
    staging_table_id: str,
    gcs_uris: Sequence[str],
    location: str,
) -> Any:
    if not gcs_uris:
        raise ValueError(
            "At least one GCS URI is required"
        )

    target = client.get_table(
        target_table_id
    )
    validate_target_table_definition(target)

    client.delete_table(
        staging_table_id,
        not_found_ok=True,
    )

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = (
        bigquery.SourceFormat.PARQUET
    )
    job_config.schema = list(target.schema)
    job_config.write_disposition = (
        bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    job_config.create_disposition = (
        bigquery.CreateDisposition.CREATE_IF_NEEDED
    )

    job = client.load_table_from_uri(
        list(gcs_uris),
        staging_table_id,
        job_config=job_config,
        location=location,
    )

    job.result()

    return client.get_table(
        staging_table_id
    )


def validate_staging_table(
    client: Any,
    *,
    staging_table_id: str,
    target_table_id: str,
    ranges: Sequence[MonthRange],
    expected_rows: int,
    location: str,
) -> dict[str, Any]:
    staging = client.get_table(
        staging_table_id
    )
    target = client.get_table(
        target_table_id
    )

    staging_columns = {
        field.name
        for field in staging.schema
    }
    target_columns = {
        field.name
        for field in target.schema
    }

    if staging_columns != target_columns:
        raise ValueError(
            "Staging and target schemas differ"
        )

    predicate = build_date_predicate(
        "`date`",
        ranges,
    )

    summary = _query_one(
        client,
        f"""
        SELECT
          COUNT(*) AS n_rows,
          MIN(date) AS min_date,
          MAX(date) AS max_date,
          COUNTIF(NOT ({predicate})) AS outside_rows
        FROM `{staging_table_id}`
        """,
        location=location,
    )

    duplicates = _query_one(
        client,
        f"""
        SELECT COUNT(*) AS duplicate_groups
        FROM (
          SELECT security_id, date
          FROM `{staging_table_id}`
          GROUP BY security_id, date
          HAVING COUNT(*) > 1
        )
        """,
        location=location,
    )

    result = {
        "n_rows": int(summary["n_rows"]),
        "min_date": _date_to_text(
            summary["min_date"]
        ),
        "max_date": _date_to_text(
            summary["max_date"]
        ),
        "outside_rows": int(
            summary["outside_rows"]
        ),
        "duplicate_groups": int(
            duplicates["duplicate_groups"]
        ),
    }

    if result["n_rows"] != expected_rows:
        raise ValueError(
            "BigQuery staging row count mismatch: "
            f"expected={expected_rows}, "
            f"actual={result['n_rows']}"
        )

    if result["outside_rows"] != 0:
        raise ValueError(
            "BigQuery staging contains rows outside "
            "the affected month range"
        )

    if result["duplicate_groups"] != 0:
        raise ValueError(
            "BigQuery staging contains duplicate "
            "security_id/date keys"
        )

    return result


def build_replace_sql(
    *,
    target_table_id: str,
    staging_table_id: str,
    ranges: Sequence[MonthRange],
) -> str:
    predicate = build_date_predicate(
        "`date`",
        ranges,
    )

    columns = ", ".join(
        f"`{column}`"
        for column in DWD_PRICE_COLUMNS
    )

    return f"""
    BEGIN TRANSACTION;

    DELETE FROM `{target_table_id}`
    WHERE {predicate};

    INSERT INTO `{target_table_id}` (
      {columns}
    )
    SELECT
      {columns}
    FROM `{staging_table_id}`;

    COMMIT TRANSACTION;
    """


def apply_staging_to_target(
    client: Any,
    *,
    target_table_id: str,
    staging_table_id: str,
    ranges: Sequence[MonthRange],
    location: str,
) -> None:
    sql = build_replace_sql(
        target_table_id=target_table_id,
        staging_table_id=staging_table_id,
        ranges=ranges,
    )

    client.query(
        sql,
        location=location,
    ).result()


def validate_target_after_update(
    client: Any,
    *,
    target_table_id: str,
    staging_table_id: str,
    ranges: Sequence[MonthRange],
    expected_rows: int,
    location: str,
) -> dict[str, Any]:
    affected = get_affected_table_summary(
        client,
        target_table_id,
        ranges,
        location=location,
    )

    join_condition = (
        "s.`security_id` = t.`security_id` "
        "AND s.`date` = t.`date`"
    )

    target_predicate = build_date_predicate(
        "t.`date`",
        ranges,
    )

    missing = _query_one(
        client,
        f"""
        SELECT COUNT(*) AS missing_keys
        FROM `{staging_table_id}` AS s
        LEFT JOIN `{target_table_id}` AS t
          ON {join_condition}
        WHERE t.`security_id` IS NULL
        """,
        location=location,
    )

    extra = _query_one(
        client,
        f"""
        SELECT COUNT(*) AS extra_keys
        FROM `{target_table_id}` AS t
        LEFT JOIN `{staging_table_id}` AS s
          ON {join_condition}
        WHERE ({target_predicate})
          AND s.`security_id` IS NULL
        """,
        location=location,
    )

    non_key_columns = [
        column
        for column in DWD_PRICE_COLUMNS
        if column not in DWD_PRICE_KEY_COLUMNS
    ]

    mismatch_condition = " OR ".join(
        (
            f"s.`{column}` IS DISTINCT FROM "
            f"t.`{column}`"
        )
        for column in non_key_columns
    )

    mismatched = _query_one(
        client,
        f"""
        SELECT COUNT(*) AS mismatched_rows
        FROM `{staging_table_id}` AS s
        JOIN `{target_table_id}` AS t
          ON {join_condition}
        WHERE {mismatch_condition}
        """,
        location=location,
    )

    result = {
        **affected,
        "missing_keys": int(
            missing["missing_keys"]
        ),
        "extra_keys": int(
            extra["extra_keys"]
        ),
        "mismatched_rows": int(
            mismatched["mismatched_rows"]
        ),
    }

    if result["n_rows"] != expected_rows:
        raise ValueError(
            "Affected BigQuery target row count "
            "does not match staging"
        )

    for field in (
        "duplicate_groups",
        "missing_keys",
        "extra_keys",
        "mismatched_rows",
    ):
        if result[field] != 0:
            raise ValueError(
                f"BigQuery target validation failed: "
                f"{field}={result[field]}"
            )

    return result


def get_table_summary(
    client: Any,
    table_id: str,
    *,
    location: str,
) -> dict[str, Any]:
    result = _query_one(
        client,
        f"""
        SELECT
          COUNT(*) AS n_rows,
          COUNT(DISTINCT ticker) AS n_tickers,
          COUNT(DISTINCT security_id)
            AS n_security_ids,
          MIN(date) AS min_date,
          MAX(date) AS max_date
        FROM `{table_id}`
        """,
        location=location,
    )

    return {
        "n_rows": int(result["n_rows"]),
        "n_tickers": int(
            result["n_tickers"]
        ),
        "n_security_ids": int(
            result["n_security_ids"]
        ),
        "min_date": _date_to_text(
            result["min_date"]
        ),
        "max_date": _date_to_text(
            result["max_date"]
        ),
    }


def drop_staging_table(
    client: Any,
    staging_table_id: str,
) -> None:
    client.delete_table(
        staging_table_id,
        not_found_ok=True,
    )