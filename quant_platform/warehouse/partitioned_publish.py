from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from quant_platform.storage.gcs_sync import gcs_object_name_from_local_path


@dataclass(frozen=True)
class PartitionedTableSpec:
    table_name: str
    partition_column: str
    key_columns: tuple[str, ...]
    clustering_fields: tuple[str, ...]


@dataclass(frozen=True)
class LocalPartitionFile:
    local_path: Path
    gcs_uri: str
    month: date
    row_count: int


def parse_month_start(
    value: str | date | pd.Timestamp | None,
    *,
    field_name: str,
) -> date | None:
    if value is None or str(value).strip() == "":
        return None

    parsed = pd.to_datetime(
        str(value).strip(),
        errors="coerce",
    )

    if pd.isna(parsed):
        raise ValueError(f"Invalid {field_name}: {value!r}")

    return pd.Timestamp(parsed).to_period("M").to_timestamp().date()


def build_table_id(
    *,
    project_id: str,
    dataset_id: str,
    table_name: str,
) -> str:
    return f"{project_id}.{dataset_id}.{table_name}"


def sanitize_table_component(value: str) -> str:
    normalized = re.sub(
        r"[^A-Za-z0-9_]+",
        "_",
        str(value),
    ).strip("_")

    if not normalized:
        raise ValueError("Table component must not be empty")

    if normalized[0].isdigit():
        normalized = f"run_{normalized}"

    return normalized


def build_staging_table_id(
    *,
    project_id: str,
    dataset_id: str,
    table_name: str,
    suffix: str,
) -> str:
    clean_suffix = sanitize_table_component(suffix)

    return build_table_id(
        project_id=project_id,
        dataset_id=dataset_id,
        table_name=f"{table_name}__stg_{clean_suffix}"[:1024],
    )


def table_exists(
    client: bigquery.Client,
    table_id: str,
) -> bool:
    try:
        client.get_table(table_id)
    except NotFound:
        return False

    return True


def _quote_columns(columns: tuple[str, ...] | list[str]) -> str:
    return ", ".join(f"`{column}`" for column in columns)


def _month_range_predicate(
    partition_column: str,
    *,
    start_month: date,
    end_month: date,
) -> str:
    return (
        f"`{partition_column}` >= DATE '{start_month.isoformat()}' "
        f"AND `{partition_column}` <= DATE '{end_month.isoformat()}'"
    )


def discover_local_partition_files(
    *,
    local_root: str | Path,
    bucket_name: str,
    partition_column: str,
    start_month: str | date,
    end_month: str | date,
) -> list[LocalPartitionFile]:
    root = Path(local_root)

    if not root.exists():
        raise FileNotFoundError(f"Local root not found: {root}")

    if not bucket_name:
        raise ValueError("bucket_name must not be empty")

    start = parse_month_start(
        start_month,
        field_name="start_month",
    )
    end = parse_month_start(
        end_month,
        field_name="end_month",
    )

    if start is None or end is None:
        raise ValueError("start_month and end_month are required")

    if start > end:
        raise ValueError("start_month must be <= end_month")

    selected: list[LocalPartitionFile] = []

    for path in sorted(root.rglob("*.parquet")):
        frame = pd.read_parquet(
            path,
            columns=[partition_column],
        )

        if frame.empty:
            continue

        months = (
            pd.to_datetime(frame[partition_column], errors="coerce")
            .dt.to_period("M")
            .dt.to_timestamp()
            .dt.date
            .dropna()
            .unique()
        )

        if len(months) != 1:
            raise ValueError(
                f"Expected exactly one {partition_column} in {path}, "
                f"got {months}"
            )

        month_value = months[0]

        if month_value < start or month_value > end:
            continue

        object_name = gcs_object_name_from_local_path(path)

        selected.append(
            LocalPartitionFile(
                local_path=path,
                gcs_uri=f"gs://{bucket_name}/{object_name}",
                month=month_value,
                row_count=len(frame),
            )
        )

    if not selected:
        raise FileNotFoundError(
            f"No local parquet files selected under {root}"
        )

    return selected


def query_one(
    client: bigquery.Client,
    sql: str,
    *,
    location: str,
) -> dict[str, Any]:
    rows = list(client.query(sql, location=location).result())

    if len(rows) != 1:
        raise ValueError(f"Expected one query row, got {len(rows)}")

    return dict(rows[0].items())


def load_gcs_uris_to_table(
    *,
    client: bigquery.Client,
    destination_table: str,
    gcs_uris: list[str],
    spec: PartitionedTableSpec,
    location: str,
    write_disposition: str,
) -> None:
    if not gcs_uris:
        raise ValueError("No GCS URIs supplied")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=write_disposition,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=spec.partition_column,
        ),
        clustering_fields=list(spec.clustering_fields),
    )

    job = client.load_table_from_uri(
        gcs_uris,
        destination_table,
        job_config=job_config,
        location=location,
    )
    job.result()


def validate_partitioned_table_range(
    *,
    client: bigquery.Client,
    table_id: str,
    spec: PartitionedTableSpec,
    start_month: date,
    end_month: date,
    expected_rows: int,
    location: str,
) -> dict[str, Any]:
    predicate = _month_range_predicate(
        spec.partition_column,
        start_month=start_month,
        end_month=end_month,
    )

    summary = query_one(
        client,
        f"""
        SELECT
          COUNT(*) AS row_count,
          MIN(`{spec.partition_column}`) AS min_month,
          MAX(`{spec.partition_column}`) AS max_month
        FROM `{table_id}`
        WHERE {predicate}
        """,
        location=location,
    )

    key_columns_sql = _quote_columns(list(spec.key_columns))

    duplicates = query_one(
        client,
        f"""
        SELECT COUNT(*) AS duplicate_key_count
        FROM (
          SELECT {key_columns_sql}, COUNT(*) AS n
          FROM `{table_id}`
          WHERE {predicate}
          GROUP BY {key_columns_sql}
          HAVING n > 1
        )
        """,
        location=location,
    )

    result = {
        "row_count": int(summary["row_count"]),
        "min_month": (
            summary["min_month"].isoformat()
            if summary["min_month"] is not None
            else None
        ),
        "max_month": (
            summary["max_month"].isoformat()
            if summary["max_month"] is not None
            else None
        ),
        "duplicate_key_count": int(
            duplicates["duplicate_key_count"]
        ),
    }

    if result["row_count"] != expected_rows:
        raise ValueError(
            "BigQuery row-count mismatch: "
            f"expected={expected_rows}, actual={result['row_count']}"
        )

    if result["duplicate_key_count"] != 0:
        raise ValueError(
            "BigQuery duplicate-key validation failed: "
            f"{result['duplicate_key_count']}"
        )

    return result


def replace_target_months_from_staging(
    *,
    client: bigquery.Client,
    target_table_id: str,
    staging_table_id: str,
    spec: PartitionedTableSpec,
    start_month: date,
    end_month: date,
    location: str,
) -> None:
    target = client.get_table(target_table_id)
    staging = client.get_table(staging_table_id)

    target_columns = [field.name for field in target.schema]
    staging_columns = {field.name for field in staging.schema}

    missing = sorted(set(target_columns) - staging_columns)

    if missing:
        raise ValueError(f"Staging table missing target columns: {missing}")

    columns_sql = _quote_columns(target_columns)

    predicate = _month_range_predicate(
        spec.partition_column,
        start_month=start_month,
        end_month=end_month,
    )

    sql = f"""
    BEGIN TRANSACTION;

    DELETE FROM `{target_table_id}`
    WHERE {predicate};

    INSERT INTO `{target_table_id}` (
      {columns_sql}
    )
    SELECT
      {columns_sql}
    FROM `{staging_table_id}`;

    COMMIT TRANSACTION;
    """

    client.query(sql, location=location).result()


def drop_table(
    client: bigquery.Client,
    table_id: str,
) -> None:
    client.delete_table(
        table_id,
        not_found_ok=True,
    )