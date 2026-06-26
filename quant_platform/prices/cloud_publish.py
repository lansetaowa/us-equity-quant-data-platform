from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from quant_platform.paths.data_lake import (
    DATA_ROOT,
    DWD_PRICE_ROOT,
)
from quant_platform.storage.gcs_sync import (
    gcs_object_name_from_local_path,
    upload_file,
)


REQUIRED_MANIFEST_COLUMNS = {
    "year",
    "month",
    "final_row_count",
}


@dataclass(frozen=True)
class DwdPartitionPublishItem:
    year: int
    month: int
    expected_rows: int
    local_dir: Path
    gcs_prefix: str
    local_files: tuple[Path, ...]
    object_names: tuple[str, ...]


def load_partition_manifest(
    path: str | Path,
) -> pd.DataFrame:
    """Load and validate a Day 4 partition manifest."""
    input_path = Path(path)

    manifest_path = (
        input_path / "partition_manifest.csv"
        if input_path.is_dir()
        else input_path
    )

    if not manifest_path.exists():
        raise FileNotFoundError(
            f"Partition manifest not found: {manifest_path}"
        )

    manifest = pd.read_csv(manifest_path)

    missing = sorted(
        REQUIRED_MANIFEST_COLUMNS - set(manifest.columns)
    )

    if missing:
        raise ValueError(
            f"Partition manifest missing columns: {missing}"
        )

    output = manifest.copy()

    for column in ("year", "month", "final_row_count"):
        parsed = pd.to_numeric(
            output[column],
            errors="coerce",
        )

        if parsed.isna().any():
            raise ValueError(
                f"Partition manifest has invalid {column}"
            )

        output[column] = parsed.astype(int)

    invalid_months = ~output["month"].between(1, 12)

    if invalid_months.any():
        raise ValueError(
            "Partition manifest contains invalid months"
        )

    invalid_counts = output["final_row_count"] < 1

    if invalid_counts.any():
        raise ValueError(
            "Partition manifest contains non-positive "
            "final_row_count"
        )

    duplicates = output.duplicated(
        ["year", "month"],
        keep=False,
    )

    if duplicates.any():
        raise ValueError(
            "Partition manifest contains duplicate "
            "year/month entries"
        )

    return output.sort_values(
        ["year", "month"]
    ).reset_index(drop=True)


def _parquet_row_count(
    paths: tuple[Path, ...],
) -> int:
    return sum(
        int(
            pq.ParquetFile(
                str(path)
            ).metadata.num_rows
        )
        for path in paths
    )


def build_dwd_partition_publish_items(
    manifest: pd.DataFrame,
    *,
    dwd_root: str | Path = DWD_PRICE_ROOT,
    local_data_root: str | Path = DATA_ROOT,
    project_root: str | Path | None = None,
) -> list[DwdPartitionPublishItem]:
    """Build validated local-to-GCS partition mappings."""
    project_path = (
        Path(project_root)
        if project_root is not None
        else Path.cwd()
    )

    items: list[DwdPartitionPublishItem] = []

    for row in manifest.to_dict("records"):
        year = int(row["year"])
        month = int(row["month"])
        expected_rows = int(row["final_row_count"])

        local_dir = (
            Path(dwd_root)
            / f"year={year}"
            / f"month={month:02d}"
        )

        if not local_dir.is_dir():
            raise FileNotFoundError(
                f"Local DWD partition not found: {local_dir}"
            )

        local_files = tuple(
            sorted(local_dir.rglob("*.parquet"))
        )

        if not local_files:
            raise FileNotFoundError(
                f"No Parquet files found under {local_dir}"
            )

        actual_rows = _parquet_row_count(local_files)

        if actual_rows != expected_rows:
            raise ValueError(
                f"Local partition row count mismatch for "
                f"{year}-{month:02d}: "
                f"manifest={expected_rows}, "
                f"local={actual_rows}"
            )

        object_names = tuple(
            gcs_object_name_from_local_path(
                local_path=path,
                local_data_root=local_data_root,
                project_root=project_path,
            )
            for path in local_files
        )

        prefixes = {
            name.rsplit("/", 1)[0] + "/"
            for name in object_names
        }

        if len(prefixes) != 1:
            raise ValueError(
                f"Partition files map to multiple GCS "
                f"prefixes: {sorted(prefixes)}"
            )

        items.append(
            DwdPartitionPublishItem(
                year=year,
                month=month,
                expected_rows=expected_rows,
                local_dir=local_dir,
                gcs_prefix=next(iter(prefixes)),
                local_files=local_files,
                object_names=object_names,
            )
        )

    return items


def _list_remote_blobs(
    bucket: Any,
    prefix: str,
) -> dict[str, int | None]:
    return {
        blob.name: (
            int(blob.size)
            if blob.size is not None
            else None
        )
        for blob in bucket.list_blobs(prefix=prefix)
    }


def inspect_gcs_partition(
    bucket: Any,
    item: DwdPartitionPublishItem,
) -> dict[str, Any]:
    """Compare one local partition with its canonical GCS prefix."""
    local_sizes = {
        object_name: local_path.stat().st_size
        for local_path, object_name in zip(
            item.local_files,
            item.object_names,
            strict=True,
        )
    }

    remote_sizes = _list_remote_blobs(
        bucket,
        item.gcs_prefix,
    )

    local_names = set(local_sizes)
    remote_names = set(remote_sizes)

    missing_remote = sorted(
        local_names - remote_names
    )
    extra_remote = sorted(
        remote_names - local_names
    )

    size_mismatches = sorted(
        name
        for name in local_names & remote_names
        if remote_sizes[name] != local_sizes[name]
    )

    in_sync = not (
        missing_remote
        or extra_remote
        or size_mismatches
    )

    return {
        "year": item.year,
        "month": item.month,
        "expected_rows": item.expected_rows,
        "local_file_count": len(local_names),
        "remote_file_count": len(remote_names),
        "missing_remote_count": len(missing_remote),
        "extra_remote_count": len(extra_remote),
        "size_mismatch_count": len(size_mismatches),
        "local_bytes": sum(local_sizes.values()),
        "remote_bytes": sum(
            value or 0
            for value in remote_sizes.values()
        ),
        "gcs_prefix": item.gcs_prefix,
        "status": (
            "in_sync"
            if in_sync
            else "needs_sync"
        ),
    }


def build_gcs_sync_plan(
    bucket: Any,
    items: list[DwdPartitionPublishItem],
) -> pd.DataFrame:
    """Build a read-only GCS comparison report."""
    return pd.DataFrame(
        [
            inspect_gcs_partition(bucket, item)
            for item in items
        ]
    )


def sync_dwd_partition_to_gcs(
    bucket: Any,
    item: DwdPartitionPublishItem,
) -> dict[str, Any]:
    """
    Exact-sync one affected DWD partition.

    Local files are uploaded first. Remote objects that are not
    represented locally are deleted only after all uploads complete.
    """
    local_names = set(item.object_names)

    for local_path, object_name in zip(
        item.local_files,
        item.object_names,
        strict=True,
    ):
        upload_file(
            bucket=bucket,
            local_path=local_path,
            object_name=object_name,
        )

    remote_after_upload = _list_remote_blobs(
        bucket,
        item.gcs_prefix,
    )

    extra_names = sorted(
        set(remote_after_upload) - local_names
    )

    for object_name in extra_names:
        bucket.blob(object_name).delete()

    final_remote = _list_remote_blobs(
        bucket,
        item.gcs_prefix,
    )

    final_names = set(final_remote)

    if final_names != local_names:
        raise ValueError(
            f"GCS object-name reconciliation failed for "
            f"{item.gcs_prefix}"
        )

    local_sizes = {
        object_name: local_path.stat().st_size
        for local_path, object_name in zip(
            item.local_files,
            item.object_names,
            strict=True,
        )
    }

    size_mismatches = [
        object_name
        for object_name in local_names
        if final_remote[object_name]
        != local_sizes[object_name]
    ]

    if size_mismatches:
        raise ValueError(
            "GCS file-size verification failed for: "
            f"{size_mismatches}"
        )

    return {
        "year": item.year,
        "month": item.month,
        "gcs_prefix": item.gcs_prefix,
        "uploaded_file_count": len(item.local_files),
        "deleted_extra_count": len(extra_names),
        "verified_file_count": len(final_names),
        "status": "success",
    }


def sync_dwd_partitions_to_gcs(
    bucket: Any,
    items: list[DwdPartitionPublishItem],
) -> pd.DataFrame:
    """Exact-sync all affected DWD partitions."""
    return pd.DataFrame(
        [
            sync_dwd_partition_to_gcs(
                bucket,
                item,
            )
            for item in items
        ]
    )