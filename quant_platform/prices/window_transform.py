from __future__ import annotations

import re
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from quant_platform.prices.normalize import (
    normalize_tiingo_price_payload,
)
from quant_platform.prices.schema import (
    DWD_PRICE_COLUMNS,
    empty_dwd_price_frame,
    select_dwd_price_columns,
)
from quant_platform.prices.transform import (
    combine_dwd_price_frames,
    validate_dwd_price_frame,
    write_year_month_partitions,
)
from quant_platform.storage.local_json import (
    read_json_rows,
    write_json,
)


TERMINAL_STATUSES = {
    "downloaded",
    "existing",
    "empty",
    "existing_empty",
    "skipped",
}

EMPTY_FILE_STATUSES = {
    "empty",
    "existing_empty",
}

NO_FILE_STATUSES = {
    "skipped",
}

REQUIRED_REPORT_COLUMNS = {
    "source",
    "dataset_name",
    "ticker",
    "security_id",
    "request_start_date",
    "request_end_date",
    "status",
    "row_count",
    "local_path",
}


@dataclass(frozen=True)
class WindowTransformPaths:
    run_id: str
    staging_root: Path
    archive_root: Path
    report_dir: Path


def derive_run_id(
    download_report: str | Path,
) -> str:
    """Derive a filesystem-safe transform run ID."""
    stem = Path(download_report).stem.strip()
    run_id = re.sub(
        r"[^A-Za-z0-9_.-]+",
        "_",
        stem,
    )

    if not run_id:
        raise ValueError(
            "Could not derive transform run_id"
        )

    return run_id


def build_run_paths(
    download_report: str | Path,
    *,
    staging_base: str | Path,
    archive_base: str | Path,
    report_base: str | Path,
) -> WindowTransformPaths:
    """Build deterministic staging, archive, and report paths."""
    run_id = derive_run_id(download_report)

    return WindowTransformPaths(
        run_id=run_id,
        staging_root=(
            Path(staging_base)
            / run_id
            / "equity_price_daily"
        ),
        archive_root=(
            Path(archive_base)
            / run_id
            / "equity_price_daily"
        ),
        report_dir=Path(report_base) / run_id,
    )


def load_download_report(
    path: str | Path,
) -> pd.DataFrame:
    """Load and validate a terminal window-download report."""
    report_path = Path(path)

    if not report_path.exists():
        raise FileNotFoundError(
            f"Download report not found: {report_path}"
        )

    report = pd.read_csv(report_path)

    missing = sorted(
        REQUIRED_REPORT_COLUMNS - set(report.columns)
    )

    if missing:
        raise ValueError(
            f"Download report missing columns: {missing}"
        )

    output = report.copy()

    output["ticker"] = (
        output["ticker"]
        .astype(str)
        .str.strip()
        .str.upper()
    )
    output["security_id"] = (
        output["security_id"]
        .astype(str)
        .str.strip()
    )
    output["source"] = (
        output["source"]
        .astype(str)
        .str.strip()
    )
    output["status"] = (
        output["status"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    output["row_count"] = pd.to_numeric(
        output["row_count"],
        errors="coerce",
    ).astype("Int64")

    if output["row_count"].isna().any():
        raise ValueError(
            "Download report contains invalid row_count"
        )

    for column in (
        "request_start_date",
        "request_end_date",
    ):
        parsed = pd.to_datetime(
            output[column],
            errors="coerce",
        )

        if parsed.isna().any():
            raise ValueError(
                f"Download report contains invalid {column}"
            )

        output[column] = parsed.dt.date

    unexpected_statuses = sorted(
        set(output["status"]) - TERMINAL_STATUSES
    )

    if unexpected_statuses:
        raise ValueError(
            "Download report contains non-terminal "
            f"statuses: {unexpected_statuses}"
        )
    
    duplicate_mask = output.duplicated(
        [
            "ticker",
            "security_id",
            "request_start_date",
            "request_end_date",
        ],
        keep=False,
    )

    if duplicate_mask.any():
        raise ValueError(
            "Download report contains duplicate tasks"
        )

    invalid_windows = (
        output["request_start_date"]
        > output["request_end_date"]
    )

    if invalid_windows.any():
        raise ValueError(
            "Download report contains invalid request windows"
        )

    return output.sort_values(
        ["ticker", "security_id"]
    ).reset_index(drop=True)


def _read_partition(
    dwd_root: str | Path,
    year: int,
    month: int,
) -> pd.DataFrame:
    """Read one canonical DWD year/month partition."""
    partition_dir = (
        Path(dwd_root)
        / f"year={year}"
        / f"month={month:02d}"
    )

    files = sorted(
        partition_dir.glob("*.parquet")
    )

    if not files:
        return empty_dwd_price_frame()

    frame = pd.concat(
        [
            pd.read_parquet(path)
            for path in files
        ],
        ignore_index=True,
    )

    frame = select_dwd_price_columns(frame)

    frame["date"] = pd.to_datetime(
        frame["date"],
        errors="raise",
    ).dt.date

    frame["loaded_at"] = (
        pd.to_datetime(
            frame["loaded_at"],
            format="mixed",
            errors="raise",
            utc=True,
        )
        .map(lambda value: value.isoformat())
    )

    validate_dwd_price_frame(frame)

    return frame.sort_values(
        ["ticker", "date"]
    ).reset_index(drop=True)


def _key_set(
    df: pd.DataFrame,
) -> set[tuple[str, Any]]:
    """Return canonical security_id/date keys."""
    if df.empty:
        return set()

    dates = pd.to_datetime(
        df["date"],
        errors="raise",
    ).dt.date

    return set(
        zip(
            df["security_id"].astype(str),
            dates,
        )
    )


def normalize_window_files(
    report: pd.DataFrame,
    *,
    load_id: str,
    loaded_at: datetime,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Normalize row-bearing files and audit terminal no-row results."""
    frames: list[pd.DataFrame] = []
    audit_rows: list[dict[str, Any]] = []
    input_row_count = 0

    for row in report.to_dict("records"):
        status = str(row["status"])
        reported_count = int(row["row_count"])

        local_path_value = row.get("local_path")

        if pd.isna(local_path_value):
            local_path_text = None
        else:
            local_path_text = str(local_path_value).strip() or None

        # Skipped is a terminal no-file result. It must not require or
        # attempt to read a local JSON file.
        if status in NO_FILE_STATUSES:
            if reported_count != 0:
                raise ValueError(
                    f"Skipped window has non-zero row_count "
                    f"for {row['ticker']}: {reported_count}"
                )

            audit_rows.append(
                {
                    "ticker": row["ticker"],
                    "security_id": row["security_id"],
                    "status": status,
                    "raw_row_count": 0,
                    "normalized_row_count": 0,
                    "min_price_date": None,
                    "max_price_date": None,
                    "local_path": local_path_text,
                }
            )
            continue

        if local_path_text is None:
            raise ValueError(
                f"Missing local_path for {row['ticker']} "
                f"with status={status}"
            )

        path = Path(local_path_text)

        if not path.exists():
            raise FileNotFoundError(
                f"Windowed raw file not found: {path}"
            )

        payload = read_json_rows(path)

        actual_count = len(payload)

        if actual_count != reported_count:
            raise ValueError(
                f"Row count mismatch for {row['ticker']}: "
                f"report={reported_count}, "
                f"file={actual_count}"
            )

        expected_empty = status in EMPTY_FILE_STATUSES

        if expected_empty != (actual_count == 0):
            raise ValueError(
                f"Status/file mismatch for {row['ticker']}: "
                f"status={status}, rows={actual_count}"
            )

        if not payload:
            audit_rows.append(
                {
                    "ticker": row["ticker"],
                    "security_id": row["security_id"],
                    "status": status,
                    "raw_row_count": 0,
                    "normalized_row_count": 0,
                    "min_price_date": None,
                    "max_price_date": None,
                    "local_path": path.as_posix(),
                }
            )
            continue

        frame = normalize_tiingo_price_payload(
            payload,
            ticker=row["ticker"],
            security_id=row["security_id"],
            load_id=load_id,
            loaded_at=loaded_at,
            source=row["source"],
        )

        validate_dwd_price_frame(frame)

        dates = pd.to_datetime(
            frame["date"],
            errors="raise",
        ).dt.date

        outside_window = (
            (dates < row["request_start_date"])
            | (dates > row["request_end_date"])
        )

        if outside_window.any():
            raise ValueError(
                "Normalized dates outside request window "
                f"for {row['ticker']}"
            )

        input_row_count += len(frame)
        frames.append(frame)

        audit_rows.append(
            {
                "ticker": row["ticker"],
                "security_id": row["security_id"],
                "status": status,
                "raw_row_count": actual_count,
                "normalized_row_count": len(frame),
                "min_price_date": dates.min(),
                "max_price_date": dates.max(),
                "local_path": path.as_posix(),
            }
        )

    normalized = combine_dwd_price_frames(
        frames
    )

    if len(normalized) != input_row_count:
        raise ValueError(
            "Duplicate security_id/date rows exist "
            "across window files"
        )

    return normalized, pd.DataFrame(audit_rows)


def preview_windowed_dwd_update(
    download_report: str | Path,
) -> dict[str, Any]:
    """Validate raw inputs and return a no-write transform preview."""
    report = load_download_report(
        download_report
    )

    normalized, audit = normalize_window_files(
        report,
        load_id=(
            f"preview:{derive_run_id(download_report)}"
        ),
        loaded_at=datetime.now(timezone.utc),
    )

    if normalized.empty:
        affected_partitions: list[tuple[int, int]] = []
    else:
        dates = pd.to_datetime(
            normalized["date"],
            errors="raise",
        )

        affected_partitions = sorted(
            {
                (
                    timestamp.year,
                    timestamp.month,
                )
                for timestamp in dates
            }
        )

    return {
        "task_count": len(report),
        "empty_window_count": int(
            report["status"]
            .isin(EMPTY_FILE_STATUSES)
            .sum()
        ),
        "skipped_window_count": int(
            report["status"]
            .isin(NO_FILE_STATUSES)
            .sum()
        ),
        "raw_row_count": int(
            audit["raw_row_count"].sum()
        ),
        "normalized_row_count": len(normalized),
        "affected_partitions": (
            affected_partitions
        ),
    }


def prepare_windowed_dwd_update(
    download_report: str | Path,
    *,
    dwd_root: str | Path,
    staging_base: str | Path,
    archive_base: str | Path,
    report_base: str | Path,
    overwrite: bool = False,
) -> WindowTransformPaths:
    """Build and validate staged affected DWD partitions."""
    report = load_download_report(
        download_report
    )

    paths = build_run_paths(
        download_report,
        staging_base=staging_base,
        archive_base=archive_base,
        report_base=report_base,
    )

    for path in (
        paths.staging_root,
        paths.report_dir,
    ):
        if path.exists():
            if not overwrite:
                raise FileExistsError(
                    f"Path already exists: {path}"
                )

            shutil.rmtree(path)

    paths.staging_root.mkdir(
        parents=True
    )
    paths.report_dir.mkdir(
        parents=True
    )

    prepared_at = datetime.now(
        timezone.utc
    )

    normalized, file_audit = normalize_window_files(
        report,
        load_id=(
            f"window_transform:{paths.run_id}"
        ),
        loaded_at=prepared_at,
    )

    if normalized.empty:
        raise ValueError(
            "No non-empty windowed price rows "
            "to transform"
        )

    working = normalized.copy()

    dates = pd.to_datetime(
        working["date"],
        errors="raise",
    )

    working["_year"] = dates.dt.year
    working["_month"] = dates.dt.month

    staged_partitions: list[pd.DataFrame] = []
    manifest_rows: list[dict[str, Any]] = []

    for (
        year,
        month,
    ), new_partition in working.groupby(
        ["_year", "_month"],
        sort=True,
    ):
        year_int = int(year)
        month_int = int(month)

        new_partition = (
            new_partition
            .drop(columns=["_year", "_month"])
            .loc[:, list(DWD_PRICE_COLUMNS)]
            .reset_index(drop=True)
        )

        existing = _read_partition(
            dwd_root,
            year_int,
            month_int,
        )

        existing_keys = _key_set(existing)
        new_keys = _key_set(new_partition)

        final_partition = (
            combine_dwd_price_frames(
                [existing, new_partition]
            )
        )

        expected_keys = (
            existing_keys | new_keys
        )

        if _key_set(final_partition) != expected_keys:
            raise ValueError(
                "Key reconciliation failed for "
                f"{year_int}-{month_int:02d}"
            )

        staged_partitions.append(
            final_partition
        )

        manifest_rows.append(
            {
                "year": year_int,
                "month": month_int,
                "existing_row_count": len(existing),
                "new_row_count": len(new_partition),
                "overlap_key_count": len(
                    existing_keys & new_keys
                ),
                "inserted_key_count": len(
                    new_keys - existing_keys
                ),
                "final_row_count": len(
                    final_partition
                ),
            }
        )

    all_staged_rows = pd.concat(
        staged_partitions,
        ignore_index=True,
    )

    write_year_month_partitions(
        all_staged_rows,
        paths.staging_root,
    )

    manifest = pd.DataFrame(
        manifest_rows
    )

    for row in manifest.to_dict("records"):
        staged_partition = _read_partition(
            paths.staging_root,
            int(row["year"]),
            int(row["month"]),
        )

        if len(staged_partition) != int(
            row["final_row_count"]
        ):
            raise ValueError(
                "Staged row count mismatch for "
                f"{row['year']}-"
                f"{int(row['month']):02d}"
            )

    file_audit.to_csv(
        paths.report_dir
        / "window_file_audit.csv",
        index=False,
    )

    manifest.to_csv(
        paths.report_dir
        / "partition_manifest.csv",
        index=False,
    )

    write_json(
        paths.report_dir
        / "prepare_summary.json",
        {
            "run_id": paths.run_id,
            "download_report": Path(
                download_report
            ).as_posix(),
            "prepared_at_utc": (
                prepared_at.isoformat()
            ),
            "task_count": len(report),
            "empty_window_count": int(
                report["status"]
                .isin(EMPTY_FILE_STATUSES)
                .sum()
            ),
            "skipped_window_count": int(
                report["status"]
                .isin(NO_FILE_STATUSES)
                .sum()
            ),
            "normalized_new_row_count": len(
                normalized
            ),
            "affected_partition_count": len(
                manifest
            ),
            "staging_root": (
                paths.staging_root.as_posix()
            ),
            "archive_root": (
                paths.archive_root.as_posix()
            ),
        },
    )

    return paths


def promote_windowed_dwd_update(
    download_report: str | Path,
    *,
    dwd_root: str | Path,
    staging_base: str | Path,
    archive_base: str | Path,
    report_base: str | Path,
) -> WindowTransformPaths:
    """Archive and replace only the prepared affected partitions."""
    paths = build_run_paths(
        download_report,
        staging_base=staging_base,
        archive_base=archive_base,
        report_base=report_base,
    )

    manifest_path = (
        paths.report_dir
        / "partition_manifest.csv"
    )

    if not manifest_path.exists():
        raise FileNotFoundError(
            f"Partition manifest not found: "
            f"{manifest_path}"
        )

    manifest = pd.read_csv(
        manifest_path
    )

    if manifest.empty:
        raise ValueError(
            "No staged partitions to promote"
        )

    items: list[dict[str, Any]] = []

    for row in manifest.to_dict("records"):
        year = int(row["year"])
        month = int(row["month"])

        stage_dir = (
            paths.staging_root
            / f"year={year}"
            / f"month={month:02d}"
        )

        final_dir = (
            Path(dwd_root)
            / f"year={year}"
            / f"month={month:02d}"
        )

        archive_dir = (
            paths.archive_root
            / f"year={year}"
            / f"month={month:02d}"
        )

        staged = _read_partition(
            paths.staging_root,
            year,
            month,
        )

        if len(staged) != int(
            row["final_row_count"]
        ):
            raise ValueError(
                "Staged partition changed after "
                "preparation"
            )

        if archive_dir.exists():
            raise FileExistsError(
                f"Archive already exists: "
                f"{archive_dir}"
            )

        items.append(
            {
                "year": year,
                "month": month,
                "stage_dir": stage_dir,
                "final_dir": final_dir,
                "archive_dir": archive_dir,
                "expected_rows": len(staged),
                "had_existing": (
                    final_dir.exists()
                ),
            }
        )

    try:
        # Archive every original partition before
        # changing the final DWD tree.
        for item in items:
            if item["had_existing"]:
                item[
                    "archive_dir"
                ].parent.mkdir(
                    parents=True,
                    exist_ok=True,
                )

                shutil.copytree(
                    item["final_dir"],
                    item["archive_dir"],
                )

        # Replace only affected partitions.
        # Staging remains intact for later inspection.
        for item in items:
            if item["final_dir"].exists():
                shutil.rmtree(
                    item["final_dir"]
                )

            item[
                "final_dir"
            ].parent.mkdir(
                parents=True,
                exist_ok=True,
            )

            shutil.copytree(
                item["stage_dir"],
                item["final_dir"],
            )

            promoted = _read_partition(
                dwd_root,
                item["year"],
                item["month"],
            )

            if len(promoted) != item[
                "expected_rows"
            ]:
                raise ValueError(
                    "Promoted row count mismatch"
                )

    except Exception:
        # Restore every affected partition.
        for item in items:
            if item["final_dir"].exists():
                shutil.rmtree(
                    item["final_dir"]
                )

            if item["archive_dir"].exists():
                shutil.copytree(
                    item["archive_dir"],
                    item["final_dir"],
                )

        raise

    write_json(
        paths.report_dir
        / "promotion_complete.json",
        {
            "run_id": paths.run_id,
            "promoted_at_utc": (
                datetime.now(
                    timezone.utc
                ).isoformat()
            ),
            "affected_partition_count": len(
                items
            ),
            "dwd_root": Path(
                dwd_root
            ).as_posix(),
            "archive_root": (
                paths.archive_root.as_posix()
            ),
        },
    )

    return paths