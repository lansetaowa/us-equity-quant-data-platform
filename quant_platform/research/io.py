from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path

import pandas as pd

from quant_platform.research.operation import parse_month_start


def subtract_months(
    month: str | date | pd.Timestamp,
    months: int,
) -> date:
    parsed = parse_month_start(
        month,
        field_name="month",
    )

    return (pd.Period(parsed, freq="M") - months).to_timestamp().date()


def _month_from_partition_path(path: Path) -> date | None:
    year_value: int | None = None
    month_value: int | None = None

    for part in path.parts:
        if part.startswith("year="):
            year_value = int(part.removeprefix("year="))
        elif part.startswith("month="):
            month_value = int(part.removeprefix("month="))

    if year_value is None or month_value is None:
        return None

    return date(
        year_value,
        month_value,
        1,
    )


def select_parquet_files_by_month(
    root: str | Path,
    *,
    start_month: str | date | None = None,
    end_month: str | date | None = None,
) -> list[Path]:
    root_path = Path(root)

    if not root_path.exists():
        raise FileNotFoundError(f"Parquet root not found: {root_path}")

    files = sorted(root_path.rglob("*.parquet"))

    if not files:
        raise FileNotFoundError(f"No parquet files found under {root_path}")

    start = (
        parse_month_start(start_month, field_name="start_month")
        if start_month is not None
        else None
    )
    end = (
        parse_month_start(end_month, field_name="end_month")
        if end_month is not None
        else None
    )

    if start is not None and end is not None and start > end:
        raise ValueError("start_month must be <= end_month")

    if start is None and end is None:
        return files

    selected: list[Path] = []
    unknown_partition_files: list[Path] = []

    for path in files:
        file_month = _month_from_partition_path(path)

        if file_month is None:
            unknown_partition_files.append(path)
            continue

        if start is not None and file_month < start:
            continue

        if end is not None and file_month > end:
            continue

        selected.append(path)

    selected.extend(unknown_partition_files)

    if not selected:
        raise FileNotFoundError(
            f"No parquet files selected under {root_path} "
            f"for month range {start} -> {end}"
        )

    return selected


def read_parquet_dataset_by_month(
    root: str | Path,
    *,
    date_column: str = "date",
    start_month: str | date | None = None,
    end_month: str | date | None = None,
) -> pd.DataFrame:
    files = select_parquet_files_by_month(
        root,
        start_month=start_month,
        end_month=end_month,
    )

    frame = pd.concat(
        [pd.read_parquet(path) for path in files],
        ignore_index=True,
    )

    if date_column not in frame.columns:
        raise ValueError(f"Missing date column: {date_column}")

    start = (
        parse_month_start(start_month, field_name="start_month")
        if start_month is not None
        else None
    )
    end = (
        parse_month_start(end_month, field_name="end_month")
        if end_month is not None
        else None
    )

    if start is not None or end is not None:
        parsed = pd.to_datetime(
            frame[date_column],
            errors="coerce",
        )

        if parsed.isna().any():
            raise ValueError(f"Invalid {date_column} values")

        months = parsed.dt.to_period("M").dt.to_timestamp().dt.date

        if start is not None:
            frame = frame[months >= start].copy()
            parsed = pd.to_datetime(frame[date_column], errors="coerce")
            months = parsed.dt.to_period("M").dt.to_timestamp().dt.date

        if end is not None:
            frame = frame[months <= end].copy()

    if frame.empty:
        raise ValueError(
            f"Parquet dataset is empty after filtering: {root}"
        )

    return frame.reset_index(drop=True)


def filter_frame_by_month(
    frame: pd.DataFrame,
    *,
    date_column: str = "date",
    start_month: str | date | None = None,
    end_month: str | date | None = None,
) -> pd.DataFrame:
    output = frame.copy()

    if date_column not in output.columns:
        raise ValueError(f"Missing date column: {date_column}")

    start = (
        parse_month_start(start_month, field_name="start_month")
        if start_month is not None
        else None
    )
    end = (
        parse_month_start(end_month, field_name="end_month")
        if end_month is not None
        else None
    )

    if start is not None and end is not None and start > end:
        raise ValueError("start_month must be <= end_month")

    if start is None and end is None:
        return output.reset_index(drop=True)

    parsed = pd.to_datetime(output[date_column], errors="coerce")

    if parsed.isna().any():
        raise ValueError(f"Invalid {date_column} values")

    months = parsed.dt.to_period("M").dt.to_timestamp().dt.date

    if start is not None:
        output = output[months >= start].copy()
        parsed = pd.to_datetime(output[date_column], errors="coerce")
        months = parsed.dt.to_period("M").dt.to_timestamp().dt.date

    if end is not None:
        output = output[months <= end].copy()

    return output.reset_index(drop=True)


def write_monthly_partitions(
    frame: pd.DataFrame,
    *,
    output_root: str | Path,
    date_column: str = "date",
    columns: list[str] | None = None,
    overwrite: bool = False,
    replace_existing_partitions: bool = False,
) -> list[Path]:
    if frame.empty:
        raise ValueError("Cannot write empty frame")

    root = Path(output_root)

    if overwrite and root.exists():
        shutil.rmtree(root)

    root.mkdir(parents=True, exist_ok=True)

    working = frame.copy()

    if columns is not None:
        working = working.loc[:, columns].copy()

    parsed = pd.to_datetime(
        working[date_column],
        errors="coerce",
    )

    if parsed.isna().any():
        raise ValueError(f"Invalid {date_column} values")

    working["_year"] = parsed.dt.year
    working["_month"] = parsed.dt.month

    written: list[Path] = []

    for (year, month), partition in working.groupby(
        ["_year", "_month"],
        sort=True,
    ):
        partition_dir = root / f"year={int(year)}" / f"month={int(month):02d}"

        if partition_dir.exists():
            if replace_existing_partitions or overwrite:
                shutil.rmtree(partition_dir)
            else:
                raise FileExistsError(
                    f"Feature partition already exists: {partition_dir}. "
                    "Use --replace-existing-partitions to replace it."
                )

        partition_dir.mkdir(parents=True, exist_ok=True)

        output_path = partition_dir / "part-000.parquet"

        output = partition.drop(columns=["_year", "_month"])

        sort_columns = [
            column
            for column in [
                "date",
                "ticker",
                "security_id",
            ]
            if column in output.columns
        ]

        if sort_columns:
            output = output.sort_values(sort_columns)

        output = output.reset_index(drop=True)
        output.to_parquet(output_path, index=False)
        written.append(output_path)

    return written