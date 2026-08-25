from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd


def utc_now_string() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def default_run_id(prefix: str) -> str:
    return f"{prefix}_{utc_now_string()}"


def default_operation_id() -> str:
    return f"daily_data_{utc_now_string()}"


def parse_month_start(
    value: str | date | pd.Timestamp,
    *,
    field_name: str,
) -> date:
    parsed = pd.to_datetime(value, errors="coerce")

    if pd.isna(parsed):
        raise ValueError(f"Invalid {field_name}: {value!r}")

    return pd.Timestamp(parsed).to_period("M").to_timestamp().date()


def build_partition_manifest(
    *,
    frame: pd.DataFrame,
    date_column: str,
    output_paths: list[Path],
) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "year",
                "month",
                "row_count",
                "output_path",
            ]
        )

    parsed = pd.to_datetime(frame[date_column], errors="coerce")

    if parsed.isna().any():
        raise ValueError(f"Invalid {date_column} values")

    working = frame.copy()
    working["year"] = parsed.dt.year
    working["month"] = parsed.dt.month

    manifest = (
        working.groupby(["year", "month"], as_index=False)
        .size()
        .rename(columns={"size": "row_count"})
        .sort_values(["year", "month"])
        .reset_index(drop=True)
    )

    path_lookup: dict[tuple[int, int], str] = {}

    for path in output_paths:
        year_value: int | None = None
        month_value: int | None = None

        for part in path.parts:
            if part.startswith("year="):
                year_value = int(part.removeprefix("year="))
            elif part.startswith("month="):
                month_value = int(part.removeprefix("month="))

        if year_value is not None and month_value is not None:
            path_lookup[(year_value, month_value)] = path.as_posix()

    manifest["output_path"] = [
        path_lookup.get(
            (int(row["year"]), int(row["month"])),
            "",
        )
        for row in manifest.to_dict("records")
    ]

    return manifest


def write_build_reports(
    *,
    report_root: str | Path,
    run_id: str,
    summary: dict[str, Any],
    partition_manifest: pd.DataFrame,
) -> Path:
    report_dir = Path(report_root) / run_id
    report_dir.mkdir(parents=True, exist_ok=True)

    summary_path = report_dir / "summary.json"
    manifest_path = report_dir / "partition_manifest.csv"

    summary_path.write_text(
        json.dumps(
            summary,
            indent=2,
            default=str,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    partition_manifest.to_csv(
        manifest_path,
        index=False,
    )

    return report_dir


def month_range_from_partition_manifest_report(
    report_dir: str | Path,
) -> tuple[date, date, pd.DataFrame]:
    report_path = Path(report_dir)
    manifest_path = report_path / "partition_manifest.csv"

    if not manifest_path.exists():
        raise FileNotFoundError(
            f"partition_manifest.csv not found: {manifest_path}"
        )

    manifest = pd.read_csv(manifest_path)

    required = {"year", "month"}
    missing = required - set(manifest.columns)

    if missing:
        raise ValueError(
            f"partition_manifest.csv missing columns: {sorted(missing)}"
        )

    if manifest.empty:
        raise ValueError(f"partition_manifest.csv is empty: {manifest_path}")

    working = manifest.copy()
    working["year"] = pd.to_numeric(
        working["year"],
        errors="raise",
    ).astype(int)
    working["month"] = pd.to_numeric(
        working["month"],
        errors="raise",
    ).astype(int)

    working["month_start"] = [
        date(int(year), int(month), 1)
        for year, month in zip(
            working["year"],
            working["month"],
            strict=True,
        )
    ]

    return (
        min(working["month_start"]),
        max(working["month_start"]),
        working,
    )


def month_range_from_transform_report(
    transform_report_dir: str | Path,
) -> tuple[date, date, pd.DataFrame]:
    return month_range_from_partition_manifest_report(transform_report_dir)


def validate_manual_or_report_month_args(
    *,
    start_month: str | None,
    end_month: str | None,
    report_dir: Path | None,
    report_arg_name: str,
) -> None:
    manual_supplied = bool(start_month or end_month)
    report_supplied = report_dir is not None

    if manual_supplied and report_supplied:
        raise ValueError(
            f"Use either {report_arg_name} or "
            "--start-month/--end-month, not both"
        )

    if bool(start_month) != bool(end_month):
        raise ValueError(
            "--start-month and --end-month must be provided together"
        )


def resolve_output_month_range(
    *,
    start_month: str | None,
    end_month: str | None,
    report_dir: Path | None,
    report_arg_name: str,
) -> tuple[date | None, date | None, pd.DataFrame | None]:
    validate_manual_or_report_month_args(
        start_month=start_month,
        end_month=end_month,
        report_dir=report_dir,
        report_arg_name=report_arg_name,
    )

    if report_dir is not None:
        resolved_start, resolved_end, manifest = (
            month_range_from_partition_manifest_report(report_dir)
        )
        return resolved_start, resolved_end, manifest

    if start_month is None and end_month is None:
        return None, None, None

    return (
        parse_month_start(
            start_month,
            field_name="start_month",
        ),
        parse_month_start(
            end_month,
            field_name="end_month",
        ),
        None,
    )