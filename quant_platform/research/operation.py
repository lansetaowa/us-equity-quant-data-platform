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