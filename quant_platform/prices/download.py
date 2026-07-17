from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from quant_platform.clients.tiingo import (
    TiingoClientConfig,
    fetch_daily_prices,
)
from quant_platform.config.loaders import (
    load_yaml,
    optional_mapping,
    parse_iso_date,
    require_mapping,
)
from quant_platform.paths.data_lake import (
    ODS_ROOT,
    ensure_parent_dir,
    to_gcs_object_path,
)
from quant_platform.paths.price_paths import (
    build_windowed_price_raw_path,
    normalize_ticker,
)
from quant_platform.storage.gcs_sync import upload_file
from quant_platform.storage.local_json import (
    read_json_rows,
    write_json_rows,
)


REQUIRED_GAP_TASK_COLUMNS: tuple[str, ...] = (
    "source",
    "dataset_name",
    "ticker",
    "security_id",
    "request_start_date",
    "request_end_date",
    "reason",
)

DOWNLOAD_RESULT_COLUMNS: tuple[str, ...] = (
    "source",
    "dataset_name",
    "ticker",
    "security_id",
    "request_start_date",
    "request_end_date",
    "status",
    "row_count",
    "first_price_date",
    "last_price_date",
    "api_called",
    "uploaded_to_gcs",
    "local_path",
    "gcs_uri",
    "error_message",
    "completed_at_utc",
)


@dataclass(frozen=True)
class PriceDownloadSettings:
    filename: str = "prices.json"
    request_timeout_seconds: float = 60.0
    max_attempts: int = 3
    retry_sleep_seconds: float = 10.0
    sleep_seconds_between_requests: float = 0.25

    def __post_init__(self) -> None:
        if not self.filename.strip():
            raise ValueError("filename must not be empty")

        if self.request_timeout_seconds <= 0:
            raise ValueError(
                "request_timeout_seconds must be > 0"
            )

        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")

        if self.retry_sleep_seconds < 0:
            raise ValueError(
                "retry_sleep_seconds must be >= 0"
            )

        if self.sleep_seconds_between_requests < 0:
            raise ValueError(
                "sleep_seconds_between_requests must be >= 0"
            )


def load_price_download_settings(
    config_path: str | Path,
) -> PriceDownloadSettings:
    """Load download settings from price_update.yml."""
    config = load_yaml(config_path)
    price_update = require_mapping(config, "price_update")

    raw_layout = optional_mapping(
        price_update,
        "raw_window_layout",
        context="price_update",
    )
    download = optional_mapping(
        price_update,
        "download",
        context="price_update",
    )

    return PriceDownloadSettings(
        filename=str(
            raw_layout.get("filename", "prices.json")
        ).strip(),
        request_timeout_seconds=float(
            download.get("request_timeout_seconds", 60)
        ),
        max_attempts=int(
            download.get("max_attempts", 3)
        ),
        retry_sleep_seconds=float(
            download.get("retry_sleep_seconds", 10)
        ),
        sleep_seconds_between_requests=float(
            download.get(
                "sleep_seconds_between_requests",
                0.25,
            )
        ),
    )


def load_price_gap_tasks(path: str | Path) -> pd.DataFrame:
    """Load and validate the generated daily gap task list."""
    task_path = Path(path)

    if not task_path.exists():
        raise FileNotFoundError(
            f"Price gap task list not found: {task_path}"
        )

    tasks = pd.read_parquet(task_path)

    missing = [
        column
        for column in REQUIRED_GAP_TASK_COLUMNS
        if column not in tasks.columns
    ]

    if missing:
        raise ValueError(
            f"Price gap task list missing columns: {missing}"
        )

    required_nulls = [
        column
        for column in REQUIRED_GAP_TASK_COLUMNS
        if tasks[column].isna().any()
    ]

    if required_nulls:
        raise ValueError(
            "Price gap task list has nulls in required "
            f"columns: {required_nulls}"
        )

    output = tasks.copy()

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
    output["dataset_name"] = (
        output["dataset_name"]
        .astype(str)
        .str.strip()
    )

    for column in [
        "request_start_date",
        "request_end_date",
    ]:
        parsed = pd.to_datetime(
            output[column],
            errors="coerce",
        )

        if parsed.isna().any():
            examples = (
                output.loc[parsed.isna(), ["ticker", column]]
                .head(10)
                .to_dict("records")
            )

            raise ValueError(
                f"Invalid {column} values: {examples}"
            )

        output[column] = parsed.dt.date

    invalid_windows = (
        output["request_start_date"]
        > output["request_end_date"]
    )

    if invalid_windows.any():
        examples = (
            output.loc[
                invalid_windows,
                [
                    "ticker",
                    "request_start_date",
                    "request_end_date",
                ],
            ]
            .head(10)
            .to_dict("records")
        )

        raise ValueError(
            f"Invalid request windows: {examples}"
        )

    duplicate_keys = output.duplicated(
        subset=["ticker", "security_id"],
        keep=False,
    )

    if duplicate_keys.any():
        examples = (
            output.loc[
                duplicate_keys,
                ["ticker", "security_id"],
            ]
            .head(20)
            .to_dict("records")
        )

        raise ValueError(
            "Duplicate ticker/security_id tasks found: "
            f"{examples}"
        )

    return output.sort_values(
        ["ticker", "security_id"]
    ).reset_index(drop=True)


def parse_ticker_csv(
    raw_tickers: str | None,
) -> list[str] | None:
    """Parse an optional comma-separated ticker argument."""
    if not raw_tickers:
        return None

    tickers = [
        normalize_ticker(value)
        for value in raw_tickers.split(",")
        if value.strip()
    ]

    return list(dict.fromkeys(tickers)) or None


def select_price_download_tasks(
    tasks: pd.DataFrame,
    *,
    tickers: Sequence[str] | None = None,
    limit: int | None = None,
) -> pd.DataFrame:
    """Filter tasks by ticker and optional CLI test limit."""
    selected = tasks.copy()

    if tickers is not None:
        normalized_tickers = [
            normalize_ticker(ticker)
            for ticker in tickers
        ]

        available = set(selected["ticker"])
        missing = sorted(
            set(normalized_tickers) - available
        )

        if missing:
            raise ValueError(
                "Requested tickers are not present in the "
                f"gap task list: {missing}"
            )

        selected = selected[
            selected["ticker"].isin(normalized_tickers)
        ].copy()

    if limit is not None:
        if limit < 1:
            raise ValueError("limit must be >= 1")

        selected = selected.head(limit).copy()

    if selected.empty:
        raise ValueError("No price download tasks selected")

    return selected.reset_index(drop=True)


def validate_price_rows_for_window(
    rows: Sequence[Mapping[str, Any]],
    *,
    request_start_date: date,
    request_end_date: date,
) -> tuple[date | None, date | None]:
    """
    Validate returned Tiingo rows against the requested window.

    Empty responses are valid raw API responses and return `(None, None)`.
    """
    normalized_rows = [
        dict(row)
        for row in rows
    ]

    if not normalized_rows:
        return None, None

    if not all(
        isinstance(row, dict)
        for row in normalized_rows
    ):
        raise ValueError(
            "Tiingo rows must contain mappings only"
        )

    parsed = pd.to_datetime(
        [row.get("date") for row in normalized_rows],
        errors="coerce",
        utc=True,
    )

    invalid_dates = parsed.isna()

    if bool(invalid_dates.any()):
        bad_positions = [
            int(index)
            for index, invalid in enumerate(invalid_dates)
            if invalid
        ][:10]

        raise ValueError(
            "Tiingo response contains invalid dates at "
            f"row positions: {bad_positions}"
        )

    price_dates = [
        timestamp.date()
        for timestamp in parsed
    ]

    outside_window = [
        value
        for value in price_dates
        if (
            value < request_start_date
            or value > request_end_date
        )
    ]

    if outside_window:
        raise ValueError(
            "Tiingo response contains dates outside the "
            f"request window: {outside_window[:10]}"
        )

    duplicate_dates = (
        pd.Series(price_dates)
        .duplicated(keep=False)
    )

    if duplicate_dates.any():
        examples = (
            pd.Series(price_dates)[duplicate_dates]
            .drop_duplicates()
            .head(10)
            .tolist()
        )

        raise ValueError(
            "Tiingo response contains duplicate dates: "
            f"{examples}"
        )

    return min(price_dates), max(price_dates)


def build_price_download_plan(
    tasks: pd.DataFrame,
    *,
    ods_root: str | Path = ODS_ROOT,
    filename: str = "prices.json",
    overwrite: bool = False,
) -> pd.DataFrame:
    """Build a read-only download plan for selected tasks."""
    rows: list[dict[str, Any]] = []

    for task in tasks.to_dict("records"):
        start_date = parse_iso_date(
            task["request_start_date"]
        )
        end_date = parse_iso_date(
            task["request_end_date"]
        )

        local_path = build_windowed_price_raw_path(
            ods_root=ods_root,
            ticker=task["ticker"],
            request_start_date=start_date.isoformat(),
            request_end_date=end_date.isoformat(),
            filename=filename,
        )

        rows.append(
            {
                "ticker": task["ticker"],
                "security_id": task["security_id"],
                "request_start_date": start_date,
                "request_end_date": end_date,
                "local_path": local_path.as_posix(),
                "gcs_object_name": to_gcs_object_path(
                    local_path
                ),
                "file_exists": local_path.exists(),
                "would_call_api": (
                    overwrite or not local_path.exists()
                ),
            }
        )

    return pd.DataFrame(rows)


FetchFunction = Callable[..., list[dict[str, Any]]]
UploadFunction = Callable[..., str]


def process_price_download_task(
    task: Mapping[str, Any],
    *,
    client_config: TiingoClientConfig,
    settings: PriceDownloadSettings,
    ods_root: str | Path = ODS_ROOT,
    overwrite: bool = False,
    session: requests.Session | None = None,
    bucket: Any | None = None,
    fetch_fn: FetchFunction = fetch_daily_prices,
    upload_fn: UploadFunction = upload_file,
) -> dict[str, Any]:
    """Download or reuse one deterministic windowed raw file."""
    ticker = normalize_ticker(task["ticker"])
    security_id = str(task["security_id"]).strip()

    start_date = parse_iso_date(
        task["request_start_date"]
    )
    end_date = parse_iso_date(
        task["request_end_date"]
    )

    local_path = build_windowed_price_raw_path(
        ods_root=ods_root,
        ticker=ticker,
        request_start_date=start_date.isoformat(),
        request_end_date=end_date.isoformat(),
        filename=settings.filename,
    )

    api_called = False

    if local_path.exists() and not overwrite:
        rows = read_json_rows(local_path)

        status = (
            "existing_empty"
            if not rows
            else "existing"
        )
    else:
        rows = fetch_fn(
            ticker=ticker,
            start_date=start_date,
            end_date=end_date,
            config=client_config,
            session=session,
        )
        api_called = True

        validate_price_rows_for_window(
            rows,
            request_start_date=start_date,
            request_end_date=end_date,
        )

        write_json_rows(
            local_path,
            rows,
            indent=2,
        )

        status = "downloaded" if rows else "empty"

    first_date, last_date = (
        validate_price_rows_for_window(
            rows,
            request_start_date=start_date,
            request_end_date=end_date,
        )
    )

    gcs_uri: str | None = None

    if bucket is not None:
        gcs_uri = upload_fn(
            bucket=bucket,
            local_path=local_path,
        )

    return {
        "source": str(task["source"]),
        "dataset_name": str(task["dataset_name"]),
        "ticker": ticker,
        "security_id": security_id,
        "request_start_date": start_date,
        "request_end_date": end_date,
        "status": status,
        "row_count": len(rows),
        "first_price_date": first_date,
        "last_price_date": last_date,
        "api_called": api_called,
        "uploaded_to_gcs": gcs_uri is not None,
        "local_path": local_path.as_posix(),
        "gcs_uri": gcs_uri,
        "error_message": None,
        "completed_at_utc": datetime.now(
            timezone.utc
        ).isoformat(),
    }


def run_price_download_tasks(
    tasks: pd.DataFrame,
    *,
    client_config: TiingoClientConfig,
    settings: PriceDownloadSettings,
    ods_root: str | Path = ODS_ROOT,
    overwrite: bool = False,
    bucket: Any | None = None,
    fetch_fn: FetchFunction = fetch_daily_prices,
    result_callback: Callable[[dict[str, Any]], None] | None = None,
) -> pd.DataFrame:
    """Process selected tasks and continue after individual failures."""
    results: list[dict[str, Any]] = []
    task_records = tasks.to_dict("records")

    with requests.Session() as session:
        for index, task in enumerate(
            task_records,
            start=1,
        ):
            ticker = normalize_ticker(task["ticker"])

            start_date = parse_iso_date(
                task["request_start_date"]
            )
            end_date = parse_iso_date(
                task["request_end_date"]
            )

            local_path = build_windowed_price_raw_path(
                ods_root=ods_root,
                ticker=ticker,
                request_start_date=start_date.isoformat(),
                request_end_date=end_date.isoformat(),
                filename=settings.filename,
            )

            print(
                f"[{index}/{len(task_records)}] "
                f"{ticker} "
                f"{start_date} -> {end_date}"
            )

            api_expected = (
                overwrite or not local_path.exists()
            )

            try:
                result = process_price_download_task(
                        task,
                        client_config=client_config,
                        settings=settings,
                        ods_root=ods_root,
                        overwrite=overwrite,
                        session=session,
                        bucket=bucket,
                        fetch_fn=fetch_fn,
                    )

                print(
                    "  "
                    f"status={result['status']} "
                    f"rows={result['row_count']} "
                    f"last_date={result['last_price_date']} "
                    f"uploaded={result['uploaded_to_gcs']}"
                )

            except Exception as exc:
                result = {
                    "source": str(task["source"]),
                    "dataset_name": str(
                        task["dataset_name"]
                    ),
                    "ticker": ticker,
                    "security_id": str(
                        task["security_id"]
                    ),
                    "request_start_date": start_date,
                    "request_end_date": end_date,
                    "status": "failed",
                    "row_count": None,
                    "first_price_date": None,
                    "last_price_date": None,
                    "api_called": api_expected,
                    "uploaded_to_gcs": False,
                    "local_path": local_path.as_posix(),
                    "gcs_uri": None,
                    "error_message": repr(exc)[:2000],
                    "completed_at_utc": datetime.now(
                        timezone.utc
                    ).isoformat(),
                }

                print(
                    f"  status=failed error={repr(exc)}"
                )

            results.append(result)

            if result_callback is not None:
                result_callback(result)

            should_sleep = (
                index < len(task_records)
                and result["status"]
                not in {"existing", "existing_empty"}
                and settings.sleep_seconds_between_requests
                > 0
            )

            if should_sleep:
                time.sleep(
                    settings.sleep_seconds_between_requests
                )

    return pd.DataFrame(
        results,
        columns=list(DOWNLOAD_RESULT_COLUMNS),
    )


def save_price_download_results(
    results: pd.DataFrame,
    output_path: str | Path,
) -> Path:
    """Save one download-run result report as CSV."""
    path = Path(output_path)
    ensure_parent_dir(path)
    results.to_csv(path, index=False)

    return path


def print_price_download_summary(
    results: pd.DataFrame,
) -> None:
    """Print download-run status and API-call summary."""
    print("\nPrice download summary")
    print("----------------------")
    print(f"tasks: {len(results)}")

    if results.empty:
        return

    print("\nStatus counts:")
    print(
        results["status"]
        .value_counts(dropna=False)
        .to_string()
    )

    print(
        "\nAPI calls:",
        int(results["api_called"].fillna(False).sum()),
    )
    print(
        "GCS uploads:",
        int(
            results[
                "uploaded_to_gcs"
            ].fillna(False).sum()
        ),
    )
    print(
        "Rows returned:",
        int(
            pd.to_numeric(
                results["row_count"],
                errors="coerce",
            )
            .fillna(0)
            .sum()
        ),
    )