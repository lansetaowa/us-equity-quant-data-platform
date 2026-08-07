from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pandas_market_calendars as mcal
import yaml


@dataclass(frozen=True)
class LiquidityFilterConfig:
    min_median_close: float
    min_trading_day_coverage: float
    min_median_dollar_volume: float
    require_positive_volume_days: bool


@dataclass(frozen=True)
class LiquidityBuildConfig:
    source_price_dwd_root: Path
    liquidity_monthly_output_root: Path
    calendar: str
    include_incomplete_months: bool
    filters: LiquidityFilterConfig


REQUIRED_PRICE_COLUMNS = {
    "security_id",
    "ticker",
    "date",
    "close",
    "volume",
}


LIQUIDITY_OUTPUT_COLUMNS = [
    "metric_month",
    "security_id",
    "ticker",
    "first_price_date",
    "last_price_date",
    "expected_trading_days",
    "trading_day_count",
    "trading_day_coverage",
    "price_day_count",
    "volume_day_count",
    "positive_volume_day_count",
    "zero_volume_day_count",
    "median_close",
    "median_liquidity_price",
    "median_dollar_volume",
    "avg_dollar_volume",
    "p20_dollar_volume",
    "p80_dollar_volume",
    "is_complete_month",
    "passes_price_filter",
    "passes_coverage_filter",
    "passes_dollar_volume_filter",
    "passes_positive_volume_filter",
    "passes_liquidity_filters",
    "liquidity_score",
    "created_at_utc",
]


def resolve_project_path(value: str | Path) -> Path:
    return Path(value)


def load_liquidity_build_config(
    config_path: str | Path,
) -> LiquidityBuildConfig:
    path = Path(config_path)

    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    data = yaml.safe_load(path.read_text(encoding="utf-8"))

    if not isinstance(data, dict):
        raise ValueError("liquid_universe.yml must contain a YAML mapping")

    config = data.get("liquid_universe")

    if not isinstance(config, dict):
        raise ValueError("liquid_universe config section is required")

    filters = config.get("filters", {})

    if not isinstance(filters, dict):
        raise ValueError("liquid_universe.filters must be a mapping")

    return LiquidityBuildConfig(
        source_price_dwd_root=resolve_project_path(
            config["source_price_dwd_root"]
        ),
        liquidity_monthly_output_root=resolve_project_path(
            config["liquidity_monthly_output_root"]
        ),
        calendar=str(config.get("calendar", "XNYS")),
        include_incomplete_months=bool(
            config.get("include_incomplete_months", False)
        ),
        filters=LiquidityFilterConfig(
            min_median_close=float(filters.get("min_median_close", 5.0)),
            min_trading_day_coverage=float(
                filters.get("min_trading_day_coverage", 0.80)
            ),
            min_median_dollar_volume=float(
                filters.get("min_median_dollar_volume", 1_000_000.0)
            ),
            require_positive_volume_days=bool(
                filters.get("require_positive_volume_days", True)
            ),
        ),
    )


def read_dwd_price_frame(
    dwd_root: str | Path,
) -> pd.DataFrame:
    root = Path(dwd_root)
    files = sorted(root.rglob("*.parquet"))

    if not files:
        raise FileNotFoundError(f"No DWD price parquet files found under {root}")

    frame = pd.concat(
        [pd.read_parquet(path) for path in files],
        ignore_index=True,
    )

    missing = sorted(REQUIRED_PRICE_COLUMNS - set(frame.columns))

    if missing:
        raise ValueError(f"DWD price frame missing columns: {missing}")

    return frame


def parse_month_start(
    value: str | date | pd.Timestamp | None,
    *,
    field_name: str,
) -> date | None:
    """Parse YYYY-MM-like input to month-start date."""
    if value is None or str(value).strip() == "":
        return None

    parsed = pd.to_datetime(
        str(value).strip(),
        errors="coerce",
    )

    if pd.isna(parsed):
        raise ValueError(f"Invalid {field_name}: {value!r}")

    return pd.Timestamp(parsed).to_period("M").to_timestamp().date()


def filter_liquidity_metrics_by_month(
    metrics: pd.DataFrame,
    *,
    start_month: str | date | None = None,
    end_month: str | date | None = None,
) -> pd.DataFrame:
    """Filter liquidity metrics by inclusive metric-month range."""
    output = metrics.copy()

    output["metric_month"] = pd.to_datetime(
        output["metric_month"],
        errors="raise",
    ).map(_month_start)

    start = parse_month_start(
        start_month,
        field_name="start_month",
    )
    end = parse_month_start(
        end_month,
        field_name="end_month",
    )

    if start is not None and end is not None and start > end:
        raise ValueError("start_month must be <= end_month")

    if start is not None:
        output = output[output["metric_month"] >= start].copy()

    if end is not None:
        output = output[output["metric_month"] <= end].copy()

    return output.reset_index(drop=True)


def liquidity_partition_dir(
    output_root: str | Path,
    metric_month: date,
) -> Path:
    """Return local liquidity metric partition directory."""
    month = pd.Timestamp(metric_month)

    return (
        Path(output_root)
        / f"year={month.year}"
        / f"month={month.month:02d}"
    )

def _month_start(value: Any) -> date:
    return pd.Timestamp(value).to_period("M").to_timestamp().date()


def _last_calendar_day_of_month(month_start: date) -> date:
    return (
        pd.Timestamp(month_start)
        .to_period("M")
        .end_time
        .date()
    )


def build_expected_trading_days_by_month(
    *,
    calendar_name: str,
    min_month: date,
    max_month: date,
) -> pd.DataFrame:
    calendar = mcal.get_calendar(calendar_name)

    start_date = min_month
    end_date = _last_calendar_day_of_month(max_month)

    schedule = calendar.schedule(
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
    )

    if schedule.empty:
        raise ValueError(
            f"No market sessions found for {calendar_name} "
            f"from {start_date} to {end_date}"
        )

    sessions = pd.DataFrame(
        {
            "session_date": pd.to_datetime(schedule.index).date,
        }
    )
    sessions["metric_month"] = sessions["session_date"].map(_month_start)

    monthly = (
        sessions.groupby("metric_month")
        .agg(
            expected_trading_days=("session_date", "nunique"),
            last_expected_session=("session_date", "max"),
        )
        .reset_index()
    )

    return monthly


def _numeric_series(
    frame: pd.DataFrame,
    column: str,
) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(pd.NA, index=frame.index, dtype="Float64")

    return pd.to_numeric(
        frame[column],
        errors="coerce",
    )


def prepare_price_frame_for_liquidity(
    price_frame: pd.DataFrame,
) -> pd.DataFrame:
    missing = sorted(REQUIRED_PRICE_COLUMNS - set(price_frame.columns))

    if missing:
        raise ValueError(f"DWD price frame missing columns: {missing}")

    output = price_frame.copy()

    output["security_id"] = output["security_id"].astype(str).str.strip()
    output["ticker"] = output["ticker"].astype(str).str.strip().str.upper()
    output["date"] = pd.to_datetime(output["date"], errors="coerce")

    if output["date"].isna().any():
        raise ValueError("DWD price frame contains invalid dates")

    output["metric_month"] = output["date"].map(_month_start)

    output["close"] = _numeric_series(output, "close")
    output["volume"] = _numeric_series(output, "volume")

    adj_close = _numeric_series(output, "adj_close")
    adj_volume = _numeric_series(output, "adj_volume")

    output["liquidity_price"] = adj_close.where(
        adj_close.notna(),
        output["close"],
    )
    output["liquidity_volume"] = adj_volume.where(
        adj_volume.notna(),
        output["volume"],
    )

    output["dollar_volume"] = (
        output["liquidity_price"] * output["liquidity_volume"]
    )

    output = output[
        output["security_id"].ne("")
        & output["ticker"].ne("")
        & output["date"].notna()
    ].copy()

    return output


def build_monthly_liquidity_metrics(
    price_frame: pd.DataFrame,
    *,
    calendar_name: str,
    filters: LiquidityFilterConfig,
    include_incomplete_months: bool,
) -> pd.DataFrame:
    working = prepare_price_frame_for_liquidity(price_frame)

    if working.empty:
        raise ValueError("Prepared price frame is empty")

    min_month = min(working["metric_month"])
    max_month = max(working["metric_month"])
    max_available_date = working["date"].dt.date.max()

    expected = build_expected_trading_days_by_month(
        calendar_name=calendar_name,
        min_month=min_month,
        max_month=max_month,
    )

    monthly = (
        working.groupby(["metric_month", "security_id", "ticker"])
        .agg(
            first_price_date=("date", "min"),
            last_price_date=("date", "max"),
            trading_day_count=("date", "nunique"),
            price_day_count=("liquidity_price", "count"),
            volume_day_count=("liquidity_volume", "count"),
            positive_volume_day_count=(
                "liquidity_volume",
                lambda value: int((value.fillna(0) > 0).sum()),
            ),
            zero_volume_day_count=(
                "liquidity_volume",
                lambda value: int((value.fillna(0) <= 0).sum()),
            ),
            median_close=("close", "median"),
            median_liquidity_price=("liquidity_price", "median"),
            median_dollar_volume=("dollar_volume", "median"),
            avg_dollar_volume=("dollar_volume", "mean"),
            p20_dollar_volume=(
                "dollar_volume",
                lambda value: value.quantile(0.20),
            ),
            p80_dollar_volume=(
                "dollar_volume",
                lambda value: value.quantile(0.80),
            ),
        )
        .reset_index()
    )

    monthly["first_price_date"] = pd.to_datetime(
        monthly["first_price_date"]
    ).dt.date
    monthly["last_price_date"] = pd.to_datetime(
        monthly["last_price_date"]
    ).dt.date

    monthly = monthly.merge(
        expected,
        on="metric_month",
        how="left",
    )

    if monthly["expected_trading_days"].isna().any():
        raise ValueError("Missing expected trading-day counts")

    monthly["expected_trading_days"] = monthly[
        "expected_trading_days"
    ].astype(int)

    monthly["trading_day_coverage"] = (
        monthly["trading_day_count"] / monthly["expected_trading_days"]
    )

    monthly["is_complete_month"] = (
        monthly["last_expected_session"] <= max_available_date
    )

    monthly["passes_price_filter"] = (
        monthly["median_close"] >= filters.min_median_close
    )
    monthly["passes_coverage_filter"] = (
        monthly["trading_day_coverage"]
        >= filters.min_trading_day_coverage
    )
    monthly["passes_dollar_volume_filter"] = (
        monthly["median_dollar_volume"]
        >= filters.min_median_dollar_volume
    )

    if filters.require_positive_volume_days:
        monthly["passes_positive_volume_filter"] = (
            monthly["positive_volume_day_count"] > 0
        )
    else:
        monthly["passes_positive_volume_filter"] = True

    monthly["passes_liquidity_filters"] = (
        monthly["passes_price_filter"]
        & monthly["passes_coverage_filter"]
        & monthly["passes_dollar_volume_filter"]
        & monthly["passes_positive_volume_filter"]
    )

    monthly["liquidity_score"] = monthly["median_dollar_volume"]

    created_at = datetime.now(UTC).isoformat()
    monthly["created_at_utc"] = created_at

    monthly = monthly.drop(columns=["last_expected_session"])

    if not include_incomplete_months:
        monthly = monthly[monthly["is_complete_month"]].copy()

    monthly = monthly.loc[:, LIQUIDITY_OUTPUT_COLUMNS]

    return monthly.sort_values(
        ["metric_month", "ticker", "security_id"]
    ).reset_index(drop=True)

def write_liquidity_metrics(
    metrics: pd.DataFrame,
    output_root: str | Path,
    *,
    overwrite: bool = False,
    missing_only: bool = False,
    replace_existing_partitions: bool = False,
) -> list[Path]:
    root = Path(output_root)

    selected_modes = sum(
        [
            bool(overwrite),
            bool(missing_only),
            bool(replace_existing_partitions),
        ]
    )

    if selected_modes > 1:
        raise ValueError(
            "Use only one of overwrite, missing_only, "
            "or replace_existing_partitions"
        )

    if metrics.empty:
        raise ValueError("Cannot write empty liquidity metrics")

    if overwrite:
        if root.exists():
            shutil.rmtree(root)

        root.mkdir(parents=True, exist_ok=True)

    elif root.exists() and not missing_only and not replace_existing_partitions:
        raise FileExistsError(
            f"Output root already exists: {root}. "
            "Use --overwrite for full rebuild, --missing-only for "
            "incremental writes, or --replace-existing-partitions for "
            "targeted correction."
        )

    else:
        root.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []

    working = metrics.copy()
    metric_month = pd.to_datetime(
        working["metric_month"],
        errors="raise",
    )
    working["metric_month"] = metric_month.map(_month_start)
    working["_year"] = metric_month.dt.year
    working["_month"] = metric_month.dt.month

    for (year, month), partition in working.groupby(
        ["_year", "_month"],
        sort=True,
    ):
        year_int = int(year)
        month_int = int(month)
        month_start = date(year_int, month_int, 1)

        partition_dir = liquidity_partition_dir(
            root,
            month_start,
        )

        if partition_dir.exists():
            if missing_only:
                continue

            if replace_existing_partitions:
                shutil.rmtree(partition_dir)

            elif not overwrite:
                raise FileExistsError(
                    f"Liquidity metric partition already exists: "
                    f"{partition_dir}"
                )

        partition_dir.mkdir(parents=True, exist_ok=True)

        output_path = partition_dir / "part-000.parquet"

        partition = (
            partition.drop(columns=["_year", "_month"])
            .reset_index(drop=True)
        )
        partition.to_parquet(output_path, index=False)

        written.append(output_path)

    return written


def summarize_liquidity_metrics(
    metrics: pd.DataFrame,
) -> dict[str, Any]:
    if metrics.empty:
        return {
            "rows": 0,
            "months": 0,
            "min_month": None,
            "max_month": None,
            "passing_rows": 0,
        }

    return {
        "rows": len(metrics),
        "months": int(metrics["metric_month"].nunique()),
        "min_month": str(min(metrics["metric_month"])),
        "max_month": str(max(metrics["metric_month"])),
        "security_ids": int(metrics["security_id"].nunique()),
        "tickers": int(metrics["ticker"].nunique()),
        "passing_rows": int(metrics["passes_liquidity_filters"].sum()),
        "complete_months": int(
            metrics.loc[
                metrics["is_complete_month"],
                "metric_month",
            ].nunique()
        ),
    }