from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import yaml


@dataclass(frozen=True)
class UniverseSpec:
    name: str
    size: int


@dataclass(frozen=True)
class UniverseMembershipConfig:
    liquidity_monthly_output_root: Path
    universe_membership_output_root: Path
    liquidity_score_method: str
    score_aggregation: str
    lookback_months: int
    universes: tuple[UniverseSpec, ...]


REQUIRED_LIQUIDITY_COLUMNS = {
    "metric_month",
    "security_id",
    "ticker",
    "trading_day_count",
    "expected_trading_days",
    "trading_day_coverage",
    "median_close",
    "median_dollar_volume",
    "last_price_date",
    "is_complete_month",
    "passes_liquidity_filters",
}


UNIVERSE_MEMBERSHIP_COLUMNS = [
    "universe_name",
    "universe_size",
    "membership_month",
    "effective_start_date",
    "effective_end_date",
    "security_id",
    "ticker",
    "rank",
    "liquidity_score",
    "score_metric_name",
    "score_aggregation",
    "score_observation_count",
    "source_metric_month",
    "lookback_months",
    "lookback_start_month",
    "lookback_end_month",
    "source_trading_day_count",
    "source_expected_trading_days",
    "source_trading_day_coverage",
    "source_median_close",
    "source_median_dollar_volume",
    "source_last_price_date",
    "created_at_utc",
]


def _as_month_start(value: Any) -> date:
    return pd.Timestamp(value).to_period("M").to_timestamp().date()


def _add_months(month_start: date, months: int) -> date:
    period = pd.Period(month_start, freq="M") + months
    return period.to_timestamp().date()


def _load_yaml_mapping(path: str | Path) -> dict[str, Any]:
    config_path = Path(path)

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    data = yaml.safe_load(config_path.read_text(encoding="utf-8"))

    if not isinstance(data, dict):
        raise ValueError("liquid_universe.yml must contain a YAML mapping")

    return data

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


def filter_membership_by_month(
    membership: pd.DataFrame,
    *,
    start_membership_month: str | date | None = None,
    end_membership_month: str | date | None = None,
) -> pd.DataFrame:
    """Filter universe membership by inclusive membership-month range."""
    output = membership.copy()

    output["membership_month"] = pd.to_datetime(
        output["membership_month"],
        errors="raise",
    ).map(_as_month_start)

    start = parse_month_start(
        start_membership_month,
        field_name="start_membership_month",
    )
    end = parse_month_start(
        end_membership_month,
        field_name="end_membership_month",
    )

    if start is not None and end is not None and start > end:
        raise ValueError(
            "start_membership_month must be <= end_membership_month"
        )

    if start is not None:
        output = output[output["membership_month"] >= start].copy()

    if end is not None:
        output = output[output["membership_month"] <= end].copy()

    return output.reset_index(drop=True)


def membership_partition_dir(
    output_root: str | Path,
    *,
    universe_name: str,
    membership_month: date,
) -> Path:
    """Return local universe membership partition directory."""
    month = pd.Timestamp(membership_month)

    return (
        Path(output_root)
        / f"universe_name={universe_name}"
        / f"year={month.year}"
        / f"month={month.month:02d}"
    )

def load_universe_membership_config(
    config_path: str | Path,
) -> UniverseMembershipConfig:
    data = _load_yaml_mapping(config_path)

    config = data.get("liquid_universe")

    if not isinstance(config, dict):
        raise ValueError("liquid_universe config section is required")

    ranking = config.get("ranking", {})

    if not isinstance(ranking, dict):
        raise ValueError("liquid_universe.ranking must be a mapping")

    universes_raw = config.get("universes", [])

    if not isinstance(universes_raw, list) or not universes_raw:
        raise ValueError("liquid_universe.universes must be a non-empty list")

    universes: list[UniverseSpec] = []

    for item in universes_raw:
        if not isinstance(item, dict):
            raise ValueError("Each universe config entry must be a mapping")

        name = str(item.get("name", "")).strip()
        size = int(item.get("size", 0))

        if not name:
            raise ValueError("Universe name must not be empty")

        if size <= 0:
            raise ValueError(f"Universe size must be positive for {name}")

        universes.append(UniverseSpec(name=name, size=size))

    lookback_months = int(ranking.get("lookback_months", 3))

    if lookback_months <= 0:
        raise ValueError("ranking.lookback_months must be positive")

    score_aggregation = str(
        ranking.get("score_aggregation", "median")
    ).strip().lower()

    if score_aggregation not in {"median", "mean"}:
        raise ValueError("ranking.score_aggregation must be median or mean")

    return UniverseMembershipConfig(
        liquidity_monthly_output_root=Path(
            config["liquidity_monthly_output_root"]
        ),
        universe_membership_output_root=Path(
            config["universe_membership_output_root"]
        ),
        liquidity_score_method=str(
            ranking.get("liquidity_score_method", "median_dollar_volume")
        ).strip(),
        score_aggregation=score_aggregation,
        lookback_months=lookback_months,
        universes=tuple(universes),
    )


def read_liquidity_metrics(
    root: str | Path,
) -> pd.DataFrame:
    path = Path(root)
    files = sorted(path.rglob("*.parquet"))

    if not files:
        raise FileNotFoundError(
            f"No liquidity metric parquet files found under {path}"
        )

    frame = pd.concat(
        [pd.read_parquet(file_path) for file_path in files],
        ignore_index=True,
    )

    missing = sorted(REQUIRED_LIQUIDITY_COLUMNS - set(frame.columns))

    if missing:
        raise ValueError(f"Liquidity metrics missing columns: {missing}")

    return normalize_liquidity_metrics(frame)


def normalize_liquidity_metrics(
    metrics: pd.DataFrame,
) -> pd.DataFrame:
    missing = sorted(REQUIRED_LIQUIDITY_COLUMNS - set(metrics.columns))

    if missing:
        raise ValueError(f"Liquidity metrics missing columns: {missing}")

    output = metrics.copy()

    output["metric_month"] = pd.to_datetime(
        output["metric_month"],
        errors="coerce",
    )

    if output["metric_month"].isna().any():
        raise ValueError("Liquidity metrics contain invalid metric_month")

    output["metric_month"] = output["metric_month"].map(_as_month_start)

    for column in ["security_id", "ticker"]:
        output[column] = output[column].astype(str).str.strip()

    output["ticker"] = output["ticker"].str.upper()

    output = output[
        output["security_id"].ne("")
        & output["ticker"].ne("")
    ].copy()

    for column in [
        "trading_day_count",
        "expected_trading_days",
        "trading_day_coverage",
        "median_close",
        "median_dollar_volume",
    ]:
        output[column] = pd.to_numeric(output[column], errors="coerce")

    output["last_price_date"] = pd.to_datetime(
        output["last_price_date"],
        errors="coerce",
    ).dt.date

    output["is_complete_month"] = output["is_complete_month"].astype(bool)
    output["passes_liquidity_filters"] = output[
        "passes_liquidity_filters"
    ].astype(bool)

    duplicates = output.duplicated(
        ["metric_month", "security_id"],
        keep=False,
    )

    if duplicates.any():
        examples = (
            output.loc[
                duplicates,
                ["metric_month", "security_id", "ticker"],
            ]
            .head(20)
            .to_dict("records")
        )
        raise ValueError(
            "Liquidity metrics contain duplicate "
            f"metric_month/security_id rows: {examples}"
        )

    return output.sort_values(
        ["metric_month", "ticker", "security_id"]
    ).reset_index(drop=True)


def _aggregate_score(
    values: pd.Series,
    method: str,
) -> float:
    numeric = pd.to_numeric(values, errors="coerce").dropna()

    if numeric.empty:
        return float("nan")

    if method == "median":
        return float(numeric.median())

    if method == "mean":
        return float(numeric.mean())

    raise ValueError(f"Unsupported score aggregation: {method}")


def _build_source_month_candidates(
    eligible: pd.DataFrame,
    *,
    source_metric_month: date,
    lookback_months: int,
    score_column: str,
    score_aggregation: str,
) -> pd.DataFrame:
    all_months = sorted(eligible["metric_month"].unique())

    if source_metric_month not in all_months:
        raise ValueError(
            f"source_metric_month not present: {source_metric_month}"
        )

    source_index = all_months.index(source_metric_month)
    lookback_values = all_months[
        max(0, source_index - lookback_months + 1) : source_index + 1
    ]

    lookback = eligible[
        eligible["metric_month"].isin(lookback_values)
    ].copy()

    current = eligible[
        eligible["metric_month"] == source_metric_month
    ].copy()

    # Require the security to pass filters in the source metric month.
    # This prevents stale names from entering next month's universe merely
    # because they were liquid in earlier lookback months.
    current_keys = set(current["security_id"])

    lookback = lookback[lookback["security_id"].isin(current_keys)].copy()

    if lookback.empty:
        return pd.DataFrame()

    grouped = (
        lookback.groupby("security_id")
        .agg(
            ticker=("ticker", "last"),
            liquidity_score=(
                score_column,
                lambda value: _aggregate_score(
                    value,
                    score_aggregation,
                ),
            ),
            score_observation_count=(score_column, "count"),
            lookback_start_month=("metric_month", "min"),
            lookback_end_month=("metric_month", "max"),
        )
        .reset_index()
    )

    current_columns = [
        "security_id",
        "trading_day_count",
        "expected_trading_days",
        "trading_day_coverage",
        "median_close",
        "median_dollar_volume",
        "last_price_date",
    ]

    current_for_join = current[current_columns].rename(
        columns={
            "trading_day_count": "source_trading_day_count",
            "expected_trading_days": "source_expected_trading_days",
            "trading_day_coverage": "source_trading_day_coverage",
            "median_close": "source_median_close",
            "median_dollar_volume": "source_median_dollar_volume",
            "last_price_date": "source_last_price_date",
        }
    )

    output = grouped.merge(
        current_for_join,
        on="security_id",
        how="inner",
    )

    output = output[
        output["liquidity_score"].notna()
        & output["liquidity_score"].gt(0)
    ].copy()

    return output.sort_values(
        ["liquidity_score", "security_id"],
        ascending=[False, True],
    ).reset_index(drop=True)


def build_universe_membership(
    liquidity_metrics: pd.DataFrame,
    *,
    config: UniverseMembershipConfig,
) -> pd.DataFrame:
    metrics = normalize_liquidity_metrics(liquidity_metrics)

    score_column = config.liquidity_score_method

    if score_column not in metrics.columns:
        raise ValueError(
            f"Liquidity score column not found in metrics: {score_column}"
        )

    eligible = metrics[
        metrics["is_complete_month"]
        & metrics["passes_liquidity_filters"]
    ].copy()

    if eligible.empty:
        raise ValueError("No eligible liquidity metrics found")

    source_months = sorted(eligible["metric_month"].unique())
    created_at = datetime.now(UTC).isoformat()

    membership_rows: list[pd.DataFrame] = []

    for source_metric_month in source_months:
        ranked_candidates = _build_source_month_candidates(
            eligible,
            source_metric_month=source_metric_month,
            lookback_months=config.lookback_months,
            score_column=score_column,
            score_aggregation=config.score_aggregation,
        )

        if ranked_candidates.empty:
            continue

        membership_month = _add_months(source_metric_month, 1)
        effective_start_date = membership_month
        effective_end_date = _add_months(membership_month, 1)

        for universe in config.universes:
            selected = ranked_candidates.head(universe.size).copy()

            if selected.empty:
                continue

            selected["rank"] = range(1, len(selected) + 1)
            selected["universe_name"] = universe.name
            selected["universe_size"] = universe.size
            selected["membership_month"] = membership_month
            selected["effective_start_date"] = effective_start_date
            selected["effective_end_date"] = effective_end_date
            selected["score_metric_name"] = score_column
            selected["score_aggregation"] = config.score_aggregation
            selected["source_metric_month"] = source_metric_month
            selected["lookback_months"] = config.lookback_months
            selected["created_at_utc"] = created_at

            membership_rows.append(selected)

    if not membership_rows:
        raise ValueError("No universe membership rows were generated")

    membership = pd.concat(membership_rows, ignore_index=True)

    membership = membership.loc[:, UNIVERSE_MEMBERSHIP_COLUMNS]

    duplicates = membership.duplicated(
        ["universe_name", "membership_month", "security_id"],
        keep=False,
    )

    if duplicates.any():
        examples = (
            membership.loc[
                duplicates,
                [
                    "universe_name",
                    "membership_month",
                    "security_id",
                    "ticker",
                ],
            ]
            .head(20)
            .to_dict("records")
        )
        raise ValueError(f"Duplicate universe membership rows: {examples}")

    return membership.sort_values(
        ["universe_name", "membership_month", "rank"]
    ).reset_index(drop=True)

def write_universe_membership(
    membership: pd.DataFrame,
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

    if membership.empty:
        raise ValueError("Cannot write empty universe membership")

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

    working = membership.copy()
    membership_month = pd.to_datetime(
        working["membership_month"],
        errors="raise",
    )
    working["membership_month"] = membership_month.map(_as_month_start)
    working["_year"] = membership_month.dt.year
    working["_month"] = membership_month.dt.month

    for (universe_name, year, month_num), partition in working.groupby(
        ["universe_name", "_year", "_month"],
        sort=True,
    ):
        year_int = int(year)
        month_int = int(month_num)
        month_start = date(year_int, month_int, 1)

        partition_dir = membership_partition_dir(
            root,
            universe_name=str(universe_name),
            membership_month=month_start,
        )

        if partition_dir.exists():
            if missing_only:
                continue

            if replace_existing_partitions:
                shutil.rmtree(partition_dir)

            elif not overwrite:
                raise FileExistsError(
                    f"Universe membership partition already exists: "
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


def summarize_universe_membership(
    membership: pd.DataFrame,
) -> dict[str, Any]:
    if membership.empty:
        return {
            "rows": 0,
            "universes": 0,
            "months": 0,
        }

    return {
        "rows": len(membership),
        "universes": int(membership["universe_name"].nunique()),
        "months": int(membership["membership_month"].nunique()),
        "min_membership_month": str(min(membership["membership_month"])),
        "max_membership_month": str(max(membership["membership_month"])),
        "min_source_metric_month": str(min(membership["source_metric_month"])),
        "max_source_metric_month": str(max(membership["source_metric_month"])),
    }