from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from quant_platform.research.config import ResearchPanelConfig
from quant_platform.research.io import (
    filter_frame_by_month,
    read_parquet_dataset_by_month,
    write_monthly_partitions,
)

RAW_PRICE_COLUMNS = {
    "open",
    "high",
    "low",
    "close",
    "volume",
    "adj_open",
    "adj_high",
    "adj_low",
    "adj_close",
    "adj_volume",
    "div_cash",
    "split_factor",
    "source",
    "load_id",
    "loaded_at",
}

MARKET_CONTEXT_EXCLUDED_COLUMNS = {
    "date",
    "context_set",
    "context_group",
    "security_id",
    "ticker",
    "created_at_utc",
    *RAW_PRICE_COLUMNS,
}


def _factor_root(
    *,
    feature_root: str | Path,
    factor_set: str,
) -> Path:
    root = Path(feature_root)
    factor_part = f"factor_set={factor_set}"

    if root.name == factor_part:
        return root

    return root / factor_part


def _label_root(
    *,
    label_root: str | Path,
    label_set: str,
) -> Path:
    root = Path(label_root)
    label_part = f"label_set={label_set}"

    if root.name == label_part:
        return root

    return root / label_part


def _context_root(
    *,
    market_context_feature_root: str | Path,
    context_set: str,
) -> Path:
    root = Path(market_context_feature_root)
    context_part = f"context_set={context_set}"

    if root.name == context_part:
        return root

    return root / context_part


def _panel_root(
    *,
    panel_output_root: str | Path,
    universe_name: str,
    factor_set: str,
) -> Path:
    return (
        Path(panel_output_root)
        / f"universe_name={universe_name}"
        / f"factor_set={factor_set}"
    )


def _month_start_series(series: pd.Series) -> pd.Series:
    return (
        pd.to_datetime(series, errors="coerce")
        .dt.to_period("M")
        .dt.to_timestamp()
        .dt.date
    )


def read_equity_features_for_panel_build(
    *,
    config: ResearchPanelConfig,
    start_month: str | date | None,
    end_month: str | date | None,
) -> pd.DataFrame:
    return read_parquet_dataset_by_month(
        _factor_root(
            feature_root=config.feature_output_root,
            factor_set=config.factor_set,
        ),
        date_column="date",
        start_month=start_month,
        end_month=end_month,
    )


def read_labels_for_panel_build(
    *,
    config: ResearchPanelConfig,
    start_month: str | date | None,
    end_month: str | date | None,
) -> pd.DataFrame:
    return read_parquet_dataset_by_month(
        _label_root(
            label_root=config.label_output_root,
            label_set=config.label_set,
        ),
        date_column="date",
        start_month=start_month,
        end_month=end_month,
    )


def read_market_context_features_for_panel_build(
    *,
    config: ResearchPanelConfig,
    context_set: str,
    start_month: str | date | None,
    end_month: str | date | None,
) -> pd.DataFrame:
    return read_parquet_dataset_by_month(
        _context_root(
            market_context_feature_root=config.market_context_feature_root,
            context_set=context_set,
        ),
        date_column="date",
        start_month=start_month,
        end_month=end_month,
    )


def read_universe_membership_for_panel_build(
    *,
    membership_root: str | Path,
    universe_name: str,
    start_month: str | date | None,
    end_month: str | date | None,
) -> pd.DataFrame:
    frame = read_parquet_dataset_by_month(
        membership_root,
        date_column="membership_month",
        start_month=start_month,
        end_month=end_month,
    )

    required = {
        "universe_name",
        "membership_month",
        "security_id",
        "ticker",
        "rank",
        "effective_start_date",
        "effective_end_date",
    }
    missing = sorted(required - set(frame.columns))

    if missing:
        raise ValueError(f"Universe membership missing columns: {missing}")

    output = frame[frame["universe_name"] == universe_name].copy()

    if output.empty:
        raise ValueError(f"No membership rows for {universe_name}")

    date_columns = [
        "membership_month",
        "effective_start_date",
        "effective_end_date",
        "source_metric_month",
        "lookback_start_month",
        "lookback_end_month",
    ]

    for column in date_columns:
        if column in output.columns:
            output[column] = pd.to_datetime(
                output[column],
                errors="coerce",
            ).dt.date

    output["security_id"] = output["security_id"].astype(str).str.strip()
    output["ticker"] = output["ticker"].astype(str).str.upper().str.strip()

    return output.reset_index(drop=True)


def market_context_feature_columns(
    market_context_features: pd.DataFrame,
) -> list[str]:
    return [
        column
        for column in market_context_features.columns
        if column not in MARKET_CONTEXT_EXCLUDED_COLUMNS
    ]


def build_market_context_wide_features(
    market_context_features: pd.DataFrame,
) -> pd.DataFrame:
    required = {
        "date",
        "context_set",
        "context_group",
        "security_id",
        "ticker",
    }
    missing = sorted(required - set(market_context_features.columns))

    if missing:
        raise ValueError(f"Market context features missing columns: {missing}")

    feature_columns = market_context_feature_columns(market_context_features)

    if not feature_columns:
        raise ValueError("No market context feature columns found")

    working = market_context_features.copy()
    working["date"] = pd.to_datetime(working["date"], errors="coerce").dt.date
    working["ticker"] = working["ticker"].astype(str).str.upper().str.strip()

    duplicate_count = int(working.duplicated(["date", "ticker"]).sum())

    if duplicate_count != 0:
        raise ValueError(
            f"Duplicate market context date/ticker rows: {duplicate_count}"
        )

    wide_parts: list[pd.DataFrame] = []

    for ticker, ticker_frame in working.groupby("ticker", sort=True):
        prefix = f"mkt_{ticker.lower()}_"

        selected = ticker_frame[["date", *feature_columns]].copy()
        selected = selected.rename(
            columns={
                column: f"{prefix}{column}"
                for column in feature_columns
            }
        )

        wide_parts.append(selected)

    if not wide_parts:
        raise ValueError("No market context rows found")

    output = wide_parts[0]

    for part in wide_parts[1:]:
        output = output.merge(
            part,
            on="date",
            how="outer",
            validate="one_to_one",
        )

    return output.sort_values("date").reset_index(drop=True)


def add_benchmark_relative_features(
    panel: pd.DataFrame,
    *,
    benchmark_ticker: str = "spy",
) -> pd.DataFrame:
    output = panel.copy()
    benchmark_prefix = f"mkt_{benchmark_ticker.lower()}_"

    for column in list(output.columns):
        if not column.startswith("ret_") or not column.endswith("d"):
            continue

        benchmark_column = f"{benchmark_prefix}{column}"

        if benchmark_column not in output.columns:
            continue

        output[f"excess_{column}_vs_{benchmark_ticker.lower()}"] = (
            output[column] - output[benchmark_column]
        )

    return output


def build_equity_research_panel(
    *,
    features: pd.DataFrame,
    labels: pd.DataFrame,
    market_context_features: pd.DataFrame,
    membership: pd.DataFrame,
    universe_name: str,
    factor_set: str,
    label_set: str,
) -> pd.DataFrame:
    feature_required = {"date", "security_id", "ticker", "factor_set"}
    label_required = {"date", "security_id", "label_set"}
    membership_required = {
        "universe_name",
        "membership_month",
        "security_id",
        "rank",
        "effective_start_date",
        "effective_end_date",
    }

    for name, frame, required in [
        ("features", features, feature_required),
        ("labels", labels, label_required),
        ("membership", membership, membership_required),
    ]:
        missing = sorted(required - set(frame.columns))

        if missing:
            raise ValueError(f"{name} missing columns: {missing}")

    feature_frame = features.copy()
    label_frame = labels.copy()
    membership_frame = membership.copy()

    feature_frame["date"] = pd.to_datetime(
        feature_frame["date"],
        errors="coerce",
    ).dt.date
    label_frame["date"] = pd.to_datetime(
        label_frame["date"],
        errors="coerce",
    ).dt.date

    feature_frame["security_id"] = (
        feature_frame["security_id"].astype(str).str.strip()
    )
    label_frame["security_id"] = (
        label_frame["security_id"].astype(str).str.strip()
    )

    feature_frame = feature_frame[feature_frame["factor_set"] == factor_set].copy()
    label_frame = label_frame[label_frame["label_set"] == label_set].copy()
    membership_frame = membership_frame[
        membership_frame["universe_name"] == universe_name
    ].copy()

    if feature_frame.empty:
        raise ValueError(f"No feature rows for factor_set={factor_set}")

    if label_frame.empty:
        raise ValueError(f"No label rows for label_set={label_set}")

    if membership_frame.empty:
        raise ValueError(f"No membership rows for {universe_name}")

    feature_duplicate_count = int(
        feature_frame.duplicated(["date", "security_id", "factor_set"]).sum()
    )
    label_duplicate_count = int(
        label_frame.duplicated(["date", "security_id", "label_set"]).sum()
    )
    membership_duplicate_count = int(
        membership_frame.duplicated(
            ["universe_name", "membership_month", "security_id"]
        ).sum()
    )

    if feature_duplicate_count != 0:
        raise ValueError(f"Duplicate feature keys: {feature_duplicate_count}")

    if label_duplicate_count != 0:
        raise ValueError(f"Duplicate label keys: {label_duplicate_count}")

    if membership_duplicate_count != 0:
        raise ValueError(
            f"Duplicate membership keys: {membership_duplicate_count}"
        )

    feature_frame["_membership_month"] = _month_start_series(feature_frame["date"])

    joined = feature_frame.merge(
        membership_frame,
        left_on=["security_id", "_membership_month"],
        right_on=["security_id", "membership_month"],
        how="inner",
        suffixes=("", "_membership"),
        validate="many_to_one",
    )

    if joined.empty:
        raise ValueError("Feature/membership join produced zero rows")

    joined = joined[
        (joined["date"] >= joined["effective_start_date"])
        & (joined["date"] < joined["effective_end_date"])
    ].copy()

    if joined.empty:
        raise ValueError("Effective-date membership filter produced zero rows")

    label_drop_columns = [
        column
        for column in ["ticker"]
        if column in label_frame.columns
    ]

    joined = joined.merge(
        label_frame.drop(columns=label_drop_columns),
        on=["date", "security_id"],
        how="left",
        validate="one_to_one",
    )

    market_context_wide = build_market_context_wide_features(
        market_context_features
    )

    joined = joined.merge(
        market_context_wide,
        on="date",
        how="left",
        validate="many_to_one",
    )

    joined = add_benchmark_relative_features(
        joined,
        benchmark_ticker="spy",
    )

    joined = joined.drop(columns=["_membership_month"])

    membership_columns = [
        column
        for column in [
            "universe_name",
            "membership_month",
            "effective_start_date",
            "effective_end_date",
            "rank",
            "source_metric_month",
            "lookback_start_month",
            "lookback_end_month",
            "liquidity_score",
            "source_median_dollar_volume",
            "score_observation_count",
        ]
        if column in joined.columns
    ]

    leading_columns = [
        "date",
        *membership_columns,
        "security_id",
        "ticker",
        "factor_set",
        "label_set",
    ]

    existing_leading = [
        column for column in leading_columns if column in joined.columns
    ]

    ordered_columns = [
        *existing_leading,
        *[column for column in joined.columns if column not in existing_leading],
    ]

    return joined.loc[:, ordered_columns].sort_values(
        ["date", "rank", "ticker", "security_id"]
    ).reset_index(drop=True)


def write_panel_partitions(
    panel: pd.DataFrame,
    *,
    config: ResearchPanelConfig,
    universe_name: str,
    start_month: str | date | None = None,
    end_month: str | date | None = None,
    overwrite: bool = False,
    replace_existing_partitions: bool = False,
) -> tuple[list[Path], pd.DataFrame]:
    selected = filter_frame_by_month(
        panel,
        date_column="date",
        start_month=start_month,
        end_month=end_month,
    )

    root = _panel_root(
        panel_output_root=config.panel_output_root,
        universe_name=universe_name,
        factor_set=config.factor_set,
    )

    written = write_monthly_partitions(
        selected,
        output_root=root,
        date_column="date",
        overwrite=overwrite,
        replace_existing_partitions=replace_existing_partitions,
    )

    return written, selected


def summarize_panel(
    panel: pd.DataFrame,
) -> dict[str, Any]:
    return {
        "row_count": len(panel),
        "date_count": panel["date"].nunique(),
        "security_id_count": panel["security_id"].nunique(),
        "ticker_count": panel["ticker"].nunique(),
        "min_date": panel["date"].min(),
        "max_date": panel["date"].max(),
        "duplicate_key_count": int(
            panel.duplicated(
                ["date", "universe_name", "security_id", "factor_set"]
            ).sum()
        ),
        "market_context_column_count": len(
            [column for column in panel.columns if column.startswith("mkt_")]
        ),
        "label_column_count": len(
            [column for column in panel.columns if column.startswith("label_")]
        ),
        "feature_column_count": len(
            [
                column
                for column in panel.columns
                if column.startswith(("ret_", "mom_", "rev_", "tech_", "cdl_"))
            ]
        ),
    }