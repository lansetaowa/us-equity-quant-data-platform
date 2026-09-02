from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from quant_platform.clients.tiingo import (
    TiingoClientConfig,
    fetch_daily_prices,
)
from quant_platform.prices.normalize import normalize_tiingo_price_rows
from quant_platform.prices.schema import DWD_PRICE_COLUMNS
from quant_platform.prices.transform import combine_dwd_price_frames
from quant_platform.research.config import MarketContextConfig
from quant_platform.storage.local_json import write_json_rows

MARKET_CONTEXT_PRICE_COLUMNS = [
    "context_set",
    "context_group",
    *DWD_PRICE_COLUMNS,
]


@dataclass(frozen=True)
class MarketContextPriceTask:
    context_set: str
    context_group: str
    ticker: str
    security_id: str
    request_start_date: date
    request_end_date: date
    local_path: Path


def build_market_context_raw_path(
    *,
    ods_root: str | Path,
    context_set: str,
    ticker: str,
    request_start_date: date,
    request_end_date: date,
) -> Path:
    return (
        Path(ods_root)
        / f"context_set={context_set}"
        / f"symbol={ticker}"
        / f"request_start={request_start_date.isoformat()}"
        / f"request_end={request_end_date.isoformat()}"
        / "prices.json"
    )


def context_dwd_root(
    *,
    dwd_root: str | Path,
    context_set: str,
) -> Path:
    root = Path(dwd_root)
    context_part = f"context_set={context_set}"

    if root.name == context_part:
        return root

    return root / context_part


def read_existing_market_context_prices(
    *,
    dwd_root: str | Path,
    context_set: str,
) -> pd.DataFrame:
    root = context_dwd_root(
        dwd_root=dwd_root,
        context_set=context_set,
    )

    if not root.exists():
        return pd.DataFrame(columns=MARKET_CONTEXT_PRICE_COLUMNS)

    files = sorted(root.rglob("*.parquet"))

    if not files:
        return pd.DataFrame(columns=MARKET_CONTEXT_PRICE_COLUMNS)

    return pd.concat(
        [pd.read_parquet(path) for path in files],
        ignore_index=True,
    )


def latest_market_context_dates(
    prices: pd.DataFrame,
) -> pd.DataFrame:
    if prices.empty:
        return pd.DataFrame(
            columns=["ticker", "security_id", "latest_dwd_date"]
        )

    required = {"ticker", "security_id", "date"}
    missing = required - set(prices.columns)

    if missing:
        raise ValueError(f"Market context prices missing columns: {missing}")

    output = prices.copy()
    output["ticker"] = output["ticker"].astype(str).str.upper().str.strip()
    output["security_id"] = output["security_id"].astype(str).str.strip()
    output["date"] = pd.to_datetime(output["date"], errors="coerce").dt.date

    return (
        output.groupby(["ticker", "security_id"], as_index=False)["date"]
        .max()
        .rename(columns={"date": "latest_dwd_date"})
    )


def build_market_context_price_tasks(
    *,
    symbols: pd.DataFrame,
    config: MarketContextConfig,
    request_end_date: date,
    existing_prices: pd.DataFrame | None = None,
) -> list[MarketContextPriceTask]:
    if request_end_date < config.price_start_date:
        raise ValueError("request_end_date is before price_start_date")

    required = {
        "context_set",
        "context_group",
        "ticker",
        "security_id",
        "is_active",
    }
    missing = required - set(symbols.columns)

    if missing:
        raise ValueError(f"Market context symbols missing columns: {missing}")

    existing = (
        pd.DataFrame(columns=["ticker", "security_id", "latest_dwd_date"])
        if existing_prices is None
        else latest_market_context_dates(existing_prices)
    )

    working = symbols.copy()
    working["ticker"] = working["ticker"].astype(str).str.upper().str.strip()
    working["security_id"] = working["security_id"].astype(str).str.strip()

    merged = working.merge(
        existing,
        on=["ticker", "security_id"],
        how="left",
    )

    tasks: list[MarketContextPriceTask] = []

    for row in merged.to_dict("records"):
        latest = row.get("latest_dwd_date")

        if pd.isna(latest):
            request_start = config.price_start_date
        else:
            request_start = pd.Timestamp(latest).date() + timedelta(days=1)

        if request_start > request_end_date:
            continue

        ticker = str(row["ticker"])
        local_path = build_market_context_raw_path(
            ods_root=config.price_ods_root,
            context_set=config.context_set,
            ticker=ticker,
            request_start_date=request_start,
            request_end_date=request_end_date,
        )

        tasks.append(
            MarketContextPriceTask(
                context_set=config.context_set,
                context_group=str(row["context_group"]),
                ticker=ticker,
                security_id=str(row["security_id"]),
                request_start_date=request_start,
                request_end_date=request_end_date,
                local_path=local_path,
            )
        )

    return tasks


def download_market_context_price_tasks(
    *,
    tasks: list[MarketContextPriceTask],
    client_config: TiingoClientConfig,
    load_id: str,
    loaded_at: datetime,
    source: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    result_rows: list[dict[str, Any]] = []
    frames: list[pd.DataFrame] = []

    for task in tasks:
        try:
            rows = fetch_daily_prices(
                ticker=task.ticker,
                start_date=task.request_start_date,
                end_date=task.request_end_date,
                config=client_config,
            )

            write_json_rows(
                task.local_path,
                rows,
            )

            normalized = normalize_tiingo_price_rows(
                raw_rows=rows,
                ticker=task.ticker,
                security_id=task.security_id,
                load_id=load_id,
                loaded_at=loaded_at,
                source=source,
            )

            if not normalized.empty:
                normalized["context_set"] = task.context_set
                normalized["context_group"] = task.context_group
                frames.append(normalized)

            result_rows.append(
                {
                    "context_set": task.context_set,
                    "context_group": task.context_group,
                    "ticker": task.ticker,
                    "security_id": task.security_id,
                    "request_start_date": task.request_start_date,
                    "request_end_date": task.request_end_date,
                    "status": "success" if rows else "empty",
                    "row_count": len(rows),
                    "local_path": task.local_path.as_posix(),
                    "error_message": None,
                }
            )

        except Exception as exc:
            result_rows.append(
                {
                    "context_set": task.context_set,
                    "context_group": task.context_group,
                    "ticker": task.ticker,
                    "security_id": task.security_id,
                    "request_start_date": task.request_start_date,
                    "request_end_date": task.request_end_date,
                    "status": "failed",
                    "row_count": 0,
                    "local_path": task.local_path.as_posix(),
                    "error_message": repr(exc)[:2000],
                }
            )

    results = pd.DataFrame(result_rows)

    if frames:
        new_prices = pd.concat(frames, ignore_index=True)
    else:
        new_prices = pd.DataFrame(columns=MARKET_CONTEXT_PRICE_COLUMNS)

    return results, new_prices


def combine_market_context_prices(
    *,
    existing_prices: pd.DataFrame,
    new_prices: pd.DataFrame,
    symbols: pd.DataFrame,
    context_set: str,
) -> pd.DataFrame:
    canonical_frames = []

    if not existing_prices.empty:
        canonical_frames.append(existing_prices.loc[:, list(DWD_PRICE_COLUMNS)])

    if not new_prices.empty:
        canonical_frames.append(new_prices.loc[:, list(DWD_PRICE_COLUMNS)])

    if not canonical_frames:
        return pd.DataFrame(columns=MARKET_CONTEXT_PRICE_COLUMNS)

    combined = combine_dwd_price_frames(canonical_frames)

    context_lookup = symbols[
        ["security_id", "ticker", "context_group"]
    ].drop_duplicates()

    output = combined.merge(
        context_lookup,
        on=["security_id", "ticker"],
        how="left",
    )

    if output["context_group"].isna().any():
        missing = (
            output.loc[
                output["context_group"].isna(),
                ["ticker", "security_id"],
            ]
            .drop_duplicates()
            .head(20)
            .to_dict("records")
        )
        raise ValueError(f"Missing market context groups: {missing}")

    output["context_set"] = context_set

    return output.loc[:, MARKET_CONTEXT_PRICE_COLUMNS].sort_values(
        ["ticker", "date"]
    ).reset_index(drop=True)


def price_months_in_frame(prices: pd.DataFrame) -> set[date]:
    if prices.empty:
        return set()

    parsed = pd.to_datetime(
        prices["date"],
        errors="coerce",
    )

    if parsed.isna().any():
        raise ValueError("Market context prices contain invalid dates")

    months = parsed.dt.to_period("M").dt.to_timestamp().dt.date

    return set(months.tolist())


def write_market_context_price_partitions(
    *,
    prices: pd.DataFrame,
    dwd_root: str | Path,
    context_set: str,
    replace_existing: bool = False,
    months_to_write: set[date] | None = None,
) -> list[Path]:
    if prices.empty:
        raise ValueError("Cannot write empty market context price frame")

    root = context_dwd_root(
        dwd_root=dwd_root,
        context_set=context_set,
    )

    if root.exists() and replace_existing:
        shutil.rmtree(root)

    root.mkdir(parents=True, exist_ok=True)

    working = prices.loc[:, MARKET_CONTEXT_PRICE_COLUMNS].copy()
    dates = pd.to_datetime(working["date"], errors="coerce")

    if dates.isna().any():
        raise ValueError("Market context prices contain invalid dates")

    working["_month_start"] = dates.dt.to_period("M").dt.to_timestamp().dt.date
    working["_year"] = dates.dt.year
    working["_month"] = dates.dt.month

    if months_to_write is not None:
        working = working[working["_month_start"].isin(months_to_write)].copy()

    if working.empty:
        return []

    written: list[Path] = []

    for (year, month), partition in working.groupby(
        ["_year", "_month"],
        sort=True,
    ):
        partition_dir = root / f"year={int(year)}" / f"month={int(month):02d}"

        # Exact-replace the affected partition. This avoids stale extra files if
        # a future writer emits more than one parquet file per month.
        if partition_dir.exists():
            shutil.rmtree(partition_dir)

        partition_dir.mkdir(parents=True, exist_ok=True)

        output_path = partition_dir / "part-000.parquet"

        write_df = (
            partition.drop(columns=["_month_start", "_year", "_month"])
            .loc[:, MARKET_CONTEXT_PRICE_COLUMNS]
            .sort_values(["ticker", "date"])
            .reset_index(drop=True)
        )

        write_df.to_parquet(output_path, index=False)
        written.append(output_path)

    return written