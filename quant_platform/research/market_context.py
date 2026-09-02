from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from quant_platform.research.config import MarketContextConfig

REQUIRED_DIM_SECURITY_COLUMNS = {
    "ticker",
    "security_id",
    "exchange",
    "asset_type",
    "price_currency",
    "start_date",
    "end_date",
    "is_active",
}


OUTPUT_COLUMNS = [
    "context_set",
    "context_group",
    "ticker",
    "security_id",
    "source_ticker",
    "exchange",
    "asset_type",
    "price_currency",
    "start_date",
    "end_date",
    "is_active",
    "is_required",
    "is_primary_benchmark",
    "loaded_at_utc",
]


def load_dim_security_for_market_context(path: str | Path) -> pd.DataFrame:
    dim_path = Path(path)

    if not dim_path.exists():
        raise FileNotFoundError(f"dim_security not found: {dim_path}")

    frame = pd.read_parquet(dim_path)

    missing = sorted(REQUIRED_DIM_SECURITY_COLUMNS - set(frame.columns))

    if missing:
        raise ValueError(f"dim_security missing columns: {missing}")

    output = frame.copy()
    output["ticker"] = output["ticker"].astype(str).str.upper().str.strip()
    output["security_id"] = output["security_id"].astype(str).str.strip()

    if "source_ticker" not in output.columns:
        output["source_ticker"] = output["ticker"]

    output["start_date"] = pd.to_datetime(
        output["start_date"],
        errors="coerce",
    ).dt.date
    output["end_date"] = pd.to_datetime(
        output["end_date"],
        errors="coerce",
    ).dt.date

    return output


def build_dim_market_context_symbol(
    *,
    dim_security: pd.DataFrame,
    config: MarketContextConfig,
) -> pd.DataFrame:
    specs = pd.DataFrame(
        [
            {
                "context_set": config.context_set,
                "context_group": spec.context_group,
                "ticker": spec.ticker,
                "is_required": spec.is_required,
                "is_primary_benchmark": spec.is_primary_benchmark,
            }
            for spec in config.symbols
        ]
    )

    security = dim_security.copy()
    security["ticker"] = security["ticker"].astype(str).str.upper().str.strip()

    security_columns = [
        "ticker",
        "security_id",
        "source_ticker",
        "exchange",
        "asset_type",
        "price_currency",
        "start_date",
        "end_date",
        "is_active",
    ]

    merged = specs.merge(
        security[security_columns].drop_duplicates("ticker"),
        on="ticker",
        how="left",
        indicator=True,
    )

    missing_required = merged[
        merged["is_required"] & (merged["_merge"] != "both")
    ]

    if not missing_required.empty:
        tickers = missing_required["ticker"].tolist()
        raise ValueError(
            f"Required market context tickers missing from dim_security: {tickers}"
        )

    resolved = merged[merged["_merge"] == "both"].copy()
    resolved = resolved.drop(columns=["_merge"])

    non_etf = resolved[
        resolved["asset_type"].astype(str).str.upper().ne("ETF")
    ]

    if not non_etf.empty:
        examples = non_etf[
            ["ticker", "security_id", "asset_type"]
        ].to_dict("records")
        raise ValueError(
            f"Market context symbols must be ETFs. Examples: {examples}"
        )

    duplicate_tickers = resolved[
        resolved["ticker"].duplicated(keep=False)
    ]

    if not duplicate_tickers.empty:
        examples = duplicate_tickers[
            ["ticker", "security_id", "context_group"]
        ].to_dict("records")
        raise ValueError(
            f"Duplicate market context tickers resolved: {examples}"
        )

    resolved["loaded_at_utc"] = datetime.now(UTC).isoformat()

    output = resolved.loc[:, OUTPUT_COLUMNS].sort_values(
        ["context_group", "ticker"]
    )

    return output.reset_index(drop=True)


def write_dim_market_context_symbol(
    frame: pd.DataFrame,
    output_path: str | Path,
) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)
    return path