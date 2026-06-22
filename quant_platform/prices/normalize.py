from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

import pandas as pd

from quant_platform.paths.price_paths import normalize_ticker
from quant_platform.prices.schema import (
    DWD_PRICE_COLUMNS,
    empty_dwd_price_frame,
)


def _required_text(value: Any, field_name: str) -> str:
    """Normalize a required metadata string."""
    if value is None:
        raise ValueError(f"{field_name} must not be empty")

    text = str(value).strip()

    if not text or text.lower() in {"nan", "<na>", "none"}:
        raise ValueError(f"{field_name} must not be empty")

    return text


def _normalize_loaded_at(value: str | datetime) -> str:
    """Normalize a timestamp to a UTC ISO-8601 string."""
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid loaded_at value: {value!r}") from exc

    if pd.isna(timestamp):
        raise ValueError("loaded_at must not be empty")

    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")

    return timestamp.isoformat()


def extract_tiingo_price_rows(
    payload: Any,
) -> list[dict[str, Any]]:
    """
    Extract raw Tiingo price rows from supported ODS payload shapes.

    Supported shapes:

    Formal bootstrap and windowed ODS:
        [{...}, {...}]

    Legacy demo ODS:
        {"records": [{...}, {...}]}
    """
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, Mapping):
        if "records" not in payload:
            raise ValueError(
                "Tiingo mapping payload must contain a 'records' field"
            )

        rows = payload["records"]
    else:
        raise ValueError(
            "Tiingo price payload must be a list or mapping"
        )

    if not isinstance(rows, list):
        raise ValueError("Tiingo price rows must be a list")

    if not all(isinstance(row, Mapping) for row in rows):
        raise ValueError(
            "Tiingo price rows must contain mappings only"
        )

    return [dict(row) for row in rows]


def _numeric_series(
    raw_df: pd.DataFrame,
    aliases: Sequence[str],
    default: Any,
) -> pd.Series:
    """Return the first available numeric source column."""
    for alias in aliases:
        if alias in raw_df.columns:
            return pd.to_numeric(
                raw_df[alias],
                errors="coerce",
            )

    default_series = pd.Series(
        default,
        index=raw_df.index,
        dtype="object",
    )

    return pd.to_numeric(
        default_series,
        errors="coerce",
    )


def _nullable_integer_series(
    values: pd.Series,
    field_name: str,
) -> pd.Series:
    """Convert numeric values to nullable integers without silent rounding."""
    numeric = pd.to_numeric(values, errors="coerce")
    non_null = numeric.dropna()

    non_integral = non_null.mod(1).ne(0)

    if bool(non_integral.any()):
        examples = non_null[non_integral].head(5).tolist()

        raise ValueError(
            f"{field_name} contains non-integral values: {examples}"
        )

    return numeric.astype("Int64")


def normalize_tiingo_price_rows(
    raw_rows: Sequence[Mapping[str, Any]],
    *,
    ticker: str,
    security_id: str,
    load_id: str,
    loaded_at: str | datetime,
    source: str = "tiingo",
) -> pd.DataFrame:
    """
    Normalize Tiingo EOD rows into the canonical DWD price schema.

    Empty source rows return an empty canonical DataFrame. The caller decides
    whether an empty response is acceptable for its workflow.
    """
    ticker_norm = normalize_ticker(ticker)
    security_id_norm = _required_text(
        security_id,
        "security_id",
    )
    load_id_norm = _required_text(load_id, "load_id")
    source_norm = _required_text(source, "source")
    loaded_at_norm = _normalize_loaded_at(loaded_at)

    normalized_rows = [dict(row) for row in raw_rows]

    if not normalized_rows:
        return empty_dwd_price_frame()

    if not all(isinstance(row, dict) for row in normalized_rows):
        raise ValueError(
            "raw_rows must contain mapping-like rows only"
        )

    raw_df = pd.DataFrame(normalized_rows)

    if "date" not in raw_df.columns:
        raise ValueError(
            f"Raw Tiingo rows for {ticker_norm} do not contain date"
        )

    # Keep the source index so scalar assignments populate every output row.
    output = pd.DataFrame(index=raw_df.index)

    output["security_id"] = security_id_norm
    output["ticker"] = ticker_norm

    output["date"] = pd.to_datetime(
        raw_df["date"],
        errors="coerce",
        utc=True,
    ).dt.date

    output["open"] = _numeric_series(
        raw_df,
        aliases=("open",),
        default=pd.NA,
    )
    output["high"] = _numeric_series(
        raw_df,
        aliases=("high",),
        default=pd.NA,
    )
    output["low"] = _numeric_series(
        raw_df,
        aliases=("low",),
        default=pd.NA,
    )
    output["close"] = _numeric_series(
        raw_df,
        aliases=("close",),
        default=pd.NA,
    )

    output["volume"] = _nullable_integer_series(
        _numeric_series(
            raw_df,
            aliases=("volume",),
            default=0,
        ),
        field_name="volume",
    )

    output["adj_open"] = _numeric_series(
        raw_df,
        aliases=("adjOpen", "adj_open"),
        default=pd.NA,
    ).fillna(output["open"])

    output["adj_high"] = _numeric_series(
        raw_df,
        aliases=("adjHigh", "adj_high"),
        default=pd.NA,
    ).fillna(output["high"])

    output["adj_low"] = _numeric_series(
        raw_df,
        aliases=("adjLow", "adj_low"),
        default=pd.NA,
    ).fillna(output["low"])

    output["adj_close"] = _numeric_series(
        raw_df,
        aliases=("adjClose", "adj_close"),
        default=pd.NA,
    ).fillna(output["close"])

    output["adj_volume"] = _nullable_integer_series(
        _numeric_series(
            raw_df,
            aliases=("adjVolume", "adj_volume"),
            default=pd.NA,
        ).fillna(output["volume"]),
        field_name="adj_volume",
    )

    output["div_cash"] = _numeric_series(
        raw_df,
        aliases=("divCash", "div_cash"),
        default=0.0,
    ).fillna(0.0)

    output["split_factor"] = _numeric_series(
        raw_df,
        aliases=("splitFactor", "split_factor"),
        default=1.0,
    ).fillna(1.0)

    output["source"] = source_norm
    output["load_id"] = load_id_norm
    output["loaded_at"] = loaded_at_norm

    output = output.loc[:, list(DWD_PRICE_COLUMNS)]

    return output.sort_values(
        ["security_id", "date"],
        na_position="last",
    ).reset_index(drop=True)


def normalize_tiingo_price_payload(
    payload: Any,
    *,
    ticker: str,
    security_id: str,
    load_id: str,
    loaded_at: str | datetime,
    source: str = "tiingo",
) -> pd.DataFrame:
    """Extract and normalize either formal-list or legacy-records payloads."""
    rows = extract_tiingo_price_rows(payload)

    return normalize_tiingo_price_rows(
        raw_rows=rows,
        ticker=ticker,
        security_id=security_id,
        load_id=load_id,
        loaded_at=loaded_at,
        source=source,
    )