from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import json
import uuid

import pandas as pd
import yaml


CONFIG_PATH = Path("configs/legacy/universe.yml")
ODS_ROOT = Path("data/ods/source=tiingo/dataset=equity_price_daily")
DWD_ROOT = Path("data/dwd/equity_price_daily")


def load_config() -> dict:
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def read_ods_symbol(symbol: str) -> dict:
    path = ODS_ROOT / f"symbol={symbol}" / f"{symbol.lower()}_prices.json"

    if not path.exists():
        raise FileNotFoundError(f"ODS file not found: {path}")

    return json.loads(path.read_text(encoding="utf-8"))


def normalize_tiingo_price_data(
    symbol: str,
    payload: dict,
    load_id: str,
) -> pd.DataFrame:
    records = payload.get("records", [])

    if not records:
        raise ValueError(f"No records found in ODS payload for {symbol}")

    df = pd.DataFrame(records)

    required_columns = {
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "adjOpen",
        "adjHigh",
        "adjLow",
        "adjClose",
        "adjVolume",
        "divCash",
        "splitFactor",
    }

    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"Missing Tiingo columns for {symbol}: {missing}")

    out = pd.DataFrame(
        {
            "security_id": f"US_{symbol}",
            "ticker": symbol,
            "date": pd.to_datetime(df["date"], utc=True).dt.date,
            "open": pd.to_numeric(df["open"], errors="coerce"),
            "high": pd.to_numeric(df["high"], errors="coerce"),
            "low": pd.to_numeric(df["low"], errors="coerce"),
            "close": pd.to_numeric(df["close"], errors="coerce"),
            "volume": pd.to_numeric(df["volume"], errors="coerce"),
            "adj_open": pd.to_numeric(df["adjOpen"], errors="coerce"),
            "adj_high": pd.to_numeric(df["adjHigh"], errors="coerce"),
            "adj_low": pd.to_numeric(df["adjLow"], errors="coerce"),
            "adj_close": pd.to_numeric(df["adjClose"], errors="coerce"),
            "adj_volume": pd.to_numeric(df["adjVolume"], errors="coerce"),
            "div_cash": pd.to_numeric(df["divCash"], errors="coerce"),
            "split_factor": pd.to_numeric(df["splitFactor"], errors="coerce"),
            "source": "tiingo",
            "load_id": load_id,
            "loaded_at": datetime.now(timezone.utc).isoformat(),
        }
    )

    out = out.dropna(
        subset=[
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "adj_close",
        ]
    )

    out = out.sort_values(["security_id", "date"]).reset_index(drop=True)

    return out


def validate_dwd_prices(df: pd.DataFrame) -> None:
    required_cols = {
        "security_id",
        "ticker",
        "date",
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

    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing DWD columns: {missing}")

    duplicates = df.duplicated(["security_id", "date"]).sum()
    if duplicates > 0:
        raise ValueError(f"Found duplicate security_id/date rows: {duplicates}")

    if not (df["close"] > 0).all():
        raise ValueError("Found non-positive close prices")

    if not (df["adj_close"] > 0).all():
        raise ValueError("Found non-positive adjusted close prices")

    if not (df["high"] >= df["low"]).all():
        raise ValueError("Found high < low")

    if not (df["volume"] >= 0).all():
        raise ValueError("Found negative volume")

    if not (df["split_factor"] > 0).all():
        raise ValueError("Found non-positive split factor")


def write_dwd_parquet(df: pd.DataFrame) -> None:
    df = df.copy()
    df["year"] = pd.to_datetime(df["date"]).dt.year
    df["month"] = pd.to_datetime(df["date"]).dt.month

    for (year, month), part in df.groupby(["year", "month"]):
        output_dir = DWD_ROOT / f"year={year}" / f"month={month:02d}"
        output_dir.mkdir(parents=True, exist_ok=True)

        output_path = output_dir / "part-000.parquet"

        part = part.drop(columns=["year", "month"])
        part.to_parquet(output_path, index=False)

        print(f"Wrote {len(part)} rows to {output_path}")

def transform_tiingo_prices_to_dwd(
    symbols: list[str],
    load_id: str,
) -> dict:
    frames = []

    for symbol in symbols:
        print(f"Transforming {symbol}...")
        payload = read_ods_symbol(symbol)
        normalized = normalize_tiingo_price_data(symbol, payload, load_id)
        frames.append(normalized)

    dwd = pd.concat(frames, ignore_index=True)
    validate_dwd_prices(dwd)
    write_dwd_parquet(dwd)

    return {
        "records_written": len(dwd),
        "symbols_count": dwd["ticker"].nunique(),
        "min_date": str(dwd["date"].min()),
        "max_date": str(dwd["date"].max()),
    }

def main() -> None:
    config = load_config()
    symbols = config["symbols"]
    load_id = str(uuid.uuid4())

    result = transform_tiingo_prices_to_dwd(symbols=symbols, load_id=load_id)

    print(f"Completed Tiingo DWD transform: {result}")


if __name__ == "__main__":
    main()