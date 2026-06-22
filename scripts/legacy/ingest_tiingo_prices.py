from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import json
import os
# import time
import uuid

from dotenv import load_dotenv
import requests
import yaml


CONFIG_PATH = Path("configs/legacy/universe.yml")
ODS_ROOT = Path("data/ods/source=tiingo/dataset=equity_price_daily")


def load_config() -> dict:
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_tiingo_token() -> str:
    load_dotenv()
    token = os.environ.get("TIINGO_API_TOKEN")
    if not token:
        raise RuntimeError("TIINGO_API_TOKEN is missing. Check your .env file.")
    return token


def fetch_tiingo_eod_prices(
    symbol: str,
    start_date: str,
    end_date: str,
    token: str,
) -> list[dict]:
    url = f"https://api.tiingo.com/tiingo/daily/{symbol}/prices"

    params = {
        "startDate": start_date,
        "endDate": end_date,
        "format": "json",
        "token": token,
    }

    response = requests.get(url, params=params, timeout=60)

    if response.status_code == 404:
        raise ValueError(f"Tiingo returned 404 for symbol={symbol}")

    response.raise_for_status()

    data = response.json()

    if not isinstance(data, list):
        raise ValueError(f"Unexpected Tiingo response for {symbol}: {data}")

    if not data:
        raise ValueError(f"No Tiingo price data returned for {symbol}")

    return data


def write_ods_json(symbol: str, data: list[dict], run_id: str) -> Path:
    output_dir = ODS_ROOT / f"symbol={symbol}"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"{symbol.lower()}_prices.json"

    payload = {
        "source": "tiingo",
        "dataset": "equity_price_daily",
        "symbol": symbol,
        "run_id": run_id,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "records": data,
    }

    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    return output_path

def ingest_tiingo_prices(
    symbols: list[str],
    start_date: str,
    end_date: str,
    run_id: str,
) -> dict:
    token = get_tiingo_token()

    total_records = 0
    output_files = []

    for symbol in symbols:
        print(f"Fetching Tiingo EOD prices for {symbol}...")
        data = fetch_tiingo_eod_prices(symbol, start_date, end_date, token)
        output_path = write_ods_json(symbol, data, run_id)

        total_records += len(data)
        output_files.append(str(output_path))

        print(f"Wrote {len(data)} records to {output_path}")

    return {
        "source": "tiingo",
        "dataset": "equity_price_daily",
        "symbols_count": len(symbols),
        "records_written": total_records,
        "output_files": output_files,
    }

def main() -> None:
    config = load_config()
    symbols = config["symbols"]
    start_date = config.get("start_date", "2020-01-01")
    end_date = config.get("end_date", "2025-12-31")
    run_id = str(uuid.uuid4())

    result = ingest_tiingo_prices(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        run_id=run_id,
    )

    print(result)


if __name__ == "__main__":
    main()