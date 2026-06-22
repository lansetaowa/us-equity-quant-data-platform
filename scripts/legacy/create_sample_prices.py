from pathlib import Path
import uuid

import pandas as pd


OUTPUT_PATH = Path(
    "data/dwd/equity_price_daily/year=2025/month=01/equity_price_daily.parquet"
)


def build_sample_prices() -> pd.DataFrame:
    dates = pd.bdate_range("2025-01-02", periods=5)

    securities = [
        ("US000001", "AAPL", 190.0),
        ("US000002", "MSFT", 420.0),
        ("US000003", "NVDA", 130.0),
    ]

    rows = []
    load_id = str(uuid.uuid4())

    for security_id, ticker, base_price in securities:
        for i, date in enumerate(dates):
            close = base_price + i * 1.5
            rows.append(
                {
                    "security_id": security_id,
                    "ticker": ticker,
                    "date": date.date(),
                    "open": close - 0.5,
                    "high": close + 1.0,
                    "low": close - 1.2,
                    "close": close,
                    "volume": 1_000_000 + i * 10_000,
                    "source": "synthetic",
                    "load_id": load_id,
                }
            )

    df = pd.DataFrame(rows)
    return df


def main() -> None:
    df = build_sample_prices()

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUTPUT_PATH, index=False)

    print(f"Wrote {len(df)} rows to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()