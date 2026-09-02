from __future__ import annotations

import talib

REQUIRED_FUNCTIONS = [
    "RSI",
    "MFI",
    "ATR",
    "MACD",
    "BBANDS",
    "TEMA",
    "ADX",
    "PLUS_DI",
    "MINUS_DI",
    "CMO",
    "ULTOSC",
    "BOP",
    "CDLENGULFING",
    "CDLHAMMER",
]


def main() -> None:
    functions = set(talib.get_functions())
    groups = talib.get_function_groups()

    missing = [
        name for name in REQUIRED_FUNCTIONS if name not in functions
    ]

    print("TA-Lib Python version:", talib.__version__)
    print("Function count:", len(functions))
    print(
        "Pattern Recognition count:",
        len(groups.get("Pattern Recognition", [])),
    )

    if missing:
        print("Missing functions:", missing)
        raise SystemExit("TA-Lib verification failed")

    print("TA-Lib verification passed.")


if __name__ == "__main__":
    main()