import pandas as pd

from scripts.audit_backfill_coverage import build_suspicious_pattern_report
from scripts.run_tiingo_backfill import select_tasks_to_process


def test_select_tasks_to_process_skips_success_and_selects_pending_failed() -> None:
    task_df = pd.DataFrame(
        {
            "ticker": ["AAPL", "MSFT", "NVDA", "AMZN"],
            "security_id": [
                "tiingo:AAPL",
                "tiingo:MSFT",
                "tiingo:NVDA",
                "tiingo:AMZN",
            ],
            "source": ["tiingo"] * 4,
            "dataset_name": ["equity_price_daily"] * 4,
            "requested_start_date": [pd.Timestamp("2019-01-01").date()] * 4,
            "requested_end_date": [pd.Timestamp("2026-06-02").date()] * 4,
            "status": ["pending"] * 4,
            "_task_order": [0, 1, 2, 3],
        }
    )

    status_df = pd.DataFrame(
        {
            "ticker": ["AAPL", "MSFT", "NVDA", "AMZN"],
            "status": ["success", "pending", "failed", "failed"],
            "attempt_count": [1, 0, 1, 3],
            "last_error_message": [None, None, "temporary error", "max attempts"],
            "last_successful_date": [
                pd.Timestamp("2026-06-01").date(),
                None,
                None,
                None,
            ],
        }
    )

    result = select_tasks_to_process(
        task_df=task_df,
        status_df=status_df,
        max_attempts=3,
        limit=None,
    )

    tickers = result["ticker"].tolist()

    assert tickers == ["MSFT", "NVDA"]
    assert "AAPL" not in tickers
    assert "AMZN" not in tickers


def test_select_tasks_to_process_applies_limit() -> None:
    task_df = pd.DataFrame(
        {
            "ticker": ["AAPL", "MSFT", "NVDA"],
            "security_id": ["tiingo:AAPL", "tiingo:MSFT", "tiingo:NVDA"],
            "source": ["tiingo"] * 3,
            "dataset_name": ["equity_price_daily"] * 3,
            "requested_start_date": [pd.Timestamp("2019-01-01").date()] * 3,
            "requested_end_date": [pd.Timestamp("2026-06-02").date()] * 3,
            "status": ["pending"] * 3,
            "_task_order": [0, 1, 2],
        }
    )

    status_df = pd.DataFrame(
        {
            "ticker": ["AAPL", "MSFT", "NVDA"],
            "status": ["pending", "pending", "pending"],
            "attempt_count": [0, 0, 0],
            "last_error_message": [None, None, None],
            "last_successful_date": [None, None, None],
        }
    )

    result = select_tasks_to_process(
        task_df=task_df,
        status_df=status_df,
        max_attempts=3,
        limit=2,
    )

    assert result["ticker"].tolist() == ["AAPL", "MSFT"]


def test_build_suspicious_pattern_report_flags_special_security_suffixes() -> None:
    symbol_df = pd.DataFrame(
        {
            "ticker": [
                "AAPL",
                "UTF-R",
                "STR-WS",
                "VYX-W",
                "TFC-P-R",
                "BRK-B",
            ],
            "status": ["success"] * 6,
            "n_rows": [1000, 15, 7, 4, 1000, 1000],
            "min_date": ["2019-01-01"] * 6,
            "max_date": ["2026-06-01"] * 6,
        }
    )

    result = build_suspicious_pattern_report(symbol_df)

    flagged = set(result["ticker"])

    assert "UTF-R" in flagged
    assert "STR-WS" in flagged
    assert "VYX-W" in flagged
    assert "TFC-P-R" in flagged

    assert "AAPL" not in flagged
    assert "BRK-B" not in flagged