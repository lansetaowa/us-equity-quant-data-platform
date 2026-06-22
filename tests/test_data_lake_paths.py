from __future__ import annotations

from pathlib import Path

from quant_platform.paths.data_lake import (
    BOOTSTRAP_CANDIDATES_TASK_LIST_PATH,
    DIM_SECURITY_PATH,
    DWD_PRICE_ROOT,
    ODS_ROOT,
    PRICE_GAP_EXCLUDED_SYMBOLS_PATH,
    PRICE_GAP_TASK_LIST_PATH,
    PRICE_UPDATE_CONFIG_PATH,
    ensure_parent_dir,
    to_gcs_object_path,
)


def test_core_data_lake_paths():
    assert ODS_ROOT == Path("data/ods")
    assert DWD_PRICE_ROOT == Path("data/dwd/equity_price_daily")
    assert PRICE_UPDATE_CONFIG_PATH == Path("configs/price_update.yml")


def test_security_master_paths():
    assert BOOTSTRAP_CANDIDATES_TASK_LIST_PATH == Path(
        "data/dwd/security_master/backfill_task_list_bootstrap_candidates.parquet"
    )
    assert DIM_SECURITY_PATH == Path(
        "data/dwd/security_master/dim_security.parquet"
    )
    assert PRICE_GAP_TASK_LIST_PATH == Path(
        "data/dwd/security_master/price_gap_task_list.parquet"
    )
    assert PRICE_GAP_EXCLUDED_SYMBOLS_PATH == Path(
        "data/dwd/security_master/price_gap_excluded_symbols.parquet"
    )


def test_to_gcs_object_path_strips_data_prefix():
    path = Path(
        "data/ods/source=tiingo/dataset=equity_price_daily/"
        "symbol=AAPL/request_start=2026-06-12/"
        "request_end=2026-06-12/prices.json"
    )

    assert to_gcs_object_path(path) == (
        "ods/source=tiingo/dataset=equity_price_daily/"
        "symbol=AAPL/request_start=2026-06-12/"
        "request_end=2026-06-12/prices.json"
    )


def test_to_gcs_object_path_keeps_non_data_path():
    path = Path(
        "ods/source=tiingo/dataset=equity_price_daily/"
        "symbol=AAPL/prices.json"
    )

    assert to_gcs_object_path(path) == (
        "ods/source=tiingo/dataset=equity_price_daily/"
        "symbol=AAPL/prices.json"
    )


def test_ensure_parent_dir(tmp_path):
    output_path = tmp_path / "nested" / "dir" / "file.parquet"

    ensure_parent_dir(output_path)

    assert output_path.parent.exists()