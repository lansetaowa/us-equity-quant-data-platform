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
    assert Path("data/ods") == ODS_ROOT
    assert Path("data/dwd/equity_price_daily") == DWD_PRICE_ROOT
    assert Path("configs/price_update.yml") == PRICE_UPDATE_CONFIG_PATH


def test_security_master_paths():
    assert Path(
        "data/dwd/security_master/backfill_task_list_bootstrap_candidates.parquet"
    ) == BOOTSTRAP_CANDIDATES_TASK_LIST_PATH
    assert Path(
        "data/dwd/security_master/dim_security.parquet"
    ) == DIM_SECURITY_PATH
    assert Path(
        "data/dwd/security_master/price_gap_task_list.parquet"
    ) == PRICE_GAP_TASK_LIST_PATH
    assert Path(
        "data/dwd/security_master/price_gap_excluded_symbols.parquet"
    ) == PRICE_GAP_EXCLUDED_SYMBOLS_PATH


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