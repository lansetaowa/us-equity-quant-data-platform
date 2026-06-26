from __future__ import annotations

import pandas as pd

from quant_platform.warehouse.price_incremental import (
    build_date_predicate,
    build_replace_sql,
    build_staging_table_id,
    classify_target_state,
    month_ranges_from_manifest,
    summarize_manifest,
)


def manifest() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "year": 2026,
                "month": 6,
                "existing_row_count": 100,
                "inserted_key_count": 20,
                "final_row_count": 120,
            }
        ]
    )


def test_month_ranges_and_predicate():
    ranges = month_ranges_from_manifest(
        manifest()
    )

    assert len(ranges) == 1

    predicate = build_date_predicate(
        "`date`",
        ranges,
    )

    assert predicate == (
        "(`date` >= DATE '2026-06-01' "
        "AND `date` < DATE '2026-07-01')"
    )


def test_summarize_manifest():
    result = summarize_manifest(
        manifest()
    )

    assert result == {
        "expected_existing_rows": 100,
        "expected_inserted_rows": 20,
        "expected_final_rows": 120,
    }


def test_staging_table_id_is_sanitized():
    table_id = build_staging_table_id(
        "project",
        "dataset",
        "dwd_equity_price_daily",
        "price-download-20260623T025906Z",
    )

    assert table_id == (
        "project.dataset."
        "dwd_equity_price_daily__stg_"
        "price_download_20260623T025906Z"
    )


def test_build_replace_sql():
    ranges = month_ranges_from_manifest(
        manifest()
    )

    sql = build_replace_sql(
        target_table_id=(
            "project.dataset.target"
        ),
        staging_table_id=(
            "project.dataset.staging"
        ),
        ranges=ranges,
    )

    assert "BEGIN TRANSACTION" in sql
    assert "DELETE FROM" in sql
    assert "INSERT INTO" in sql
    assert "COMMIT TRANSACTION" in sql
    assert "2026-06-01" in sql
    assert "2026-07-01" in sql


def test_classify_target_state():
    assert (
        classify_target_state(
            100,
            expected_existing_rows=100,
            expected_final_rows=120,
        )
        == "pre_update"
    )

    assert (
        classify_target_state(
            120,
            expected_existing_rows=100,
            expected_final_rows=120,
        )
        == "already_updated"
    )

    assert (
        classify_target_state(
            110,
            expected_existing_rows=100,
            expected_final_rows=120,
        )
        == "unexpected"
    )