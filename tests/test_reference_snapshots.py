from __future__ import annotations

from pathlib import Path

import pytest

from scripts.build_security_master import (
    build_dim_security_snapshot_gcs_destination,
    build_dim_security_snapshot_path,
)
from scripts.build_security_master import (
    normalize_snapshot_date as normalize_security_snapshot_date,
)
from scripts.ingest_tiingo_supported_tickers import (
    build_supported_tickers_gcs_destinations,
    build_supported_tickers_paths,
)
from scripts.ingest_tiingo_supported_tickers import (
    normalize_snapshot_date as normalize_supported_snapshot_date,
)


def test_supported_tickers_snapshot_paths():
    root = Path("data/ods/source=tiingo/dataset=supported_tickers")

    paths = build_supported_tickers_paths(
        root,
        "2026-07-24",
    )

    assert paths["snapshot"] == (
        root
        / "snapshot_date=2026-07-24"
        / "supported_tickers.csv"
    )
    assert paths["latest"] == root / "supported_tickers.csv"


def test_supported_tickers_gcs_destinations():
    destinations = build_supported_tickers_gcs_destinations(
        "2026-07-24"
    )

    assert destinations["snapshot"] == (
        "ods/source=tiingo/dataset=supported_tickers/"
        "snapshot_date=2026-07-24/supported_tickers.csv"
    )
    assert destinations["latest"] == (
        "ods/source=tiingo/dataset=supported_tickers/"
        "supported_tickers.csv"
    )


def test_dim_security_snapshot_path():
    root = Path("data/dwd/security_master_snapshots")

    assert build_dim_security_snapshot_path(
        root,
        "2026-07-24",
    ) == (
        root
        / "snapshot_date=2026-07-24"
        / "dim_security.parquet"
    )


def test_dim_security_snapshot_gcs_destination():
    assert build_dim_security_snapshot_gcs_destination(
        "2026-07-24"
    ) == (
        "dwd/security_master_snapshots/"
        "snapshot_date=2026-07-24/"
        "dim_security.parquet"
    )


@pytest.mark.parametrize(
    "value",
    [
        "2026-07-24",
        "2026-07-24 00:00:00",
    ],
)
def test_snapshot_date_normalization_accepts_valid_dates(value):
    assert normalize_supported_snapshot_date(value) == "2026-07-24"
    assert normalize_security_snapshot_date(value) == "2026-07-24"


@pytest.mark.parametrize(
    "value",
    [
        "bad-date",
        "not-a-date",
    ],
)
def test_snapshot_date_normalization_rejects_invalid_dates(value):
    with pytest.raises(ValueError, match="Invalid snapshot date"):
        normalize_supported_snapshot_date(value)

    with pytest.raises(ValueError, match="Invalid snapshot date"):
        normalize_security_snapshot_date(value)