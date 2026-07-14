from __future__ import annotations

from pathlib import Path


DATA_ROOT = Path("data")
ODS_ROOT = DATA_ROOT / "ods"
DWD_ROOT = DATA_ROOT / "dwd"
REPORTS_ROOT = Path("reports")

SECURITY_MASTER_DWD_ROOT = DWD_ROOT / "security_master"
DWD_PRICE_ROOT = DWD_ROOT / "equity_price_daily"

CONFIG_ROOT = Path("configs")
PRICE_UPDATE_CONFIG_PATH = CONFIG_ROOT / "price_update.yml"

BOOTSTRAP_CANDIDATES_TASK_LIST_PATH = (
    SECURITY_MASTER_DWD_ROOT / "backfill_task_list_bootstrap_candidates.parquet"
)
DIM_SECURITY_PATH = SECURITY_MASTER_DWD_ROOT / "dim_security.parquet"

PRICE_GAP_TASK_LIST_PATH = SECURITY_MASTER_DWD_ROOT / "price_gap_task_list.parquet"
PRICE_GAP_EXCLUDED_SYMBOLS_PATH = (
    SECURITY_MASTER_DWD_ROOT / "price_gap_excluded_symbols.parquet"
)

PRICE_UPDATE_DOWNLOAD_REPORT_ROOT = (
    REPORTS_ROOT / "price_update_download"
)

DWD_PRICE_UPDATE_STAGING_ROOT = (
    DATA_ROOT / "_tmp" / "dwd_price_update"
)

DWD_PRICE_UPDATE_ARCHIVE_ROOT = (
    DATA_ROOT / "_archive" / "dwd_price_update"
)

PRICE_UPDATE_TRANSFORM_REPORT_ROOT = (
    REPORTS_ROOT / "price_update_transform"
)

PRICE_UPDATE_AUDIT_REPORT_ROOT = (
    REPORTS_ROOT / "price_update_audit"
)

def to_gcs_object_path(local_path: str | Path) -> str:
    """
    Convert a local data-lake path to a GCS object-relative path.

    Example:
    data/ods/source=tiingo/... -> ods/source=tiingo/...

    The local `data/` directory name should not become part of the GCS prefix.
    """
    path = Path(local_path)

    if path.parts and path.parts[0] == DATA_ROOT.name:
        remaining_parts = path.parts[1:]
        path = Path(*remaining_parts) if remaining_parts else Path()

    return path.as_posix()


def ensure_parent_dir(path: str | Path) -> None:
    """Create the parent directory for a file path if needed."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)