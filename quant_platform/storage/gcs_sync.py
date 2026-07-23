from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_LOCAL_DATA_ROOT = PROJECT_ROOT / "data"

ALLOWED_SUFFIXES = frozenset(
    {
        ".json",
        ".csv",
        ".parquet",
    }
)


@dataclass(frozen=True)
class GcsUploadItem:
    """One local-file-to-GCS-object upload mapping."""

    local_path: Path
    object_name: str


def _resolve_project_path(
    path: str | Path,
    project_root: str | Path = PROJECT_ROOT,
) -> Path:
    resolved = Path(path)

    if not resolved.is_absolute():
        resolved = Path(project_root) / resolved

    return resolved.resolve()


def gcs_object_name_from_local_path(
    local_path: str | Path,
    local_data_root: str | Path = DEFAULT_LOCAL_DATA_ROOT,
    *,
    project_root: str | Path = PROJECT_ROOT,
) -> str:
    """
    Convert a local data-lake path to a GCS object name.

    Examples:
    data/ods/source=tiingo/file.json
      -> ods/source=tiingo/file.json

    data/dwd/equity_price_daily/year=2026/part.parquet
      -> dwd/equity_price_daily/year=2026/part.parquet
    """
    resolved_path = _resolve_project_path(
        local_path,
        project_root=project_root,
    )
    resolved_data_root = _resolve_project_path(
        local_data_root,
        project_root=project_root,
    )

    try:
        relative_path = resolved_path.relative_to(
            resolved_data_root
        )
    except ValueError as exc:
        raise ValueError(
            "Local file must be under "
            f"{resolved_data_root}, got {resolved_path}"
        ) from exc

    return relative_path.as_posix()


def iter_files_to_upload(
    local_roots: Iterable[str | Path],
    *,
    allowed_suffixes: Iterable[str] = ALLOWED_SUFFIXES,
    project_root: str | Path = PROJECT_ROOT,
) -> Iterator[Path]:
    """Yield allowed files under one or more local roots."""
    normalized_suffixes = {
        str(suffix).lower()
        for suffix in allowed_suffixes
    }

    for root in local_roots:
        resolved_root = _resolve_project_path(
            root,
            project_root=project_root,
        )

        if not resolved_root.exists():
            continue

        if resolved_root.is_file():
            candidates = [resolved_root]
        else:
            candidates = sorted(resolved_root.rglob("*"))

        for file_path in candidates:
            if (
                file_path.is_file()
                and file_path.suffix.lower() in normalized_suffixes
            ):
                yield file_path


def build_upload_plan(
    local_roots: Iterable[str | Path],
    *,
    local_data_root: str | Path = DEFAULT_LOCAL_DATA_ROOT,
    allowed_suffixes: Iterable[str] = ALLOWED_SUFFIXES,
    project_root: str | Path = PROJECT_ROOT,
) -> list[GcsUploadItem]:
    """Build deterministic local-to-GCS upload mappings."""
    items = [
        GcsUploadItem(
            local_path=file_path,
            object_name=gcs_object_name_from_local_path(
                local_path=file_path,
                local_data_root=local_data_root,
                project_root=project_root,
            ),
        )
        for file_path in iter_files_to_upload(
            local_roots=local_roots,
            allowed_suffixes=allowed_suffixes,
            project_root=project_root,
        )
    ]

    return sorted(items, key=lambda item: item.object_name)


def upload_file(
    bucket: Any,
    local_path: str | Path,
    *,
    object_name: str | None = None,
    local_data_root: str | Path = DEFAULT_LOCAL_DATA_ROOT,
    project_root: str | Path = PROJECT_ROOT,
) -> str:
    """
    Upload one local file through an existing GCS bucket object.

    The caller owns Google authentication and client construction.
    """
    resolved_path = _resolve_project_path(
        local_path,
        project_root=project_root,
    )

    if not resolved_path.is_file():
        raise FileNotFoundError(
            f"Local upload file not found: {resolved_path}"
        )

    destination = object_name or gcs_object_name_from_local_path(
        local_path=resolved_path,
        local_data_root=local_data_root,
        project_root=project_root,
    )

    blob = bucket.blob(destination)
    blob.upload_from_filename(str(resolved_path))

    return f"gs://{bucket.name}/{destination}"


def execute_upload_plan(
    bucket: Any,
    upload_plan: Iterable[GcsUploadItem],
) -> list[str]:
    """Upload a prepared plan and return uploaded GCS URIs."""
    uploaded_uris: list[str] = []

    for item in upload_plan:
        uploaded_uris.append(
            upload_file(
                bucket=bucket,
                local_path=item.local_path,
                object_name=item.object_name,
            )
        )

    return uploaded_uris