from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from quant_platform.paths.data_lake import ensure_parent_dir


def write_json(
    path: str | Path,
    payload: Any,
    *,
    indent: int | None = 2,
) -> Path:
    """Write JSON payload to a local file and return the output path."""
    output_path = Path(path)
    ensure_parent_dir(output_path)

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(
            payload,
            file,
            indent=indent,
            ensure_ascii=False,
        )

    return output_path


def read_json(path: str | Path) -> Any:
    """Read and decode a local JSON file."""
    input_path = Path(path)

    if not input_path.exists():
        raise FileNotFoundError(f"JSON file not found: {input_path}")

    with input_path.open("r", encoding="utf-8") as file:
        return json.load(file)


def write_json_rows(
    path: str | Path,
    rows: Sequence[dict[str, Any]],
    *,
    indent: int | None = 2,
) -> Path:
    """Validate and write a list of JSON object rows."""
    normalized_rows = list(rows)

    if not all(isinstance(row, dict) for row in normalized_rows):
        raise ValueError("rows must contain dictionaries only")

    return write_json(
        path=path,
        payload=normalized_rows,
        indent=indent,
    )


def read_json_rows(path: str | Path) -> list[dict[str, Any]]:
    """Read a JSON file whose top-level value must be a list of objects."""
    payload = read_json(path)

    if not isinstance(payload, list):
        raise ValueError(
            f"Expected JSON list in {Path(path)}, "
            f"got {type(payload).__name__}"
        )

    if not all(isinstance(row, dict) for row in payload):
        raise ValueError(
            f"Expected JSON object rows in {Path(path)}"
        )

    return [dict(row) for row in payload]